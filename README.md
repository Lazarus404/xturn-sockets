# XTurn Sockets

A format-agnostic Elixir socket library with pluggable packet framing and a single reusable
drain engine for UDP, TCP, TLS, DTLS, and SCTP (listen-only; see below).

## Features

- **Transport behaviour** - uniform listen/accept/send/setopts/message normalization
- **Accumulator behaviour** - pluggable "is there a whole packet yet?" framing
- **Handler behaviour** - business logic per tier
- **Full drain loop** - extracts every whole packet from each read/datagram before re-arming
- **Built-in accumulators** - `Accumulator.Raw`, `Accumulator.LengthPrefixed`, `Accumulator.Reorder`
- **Telemetry** - optional `:telemetry` events via `Telemetry.emit/3` under `[:xturn_sockets, ...]`

**Control vs data:** route request-shaped traffic through `Engine` / `Handler`. High-rate
media belongs on `UDP.open_relay/2` or a raw socket - not through the drain loop.

**SCTP:** `listen/3` works when OTP provides `:gen_sctp`. `Acceptor` cannot drive SCTP
(`accept/2` returns `{:error, :sctp_not_supported}`); associations need a custom owner.

**Supervision:** start `SockSupervisor` and (if using async tiers) `TierSupervisor.Task` /
`TierSupervisor.Pool` in your application before listeners. `DatagramServer` is one process
per listen socket.

## Installation

```elixir
def deps do
  [
    {:xturn_sockets, "~> 2.2"},
    {:telemetry, "~> 1.0"}
  ]
end
```

## Quick start

### 1. Implement a handler

```elixir
defmodule MyApp.Handler do
  @behaviour Xirsys.Sockets.Handler

  alias Xirsys.Sockets.Conn

  @impl true
  def handle_connect(conn), do: {:ok, conn.assigns}

  @impl true
  def handle_packet(packet, _meta, conn, state) do
    IO.inspect({packet, conn.client_ip, conn.client_port})
    {:ok, state}
  end
end
```

### 2. Start a plain-UDP listener

```elixir
{:ok, pid} =
  Xirsys.Sockets.DatagramServer.start_link(
    ip: {0, 0, 0, 0},
    port: 3478,
    handler: MyApp.Handler,
    accumulator: {Xirsys.Sockets.Accumulator.LengthPrefixed, header_size: 2},
    assigns: %{}
  )
```

Each UDP datagram is framed and **fully drained** (multiple coalesced packets in one datagram are
all dispatched).

### 3. Start a TCP/TLS/SCTP/DTLS acceptor

```elixir
# Ensure the connection supervisor is running (tests start this automatically)
{:ok, _} = Xirsys.Sockets.SockSupervisor.start_link()

{:ok, pid} =
  Xirsys.Sockets.Acceptor.start_link(
    transport: Xirsys.Sockets.Transport.TCP,
    ip: {0, 0, 0, 0},
    port: 3478,
    handler: MyApp.Handler,
    accumulator: {Xirsys.Sockets.Accumulator.LengthPrefixed, header_size: 2},
    assigns: %{}
  )
```

Each accepted connection runs in its own `Xirsys.Sockets.Connection` process, accumulating
stream bytes across reads and **fully draining** after every chunk (pipelined packets in one
TCP read are all dispatched).

## Reordering

`Xirsys.Sockets.Accumulator.Reorder` wraps any inner accumulator (typically
`LengthPrefixed` or `Raw`), tags each whole packet with an integer key via `key_fun/2`, and
holds out-of-order packets until contiguous keys can be released, a window or time limit is
hit, or the packet is marked `:unordered` (immediate passthrough).

```elixir
accumulator: {
  Xirsys.Sockets.Accumulator.Reorder,
  name: :rtp,
  inner: Xirsys.Sockets.Accumulator.LengthPrefixed,
  inner_opts: [header_size: 2],
  key_fun: &MyApp.RTP.sequence/2,
  window: 32,
  max_delay_ms: 100,
  on_overflow: :flush_oldest
}
```

Tunable keys (`window`, `max_delay_ms`, `on_overflow`, `enabled`) merge with library defaults
and application config:

```elixir
config :xturn_sockets,
  config_app: :my_app,
  reorder: [
    rtp: [window: 32, max_delay_ms: 150]
  ]

config :my_app,
  reorder: [
    rtp: [window: 48]
  ]
```

Precedence: library defaults < `config :xturn_sockets, :reorder, name: [...]` <
`config :config_app, :reorder, name: [...]` < explicit keys in the accumulator spec.
`key_fun`, `inner`, and `name` are always supplied in code - never read from application
config.

When an accumulator may hold packets across reads (e.g. waiting for a missing sequence number),
pass `tick_interval_ms` to `Connection.start_link/1` or `DatagramServer.start_link/1`. The
process periodically calls the drain engine without new inbound data so time-based flushing
(`max_delay_ms`) can run. Opt-in and zero-cost when unset.

## Pipelines

Multi-tier processing uses `use Xirsys.Sockets.Pipeline` to declare tiers and handler
`{:descend, tier, payload, state}` to route a packet's payload into another tier's
accumulator/handler pair:

```elixir
defmodule MyApp.Pipeline do
  use Xirsys.Sockets.Pipeline

  tier :root,
    accumulator: {Xirsys.Sockets.Accumulator.LengthPrefixed, header_size: 2},
    handler: MyApp.Handlers.Stun

  tier :rtp,
    accumulator: Xirsys.Sockets.Accumulator.Raw,
    handler: MyApp.Handlers.Rtp
end

{:ok, pid} =
  Xirsys.Sockets.Acceptor.start_link(
    transport: Xirsys.Sockets.Transport.TCP,
    ip: {0, 0, 0, 0},
    port: 3478,
    pipeline: MyApp.Pipeline,
    assigns: %{}
  )
```

**Semantics:**

- **Crash isolation** - an unhandled exception while draining a descended tier is caught,
  emits `[:xturn_sockets, :tier_crashed]`, and the outer tier continues draining.
- **Close propagation** - an explicit `{:close, state}` from *any* tier closes the whole
  connection or stops further processing for that datagram/association.

Only `:root` receives `handle_connect/1`. Descended tiers start with `nil` handler state on
first `:descend`. On disconnect, `handle_disconnect/2` is invoked for every tier that has been
activated.

## Dispatch strategies

Each pipeline tier accepts `:dispatch` (`:inline` default, `:task`, or `:pool`) and optional
`:pool_size` for `:pool` dispatch (defaults to `Config.tier_pool_size/0`).

- **`:inline`** - drain the descended tier synchronously in the connection/datagram process
  (zero overhead for root-only pipelines).
- **`:task`** - lazily start a supervised `TierSession` worker per tier per owner; pushes are
  cast asynchronously so the root tier keeps relaying without waiting.
- **`:pool`** - same as `:task` but capped by `TierSupervisor.Pool`'s `max_children`. When the
  pool is saturated, that descend is **dropped** (at-most-once) and emits
  `[:xturn_sockets, :tier_pool_saturated]` with `dropped: true`. The owner process never holds
  a duplicate accumulator for the async tier.

Start the tier supervisors alongside `SockSupervisor`:

```elixir
{:ok, _} = Xirsys.Sockets.SockSupervisor.start_link()
{:ok, _} = Xirsys.Sockets.TierSupervisor.Task.start_link()
{:ok, _} = Xirsys.Sockets.TierSupervisor.Pool.start_link()
```

An explicit `{:close, state}` from an async tier sends `{:tier_close, tier, reason}` to the
owning `Connection`/`DatagramServer`, which stops like a transport close. Crashed or saturated
async tiers emit telemetry; packets queued in a crashed `TierSession` mailbox are lost. When
the tier supervisor is unavailable, descends to async tiers are dropped rather than processed
inline in the owner process.

## Bounded buffers

Every built-in `Accumulator` supports `:max_size`. Overflow is surfaced once from the next
`pop/1` as `{:error, :buffer_overflow, acc}`; the engine emits `[:xturn_sockets, :frame_error]`
and continues draining. `Accumulator.Reorder` also aliases `:max_size` to its reorder window and
bounds the ready output queue separately from the reorder `:window`.

## Telemetry

Emit events with `Xirsys.Sockets.Telemetry.emit/3` and attach your own `:telemetry` handlers.
`Telemetry.attach_handlers/0` is optional library logging - not required for integration.

Per-tier events (when `:telemetry_enabled` is true):

- `[:xturn_sockets, :tier_dispatch]` - handler `handle_packet/4` duration, metadata `%{tier: ...}`
- `[:xturn_sockets, :tier_crashed]` - async or descended tier failure
- `[:xturn_sockets, :tier_pool_saturated]` - async tier dropped because the pool is full
- `[:xturn_sockets, :tier_supervisor_unavailable]` - async tier dropped because no supervisor
- `[:xturn_sockets, :tier_dropped]` - async tier session failed to start for another reason
- `[:xturn_sockets, :udp_sessions_evicted]` - idle or excess UDP peer sessions removed
- `[:xturn_sockets, :frame_error]` - accumulator overflow or framing errors, tagged by tier
- `[:xturn_sockets, :message_sent]` / `:send_error` - replies include `tier` metadata

## Configuration

Host applications override library defaults by setting `:config_app`:

```elixir
config :xturn_sockets,
  config_app: :my_app,
  buffer_size: 262_144,
  listener_buffer_size: 4_194_304,
  ssl_handshake_timeout: 10_000,
  rate_limit_enabled: true,
  telemetry_enabled: true

config :my_app,
  buffer_size: 131_072
```

Lookup precedence: `config :config_app, key` -> `config :xturn_sockets, key` -> default.

TLS/DTLS certificates: pass `certfile` / `keyfile` in `listen/3` opts, or set
`config :xturn_sockets, certs: [...]` (or host `:config_app`). Legacy `:certs` / `:xturn`
application env is still supported.

## Architecture

| Module | Role |
|--------|------|
| `Transport.*` | Protocol-specific socket I/O + mailbox normalization |
| `Accumulator.*` | Framing / boundary detection |
| `Handler` | Your application logic per tier |
| `Pipeline` | Declarative multi-tier tier graph |
| `TierSession` | Async per-tier worker for `:task`/`:pool` dispatch |
| `TierSupervisor.Task` / `.Pool` | Supervisors for tier session workers |
| `Engine` | Shared push-and-drain loop |
| `Connection` | One process per stream/association |
| `DatagramServer` | One process per plain-UDP socket |
| `Acceptor` | Accept loop for connection-oriented transports |
| `SockSupervisor` | `DynamicSupervisor` for `Connection` children |

Protocol-specific framing (STUN/TURN, RTP, etc.) belongs in **your application** as custom
`Accumulator` and `Handler` modules - this library ships only the generic mechanism.

## Testing

```bash
mix test
```

## Changelog

### 2.2.0

- `DatagramServer.socket/1` and `DatagramServer.endpoint/1` for RFC 5780 CHANGE-REQUEST (reply from a different local UDP endpoint than the one that received the datagram).
- TLS listen ciphers now include TLS 1.3 exclusive suites as well as TLS 1.2 AEAD. Advertising 1.3 with only 1.2 suites made OTP fail the handshake (`no_suitable_cipher`).
- `Acceptor` transfers socket ownership, then arms `{active, :once}`. `Connection` stays passive until that handoff so the first TLS 1.3 application record is not delivered to the acceptor.

### 2.1.0

- `Transport.UDP.open_relay/2` plus `set_dont_fragment/1`, `set_tos/2`, `set_hop_limit/2`, and `set_flow_label/2` for high-rate media sockets outside the drain loop.
- `Transport.TLS.security_opts/0`: TLS 1.2/1.3, AEAD-only defaults, strip legacy versions. Certificates may be passed on `listen/3` (`certfile` / `keyfile`); `:xturn_sockets` / host `:config_app` / legacy `:certs` / `:xturn` env still work.
- UDP ICMP / `{:udp_error, ...}` normalized in `handle_message/2`.
- Document SCTP as listen-only (`Acceptor` cannot `accept/2`), required supervisors, and that `Telemetry.attach_handlers/0` is optional logging.

### 2.0.0

Breaking rewrite versus 1.x. The old `Socket` / `Listener` / `Client` modules are gone.

- `Transport`, `Accumulator`, and `Handler` behaviours with a shared `Engine` drain loop.
- `Connection` (one process per stream) and `DatagramServer` (one process per UDP listen socket); `Acceptor` for TCP/TLS/DTLS.
- Built-in `Accumulator.Raw`, `LengthPrefixed`, and `Reorder`.
- `Pipeline` DSL with `{:descend, ...}`, crash isolation, and `:inline` / `:task` / `:pool` dispatch.
- Bounded accumulator buffers, per-tier telemetry, and `SockSupervisor` / `TierSupervisor`.

## License

Apache 2.0 - see [LICENSE.md](LICENSE.md).

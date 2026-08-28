# xturn-sockets architecture

`xturn-sockets` is a format-agnostic socket library. It owns listen/accept/read/write, packet framing, and a shared drain loop. It does **not** know STUN, TURN, or RTP - those live in the host application (`xturn`), since `xturn-sockets` is designed to be app agnostic (so it was probably stupid of me to call it thus).

## Layers

```
Host app (xturn Handler + Accumulator)
        │
   Pipeline / Engine          <- drain every whole packet, then re-arm
        │
 Connection | DatagramServer  <- one process per stream, or one per UDP listen socket
        │
   Transport.*                <- :gen_tcp / :gen_udp / :ssl / :gen_sctp
```

Control-shaped traffic (requests, handshakes) goes through `Engine` and a `Handler`. High-rate media should use `Transport.UDP.open_relay/2` or a raw socket - not the drain loop.

## Module map

| Module | Role |
|---|---|
| `Xirsys.Sockets.Transport` | Behaviour: `listen/3`, `accept/2`, `send/2`, `setopts/2`, mailbox normalize |
| `Transport.UDP` | Datagram I/O, ICMP normalize, `open_relay/2` for media sockets |
| `Transport.TCP` | Stream listen/accept/connect |
| `Transport.TLS` | TLS 1.2/1.3 over TCP |
| `Transport.DTLS` | DTLS over UDP (connection-oriented accept) |
| `Transport.SCTP` | Listen-only; `Acceptor` cannot `accept/2` |
| `Xirsys.Sockets.Accumulator` | Behaviour: “is there a whole packet yet?” |
| `Accumulator.Raw` | One datagram / chunk = one packet |
| `Accumulator.LengthPrefixed` | Size-prefixed stream framing |
| `Accumulator.Reorder` | Hold out-of-order packets until a key window is contiguous |
| `Xirsys.Sockets.Handler` | App logic per tier (`handle_packet/4`) |
| `Xirsys.Sockets.Pipeline` | `use` + `tier` DSL; named accumulator/handler graph |
| `Xirsys.Sockets.Engine` | Shared push-and-drain loop |
| `Xirsys.Sockets.Connection` | One process per accepted stream/association |
| `Xirsys.Sockets.DatagramServer` | One process per UDP listen socket; per-peer session map |
| `Xirsys.Sockets.Acceptor` | Accept loop; transfers socket to `Connection` |
| `Xirsys.Sockets.SockSupervisor` | `DynamicSupervisor` for `Connection` children |
| `Xirsys.Sockets.TierSession` | Async worker for `:task` / `:pool` descended tiers |
| `TierSupervisor.Task` / `.Pool` | Supervisors for those workers |
| `Xirsys.Sockets.Config` | Host-app env overlay (`:config_app`) |
| `Xirsys.Sockets.Telemetry` | Optional `[:xturn_sockets, ...]` events |
| `Xirsys.Sockets.Conn` | Per-packet connection view (`client_ip`, `socket`, `assigns`) |

## Process model

**UDP (plain).** `DatagramServer` owns the listen socket. Each peer is a lightweight session (accumulator + handler state) inside that process. Incoming datagrams are framed and fully drained before `{active, N}` is re-armed.

**TCP / TLS / DTLS.** `Acceptor` listens, accepts, transfers ownership, then `Connection` runs the same engine. Stream bytes accumulate across reads; pipelined packets in one read are all dispatched.

**SCTP.** `listen/3` works when OTP provides `:gen_sctp`. Associations need a custom owner - `Acceptor` returns `{:error, :sctp_not_supported}`.

Start `SockSupervisor` (and `TierSupervisor.Task` / `.Pool` if using async tiers) before listeners.

## Engine and pipelines

`Engine.push_and_drain/9` (called after each read) and `Engine.drain/8` (timer tick, no new data) pop from the current tier until the accumulator returns `{:more, _}`.

A `Handler` may:

- `{:ok, state}` - consume
- `{:reply, iodata, state}` - send on the same socket
- `{:descend, tier, payload, state}` - push into another named tier
- `{:close, state}` - stop the connection / datagram processing

Tier dispatch:

- `:inline` (default) - drain the child tier in the owner process
- `:task` - supervised `TierSession` per owner/tier; casts so the root keeps moving
- `:pool` - same as `:task` with `max_children`; overflow is dropped (at-most-once)

Unhandled exceptions in a descended tier are caught, emit `[:xturn_sockets, :tier_crashed]`, and the outer tier continues. `{:close, _}` from any tier closes the whole connection.

## How xturn uses this

xturn declares two pipelines (`Xirsys.XTurn.SocketPipeline` for streams, `.Datagram` for UDP/DTLS). Both use `Xirsys.XTurn.Accumulators.StunTurn` + `Handlers.StunTurn` as the root tier. Listeners are started from `Xirsys.XTurn.Supervisor` via `DatagramListener` (UDP) or `Acceptor` (TCP/TLS/DTLS). Relay sockets for peer traffic are opened with `Transport.UDP.open_relay/2` and owned by `Xirsys.XTurn.RelayIngress`, not by `DatagramServer`.

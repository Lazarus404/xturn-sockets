# Changelog

## 2026-09-15

- DEPRECATED: in favour of [XSockets](https://github.com/Lazarus404/xsockets)

## 2.2.1

- `SctpAssociation` - WebRTC SCTP-over-DTLS association (sans-IO via Hex `ex_sctp`). Caller owns the DTLS byte pipe (`handle_packet/2` in, `{:transmit, packets}` out).
- `Sctp.Dcep` - Data Channel Establishment Protocol Open/Ack encode and decode (RFC 8832).
- Module docs aligned to required package shape (`## What problem this solves`, `## RFCs`, `@doc` / `@typedoc` / `## Fields`) across transports, pipeline, and supervisors.

## 2.2.0

- `DatagramServer.socket/1` and `DatagramServer.endpoint/1` for RFC 5780 CHANGE-REQUEST (reply from a different local UDP endpoint than the one that received the datagram).
- TLS listen ciphers now include TLS 1.3 exclusive suites as well as TLS 1.2 AEAD. Advertising 1.3 with only 1.2 suites made OTP fail the handshake (`no_suitable_cipher`).
- `Acceptor` transfers socket ownership, then arms `{active, :once}`. `Connection` stays passive until that handoff so the first TLS 1.3 application record is not delivered to the acceptor.

## 2.1.0

- `Transport.UDP.open_relay/2` plus `set_dont_fragment/1`, `set_tos/2`, `set_hop_limit/2`, and `set_flow_label/2` for high-rate media sockets outside the drain loop.
- `Transport.TLS.security_opts/0`: TLS 1.2/1.3, AEAD-only defaults, strip legacy versions. Certificates may be passed on `listen/3` (`certfile` / `keyfile`); `:xturn_sockets` / host `:config_app` / legacy `:certs` / `:xturn` env still work.
- UDP ICMP / `{:udp_error, ...}` normalized in `handle_message/2`.
- Document SCTP as listen-only (`Acceptor` cannot `accept/2`), required supervisors, and that `Telemetry.attach_handlers/0` is optional logging.

## 2.0.0

Breaking rewrite versus 1.x. The old `Socket` / `Listener` / `Client` modules are gone.

- `Transport`, `Accumulator`, and `Handler` behaviours with a shared `Engine` drain loop.
- `Connection` (one process per stream) and `DatagramServer` (one process per UDP listen socket); `Acceptor` for TCP/TLS/DTLS.
- Built-in `Accumulator.Raw`, `LengthPrefixed`, and `Reorder`.
- `Pipeline` DSL with `{:descend, ...}`, crash isolation, and `:inline` / `:task` / `:pool` dispatch.
- Bounded accumulator buffers, per-tier telemetry, and `SockSupervisor` / `TierSupervisor`.

# F19-01 — Universal tunnel: single multiplexed `r1s tunnel` command

**Status:** ✅ Complete — the F14 single interactive byte pipe is reworked into a multiplexed,
addressable, client→allocator stream transport behind the single `r1s tunnel` command, and then
(F20) narrowed to a Docker-style `--port <host>:<container>` surface with no named slots and no
interactive pipe. The multi-destination (container-port list) grant, the per-pair multiplexing
layer (framing, per-stream flow control, stream-open authorization), the `--port` CLI surface, and
the `LocalTunnelOpen.target_port` envelope are landed and covered by tests, including a two-stream
live-mesh leg; `make check` passes. **F22 note:** the standalone `r1s tunnel` command was removed;
the same transport-neutral multiplexed streams now run per-`r1s run -p host:container`
([F22-06](../f22-rns-shared-instance/f22-06-run-tunnels.md)).

## Outcome

`r1s tunnel` is the **single user-facing command** for every tunnel use-case — ngrok-style. The
underlying machinery is reworked from the F14 single interactive byte pipe into a multiplexed,
addressable stream transport, but the user does not see that machinery: one command, one process,
one clear state surface.

The command stays process-attached (you give the mappings and it runs until you stop it, printing
connection state). Its argument shape is Docker-style port mapping:

```
r1s tunnel <execution-id> --port <host>:<container>    # bind 127.0.0.1:<host>, relay to container port <container>
r1s tunnel <execution-id> --port 8080:80 --port 9090:443  # repeatable: expose many container ports
```

The client binds a real local TCP listener on `127.0.0.1:<host>`; each inbound connection opens its
own tunnel stream addressed by the container port. The direction is always client→allocator — the
client connects to the container, never the other way around, so there is no reverse/listen/publish
surface. Every tunnel requires at least one `--port`; the old interactive pipe is removed.

## Scope

- **Multiplexing layer (extends `internal/tunnel/yggdrasil`)** — the core rework:

  - Change `mux.go` from "one remote key → one payload `streamConn`" to **"one remote key → one
    `tunnelMux`, carrying many `stream`s with a stream ID"** (SSH/QUIC/H2-style). The mesh
    addressing stays key-based (Model A preserved); only *within* one authenticated pair do
    multiple logical streams share the single mesh connection.
  - Extend the framing layer (`framing.go`): the first frame of any new stream carries
    `stream_id` and a `target_port` reference — **never** a client-supplied raw
    `(host, port)`. The allocator resolves
    `target_port` against its grant-time target list; anything else is rejected as
    `ReasonUnauthorized` before a payload byte moves.
  - Keep `Reason`/`close`/`EOF` and the preamble unchanged at the *mesh connection* level; the
    per-stream half-close and teardown reasons move to the stream header so streams close
    independently of each other.
  - Backpressure and the readloop stay as-is at the connection level; per-stream flow control (a
    small window, like HTTP/2) prevents one slow stream from stalling the shared upstream.

- **Multi-destination grants (extends F14-01 registry)** — a grant carries **a list of
  container ports** instead of a single `(host, port)`. The allocator binds whatever ports the
  grant names; the client opens a stream to any port the grant names, and each stream consumes an
  independent grant binding. (This allocation of responsibility is reversed by
  [F20-01](../f20-client-tunnel-targets/f20-01-client-supplied-target-slots.md): the client now
  supplies the destination ports and the allocator only binds what the grant carries.)

- **Single command UX** — one `r1s tunnel` process:

  - takes `<execution-id>` and repeatable `--port host:container` mappings;
  - binds a local listener on `127.0.0.1:<host>` for each mapping and relays inbound connections
    to the container port over their own tunnel streams;
  - runs (foreground) until interrupted, printing per-connection state lines to stderr;
  - maps per-stream teardown `Reason` to clear diagnostics on stderr.

- **New local client API (extends `local.proto`, F13)** — additive RPCs alongside `LocalTunnel`
  (open a stream to a container port). Existing field numbers and `LocalTunnel` stay unchanged; the
  bidi message gains an optional `stream_id`/`target_port` envelope used only by the new surface.

## Acceptance

- A multi-stream mesh session carries two concurrent, independent streams (e.g. SSH and HTTP) to a
  single execution without one blocking the other, and both round-trip >MTU payloads.
- A client cannot open a stream to a `target_port` the allocator did not pre-authorize; a
  client-supplied raw `(host, port)` in a stream header is rejected with `ReasonUnauthorized` before
  any payload byte.
- Closing one stream (half-close or classified reason) does not affect the other streams on the same
  authenticated pair.
- `r1s tunnel <execution-id> --port <host>:<container>` binds `127.0.0.1:<host>`, relays an inbound
  connection to the container port, and exits cleanly on interrupt; diagnostics/reasons go to stderr
  and every tunnel requires at least one `--port` (the interactive pipe is removed).
- The contract (`internal/tunnel`) stays transport-neutral; Yggdrasil stays confined to
  `internal/tunnel/yggdrasil`; the core/protocol carry no Yggdrasil-specific type.
- The grant registry still enforces one authenticated peer per execution at the mesh level (Model A
  preserved) and binds only the client-supplied container ports carried in the grant.
- Deterministic tests cover: concurrent streams, target mis-authorization, independent stream
  close, and >MTU payload per stream; `make check` passes (including the live mesh leg, extended
  with a two-stream case).

## Notes

- **Design constraints inherited from F14** (kept): peer-key pinning, one-time short-lived grants,
  destinations never resolved from allocator-local config at grant time, transport-neutral contract,
  lifetime via the F17 lease (not a second lease), no Yggdrasil type in the core/protocol.
- **Design constraints relaxed/removed in this task**: the "one live session per execution" cap
  becomes "one authenticated mesh connection per execution, many streams"; the single `(host:port)`
  target per grant becomes a container-port list. The direction stays client→allocator, as in F14.
- **Backpressure**: per-stream flow-control window (the existing readHighWater becomes per-stream,
  not per mesh connection) so a single slow consumer does not stall the whole allocator edge
  (records the known limitation from resolved decision 17 / BACKLOG "Deferred").
- **Migration**: implemented as additive layers so the machinery lands before the surface narrows;
  the F20 `--port` landing then removes the interactive pipe entirely (every tunnel requires at least
  one `--port`, client binds a local listener).
- **Out of scope (BACKLOG)**: automatic target auto-discovery from the running workload;
  allocator-wide (not per-execution) concurrency knobs; connection pooling across executions; any
  reverse/listen/publish direction or public-URL mapping (this is a private client→allocator
  tunnel, not a reverse-proxy service).

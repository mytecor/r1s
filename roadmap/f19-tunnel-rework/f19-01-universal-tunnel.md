# F19-01 — Universal tunnel: single multiplexed `r1s tunnel` command

**Status:** ✅ Complete — the F14 single interactive byte pipe is reworked into a multiplexed,
addressable, client→allocator stream transport behind the single `r1s tunnel` command, while
staying the same byte pipe for the interactive case. The multi-destination (slot-list) grant, the
per-pair multiplexing layer (framing, per-stream flow control, stream-open authorization), the
`--target <slot>` CLI surface, and the `LocalTunnelOpen.target_slot` envelope are landed and
covered by tests, including a two-stream live-mesh leg; `make check` passes.

## Outcome

`r1s tunnel` is the **single user-facing command** for every tunnel use-case — ngrok-style. The
underlying machinery is reworked from the F14 single interactive byte pipe into a multiplexed,
addressable stream transport, but the user does not see that machinery: one command, one process,
one clear state surface.

The command stays process-attached (you give the target and it runs until you stop it, printing
connection state). It supports, with the same arguments shape:

```
r1s tunnel <execution-id>                     # interactive pipe (current behavior, backwards compatible)
r1s tunnel <execution-id> --target <id>       # open a single stream to a named pre-resolved target slot
```

Both are the **same command**: `--target` selects a named pre-resolved slot; omitting it is the
interactive pipe. The direction is always client→allocator — the client connects to the container,
never the other way around, so there is no reverse/listen/publish surface.

## Scope

- **Multiplexing layer (extends `internal/tunnel/yggdrasil`)** — the core rework:

  - Change `mux.go` from "one remote key → one payload `streamConn`" to **"one remote key → one
    `tunnelMux`, carrying many `stream`s with a stream ID"** (SSH/QUIC/H2-style). The mesh
    addressing stays key-based (Model A preserved); only *within* one authenticated pair do
    multiple logical streams share the single mesh connection.
  - Extend the framing layer (`framing.go`): the first frame of any new stream carries
    `stream_id` and an optional `target_slot` reference — **never** a client-supplied raw
    `(host, port)`. The allocator resolves
    `target_slot` against its grant-time target list; anything else is rejected as
    `ReasonUnauthorized` before a payload byte moves.
  - Keep `Reason`/`close`/`EOF` and the preamble unchanged at the *mesh connection* level; the
    per-stream half-close and teardown reasons move to the stream header so streams close
    independently of each other.
  - Backpressure and the readloop stay as-is at the connection level; per-stream flow control (a
    small window, like HTTP/2) prevents one slow stream from stalling the shared upstream.

- **Multi-destination grants (extends F14-01 registry)** — a grant carries **a list of
  allocator-resolved target slots** instead of a single `(host, port)`. The allocator still resolves
  every slot from its own configuration at mint time; the client opens a stream to any slot the
  grant names, and each stream consumes an independent grant binding.

- **Single command UX** — one `r1s tunnel` process:

  - takes `<execution-id>` and a small flag surface (`--target`);
  - runs (foreground) until interrupted, printing per-connection state lines;
  - `--target` streams a single named slot to stdout (pipe remains byte-clean); the interactive
    pipe keeps raw terminal mode;
  - maps per-stream teardown `Reason` to clear diagnostics on stderr.

- **New local client API (extends `local.proto`, F13)** — additive RPCs alongside `LocalTunnel`
  (open named stream). Existing field numbers and `LocalTunnel` stay unchanged; the bidi message
  gains an optional `stream_id`/`target_slot` envelope used only by the new surface.

## Acceptance

- A multi-stream mesh session carries two concurrent, independent streams (e.g. SSH and HTTP) to a
  single execution without one blocking the other, and both round-trip >MTU payloads.
- A client cannot open a stream to a `target_slot` the allocator did not pre-authorize; a
  client-supplied raw `(host, port)` in a stream header is rejected with `ReasonUnauthorized` before
  any payload byte.
- Closing one stream (half-close or classified reason) does not affect the other streams on the same
  authenticated pair.
- `r1s tunnel <execution-id>` (no flags) still works as today — interactive pipe, backwards
  compatible.
- `r1s tunnel <execution-id> --target <id>` streams the named slot to stdout with no extra output on
  stdout; diagnostics/reasons go to stderr.
- The contract (`internal/tunnel`) stays transport-neutral; Yggdrasil stays confined to
  `internal/tunnel/yggdrasil`; the core/protocol carry no Yggdrasil-specific type.
- The grant registry still enforces one authenticated peer per execution at the mesh level (Model A
  preserved) and resolves every target slot from allocator-local config only.
- Deterministic tests cover: concurrent streams, target mis-authorization, independent stream
  close, and >MTU payload per stream; `make check` passes (including the live mesh leg, extended
  with a two-stream case).

## Notes

- **Design constraints inherited from F14** (kept): peer-key pinning, one-time short-lived grants,
  allocator-resolved targets never client-supplied, transport-neutral contract, lifetime via the F17
  lease (not a second lease), no Yggdrasil type in the core/protocol.
- **Design constraints relaxed/removed in this task**: the "one live session per execution" cap
  becomes "one authenticated mesh connection per execution, many streams"; the single `(host:port)`
  target per grant becomes a slot list. The direction stays client→allocator, as in F14.
- **Backpressure**: per-stream flow-control window (the existing readHighWater becomes per-stream,
  not per mesh connection) so a single slow consumer does not stall the whole allocator edge
  (records the known limitation from resolved decision 17 / BACKLOG "Deferred").
- **Migration**: implement as additive layers behind the existing yield-fake so `r1s tunnel`
  degrades gracefully; the interactive pipe is re-implemented as a stream with no `target_slot`.
- **Out of scope (BACKLOG)**: automatic target auto-discovery from the running workload;
  allocator-wide (not per-execution) concurrency knobs; connection pooling across executions; any
  reverse/listen/publish direction or public-URL mapping (this is a private client→allocator
  tunnel, not a reverse-proxy service).

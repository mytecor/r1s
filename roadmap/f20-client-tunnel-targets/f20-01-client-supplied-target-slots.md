# F20-01 — Client-supplied target slots in the tunnel grant

**Status:** ✅ Complete — the `r1s` client now sends its own raw `(host, port)` slot list in the
`r1s tunnel --target-slot` grant request; the allocator validates only its shape, binds the
client-supplied slots into the minted grant, and the edge splices each stream to the slot the
client named. `r1sd --tunnel-target` / `--tunnel-default-target` are removed.

## Scope

- **Protocol**: the `ExecutionTunnelGrant` request gains an optional client-supplied target slot
  list (a message of `name@host:port` destinations, or raw `host:port`); `ExecutionTunnelGrantAck`
  echoes it back so the client and edge agree on the resolved slots.
- **Allocator**: drop the per-resource-class target map and default-target resolution touched at
  grant time; `AcceptTunnel`/session `ResolveTarget` resolve stream-open frames against the
  client-supplied slot list carried in the grant. The allocator stops deciding where a stream
  terminates. Peer-key pinning, one-live-session, and grant expiry stay.
- **CLI**: `r1s tunnel` gains a way to supply the slot list (`--target <slot>` keeps selecting
  among what the client supplied; a new flag or positional carries the raw destinations, or the
  client auto-derives them from the request's workload).
- **r1sd**: remove `--tunnel-target`, `--tunnel-default-target`, and `ParseTunnelTarget*`
  config plumbing; the edge becomes a pure proxy/splice to grant-carried destinations.
- **Local API / serve**: `LocalTunnelOpen` and the backend `Tunnel` carry the client slot list
  through to `TunnelGrant`.
- **Docs**: F14-01 task, F19 README, ROADMAP F19 section, and BACKLOG decision #9 updated to
  record the reversal; `r1s tunnel --help` reflects the new surface.

## Acceptance

- `r1s tunnel <id> --target http` reaches a `(host, port)` the client supplied, not one the
  allocator configured; verified end-to-end over the in-memory broker and the yggdrasil
  two-stream live-mesh leg.
- A grant with a client-supplied raw `(host, port)` is honored; a stream-open to a slot not in
  the client-supplied list is rejected with `ReasonUnauthorized` before any payload byte.
- `r1sd` starts and mints grants without `--tunnel-target`; those flags and their parse helpers
  are gone from the daemon surface.
- The allocator never reads a slot from its own configuration at grant time; the F14-01
  per-resource-class map and default-target path are unreachable.
- `go build ./...`, `go vet ./...`, `go test -race ./...`, and `make check` pass.

## Notes

- Recorded as a reversal of the resolved [F14-01](./../f14-direct-node-access/f14-01-access-grant.md)
  target decision: the destination is now client authority, the allocator is a proxy/splice
  point. This is the trade-off accepted here: the client can point a tunnel at any reachable
  `(host, port)` on or near the allocator, so authorization rests on the owner-only grant, not on
  an allocator-side allowlist.
- Auto-discovery of the running execution's listening ports is explicitly out of scope; this task
  only moves the source of truth for the slot address from allocator config to the client's
  request.

# F20-01 — Client-supplied target slots in the tunnel grant

**Status:** ✅ Complete — replaced by the Docker-style `--port <host>:<container>` model. The
`r1s` client now binds a real local listener on `127.0.0.1:<host>`; each inbound connection opens
its own tunnel stream addressed by a container port only, the allocator validates only its shape,
binds the client-supplied container ports into the minted grant, and the edge splices each stream
to `127.0.0.1:<container>`. There are no named slots and no interactive pipe: every tunnel requires
at least one `--port`. `r1sd --tunnel-target` / `--tunnel-default-target` are removed.

## Scope

- **Protocol**: `TunnelTarget` is reduced to a single `port` (the `name`/`host` fields are removed,
  field numbers preserved); `LocalTunnelOpen.target_slot` becomes `target_port` (uint32). The
  `ExecutionTunnelGrant` request carries the client-supplied container-port list; the ack echoes it
  back so the client and edge agree on the resolved ports.
- **Allocator**: drop the per-resource-class target map and default-target resolution; the session
  resolves stream-open frames against the client-supplied container-port list carried in the grant
  and splices each stream to `127.0.0.1:<port>`. Peer-key pinning, one-live-session, and grant
  expiry stay.
- **CLI**: `r1s tunnel <execution-id> --port <host>:<container>` (repeatable, Docker-style). The
  client binds `127.0.0.1:<host>` locally; browser/client traffic reaches the container service at
  `127.0.0.1:<container>`. The old interactive pipe (`r1s tunnel <id>` with no flags) is removed:
  every tunnel requires at least one `--port`.
- **r1sd**: remove `--tunnel-target`, `--tunnel-default-target`, and `ParseTunnelTarget*` config
  plumbing; the edge becomes a pure proxy/splice to grant-carried container ports.
- **Local API / serve**: `LocalTunnelOpen` and the backend `Tunnel` carry the target container port
  and the container-port list through to the grant.

## Acceptance

- `r1s tunnel <id> --port 8080:80` binds `127.0.0.1:8080`; a browser or client connection to it is
  relayed to the container service at container port `80`; verified end-to-end over the in-memory
  broker and the yggdrasil two-stream live-mesh leg.
- A grant with a client-supplied container port is honored; a stream-open to a port not in the
  client-supplied list is rejected with `ReasonUnauthorized` before any payload byte.
- `r1s tunnel` with no `--port` is rejected with a clear error (the interactive pipe is gone);
  unknown flags are rejected.
- `r1sd` starts and mints grants without `--tunnel-target`; those flags and their parse helpers are
  gone from the daemon surface.
- The allocator never reads a target from its own configuration at grant time; the F14-01
  per-resource-class map and default-target path are unreachable.
- `go build ./...`, `go vet ./...`, `go test -race ./...`, and `make check` pass.

## Notes

- Recorded as a reversal of the resolved [F14-01](./../f14-direct-node-access/f14-01-access-grant.md)
  target decision: the destination is now client authority, the allocator is a proxy/splice point.
  This is the trade-off accepted here: the client can point a tunnel at any reachable container
  port on or near the allocator, so authorization rests on the owner-only grant, not on an
  allocator-side allowlist.
- The container port carried in the tunnel open message and echoed in the grant is the resolved
  destination; the host port is purely a client-local bind and is never sent to the allocator.
- Auto-discovery of the running execution's listening ports is explicitly out of scope; this task
  only moves the source of truth for the destination port from allocator config to the client's
  request.

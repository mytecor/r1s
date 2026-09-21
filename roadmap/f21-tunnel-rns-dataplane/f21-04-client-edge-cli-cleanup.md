# F21-04 — Client edge and CLI/config cleanup

**Status:** ⏳ Planned

## Outcome

The `r1s` client-side edge starts with `r1s serve --tunnel` and dials the allocator over the private
tunnel RNS transport; the user-facing `r1s tunnel <execution> --port <host>:<container>` UX and the
service-backed `LocalTunnel` surface stay identical while their transport internals change. Runner
config is reduced to the new, Ygg-underlay-based surface.

## Scope

- **Client edge**: `r1s serve --tunnel` constructs the private tunnel RNS transport (F21-01) using
  the client's persistent identity, dials the advertised `[ygg-ipv6]:port`, establishes the Link,
  `Link.Identify()`, sends `Open { execution_id, port }`, then relays `127.0.0.1:<host>` ⇄
  `Channel`/`Buffer` ⇄ `127.0.0.1:<container>` (one local TCP connection = one Link = one stream).
  No embedded Ygg node is started by the client.
- **Local API**: rework `LocalTunnelOpen` / `Tunnel` on
  [`localserver`](../../internal/localserver) to the Open-message model (F21-02); keep the stream
  RPC shape and byte-clean stdout contract.
- **Config**: remove `--tunnel-peer`, `--tunnel-endpoint-pubkey`, and the opaque-Ygg
  `--tunnel-endpoint` flags. Add allocator config `--tunnel-interface ygg0`, `--tunnel-port 4242`,
  `--tunnel-enabled` (existing flag reused) and keep `r1s serve --tunnel`. Public Ygg peers are the
  system `yggdrasil` service's responsibility; there is no `r1s` peer list.
- Update README/config docs and any hard-coded default that still names the removed flags or the
  embedded node.

## Acceptance

- `r1s tunnel <execution> --port 8080:80` binds `127.0.0.1:8080` and relays each inbound connection
  to the container service replicated under the Open flow; concurrent local TCP connections behave
  independently.
- Closing one Link/session does not affect other live sessions.
- Half-close client→container correctly propagates EOF to the container writer.
- `--tunnel-peer`, `--tunnel-endpoint-pubkey`, and `--tunnel-endpoint` are removed from both
  binaries; unknown flags are rejected; `r1sd --tunnel-enabled --tunnel-interface ygg0
  --tunnel-port 4242` and `r1s serve --tunnel` parse and start.
- `go build ./...`, `go vet ./...`, `go test -race ./...`, and `make check` pass.

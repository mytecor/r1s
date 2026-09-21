# F21-06 — Remove embedded Ygg adapter, key machinery, and dependencies

**Status:** ⏳ Planned

## Outcome

The embedded Yggdrasil transport and every Ygg-derived/tunnel-grant artifact are deleted from the
tree, and the direct dependencies `yggdrasil-go`, `ironwood`, and `gologme/log` fall out of
`go.mod`/`go.sum` — leaving Reticulum-Go as the tunnel's only networking/crypto dependency.

## Scope

- Delete [`internal/tunnel/yggdrasil`](../../internal/tunnel/yggdrasil): `core.go`, `node.go`,
  `edge.go`, `mux.go`, `pair.go`, `framing.go`, `stream.go`, `dial.go`, `listen.go`, and the
  associated live-mesh/unit tests.
- Delete Ygg-specific identity/key machinery: `NodeKeyContext`, `ClientNodeKeyContext`,
  `AllocatorNodeKeyContext`, `NodeKeyFromSeed`, `NodePubKey`, Ygg peer public-key pinning,
  `tunnel.Conn.PeerKey()`, `MaxPeerKeySize`, `ErrPeerKeyMismatch`, and `Endpoint.PubKey` if it is
  unused after the new scheme.
- Delete the custom tunnel wire protocol over the Ygg `PacketConn`: no DATA/EOF/WINDOW_UPDATE/
  PREAMBLE/ACCEPT frames, no custom stream IDs, no segmentation/reassembly, no custom
  retransmission/backpressure. The payload uses only `Link`/`Channel`/`Buffer`.
- Narrow the generic [`internal/tunnel`](../../internal/tunnel) contract: `Conn` becomes
  `io.ReadWriteCloser` + `CloseWrite()`; keep `CloseRead()` only if the live forwarding path
  actually needs it. Remove `PeerKey`, `Preamble`, `PreambleWriter`, `StreamOpener`, the old
  `Listener` shape, and the session registry where they exist only for the Ygg adapter.
- Remove the tunnel grant remnants from the `internal/tunnel` registry
  (`Mint`, `Accept`, grant expiry/reuse semantics) now covered by the Open flow.
- Run `go mod tidy`; confirm `github.com/yggdrasil-network/yggdrasil-go`,
  `github.com/Arceliar/ironwood`, and `github.com/gologme/log` are no longer direct (or transitive
  reachable) dependencies.
- Update `README.md` "Build from source"/tunnel text and `ARCHITECTURE.md` transport-boundary prose
  to describe the system-Ygg + private-RNS data plane and the removed embedded node.

## Acceptance

- No `import "github.com/yggdrasil-network/yggdrasil-go"` and no `import ironwood` anywhere; a
  grep over the module surfaces none.
- No Ygg-derived keys, no Ygg public keys in the control protocol, no tunnel grants, no custom
  mux/framing, and no DATA/EOF/WINDOW protocol remain.
- `tunnel.Conn` is `io.ReadWriteCloser` + `CloseWrite()` (plus `CloseRead` only if forwarding needs
  it); dead generic abstractions are absent.
- `go build ./...`, `go vet ./...`, `go test -race ./...`, and `make check` pass; the F21-05
  benchmark was completed against the old transport before this removal.

# F21. Tunnel data plane over system Yggdrasil + private RNS

Corresponds to a future milestone in
[ROADMAP.md](../../ROADMAP.md#f21-tunnel-data-plane-over-system-yggdrasil--private-rns).

**Status:** ⏳ Planned

## Outcome

The tunnel subsystem is simplified down to its two independent halves, and the embedded
`yggdrasil-go`/`ironwood` stack is removed from `r1s` entirely:

- The **control plane** is untouched: `r1s` (and `r1s serve`) reaches `r1sd` through the existing
  authenticated RNS control plane. Tunnel data never crosses it.
- The **tunnel data plane** becomes a separate, private Reticulum transport running over the
  system `yggdrasil` daemon, which is used only as an IP underlay / NAT-traversal provider — never
  as an `r1s`-owned mesh. One local TCP connection maps to one RNS Link and one tunnel stream, so
  the custom packet multiplexer, framing, and per-stream flow control all disappear.

Authorization is delegated to the existing persistent RNS identity: on an inbound tunnel Link the
client calls `Link.Identify()`, and the allocator splices only after checking that the identified
remote identity owns a **running** execution and the requested container port is valid. Channel
grants, preamble handshakes, peer-key pinning, Ygg key derivation, and the proto `ygg_peer_pubkey`
field are deleted. The user-facing UX — `r1s tunnel <execution> --port <host>:<container>` and the
container network-namespace-isolated `DialExecution` path — is preserved unchanged.

## Dependencies

- [F19](./../f19-tunnel-rework/README.md) and [F20](./../f20-client-tunnel-targets/README.md) —
  the tunnel UX (`--port <host>:<container>`) and client-owned target model being preserved,
  whose transport internals this feature replaces.
- [F17](./../f17-execution-lease/README.md) — execution owner/lease semantics the tunnel
  authorization reuses.
- [F13](./../f13-local-client-api/README.md) — the service-backed tunnel surface.
- [F12](./../f12-cluster-membership/README.md) — the shared cluster secret the tunnel IFAC is
  derived from.

## Scope

- Add a minimal private tunnel RNS transport as a new `rns` subpackage under
  [`internal/tunnel`](../../internal/tunnel): a **separate** `Transport` instance
  (`EnableTransport == false`) with exactly one Ygg-backed Backbone/TCP interface and no interface
  from the control RNS; it never participates in public RNS routing/discovery.
- Hold `Link`/`Channel`/`Buffer` as the only tunnel data primitives; enforce the
  **one local TCP connection = one RNS Link = one tunnel stream** model; drop every custom
  DATA/EOF/WINDOW_UPDATE/PREAMBLE/ACCEPT frame, stream ID, segmentation and retransmission,
  and per-stream flow control.
- Authorize a tunnel by RNS identity only: `Link.Identify()`, then the first application message
  is `Open { execution_id, port }`; allocator replies `OK` or a classified error. No separate
  tunnel HMAC challenge, no edge key derivation, no `ygg_peer_pubkey` in the control protocol.
- Advertise the minimum for creating a private tunnel transport through the control plane:
  `[ygg-ipv6]:port` (Backbone/TCP listener address) plus the tunnel RNS destination hash; never an
  Ygg public key.
- Derive the tunnel Backbone/TCP IFAC from the cluster secret, e.g.
  `network_name = "r1s-tunnel"`, `passphrase = HKDF(cluster-key, "r1s/tunnel-ifac/v1")`, so the
  IFAC only proves early cluster membership — never a substitute for owner authorization.
- Delete the embedded adapter
  [`internal/tunnel/yggdrasil`](../../internal/tunnel/yggdrasil) (`core.go`, `node.go`, `edge.go`,
  `mux.go`, `pair.go`, `framing.go`, `stream.go`, `dial.go`, `listen.go` + live-mesh/unit tests),
  the NodeKey contexts and `NodeKeyFromSeed`/`NodePubKey`, peer-key pinning, `PeerKey()`,
  `MaxPeerKeySize`, `ErrPeerKeyMismatch`, `Endpoint.PubKey` (if no longer needed), tunnel grants
  (`ExecutionTunnelGrant`/`Ack`, grant ID, TTL, `Registry.Mint`/`Accept`, single-use semantics),
  and the `ygg_peer_pubkey` proto field. Clean the proto directly; no backward compatibility.
- Very narrowly cut `tunnel.Conn` to `io.ReadWriteCloser` + `CloseWrite()`; keep `CloseRead()`
  only if the real forwarding path needs it, not for the old tests/adapter. Revisit the old
  `internal/tunnel` generic abstractions (PeerKey, Preamble, PreambleWriter, StreamOpener, old
  Listener shape, session registry) and drop what only the Ygg adapter needed.
- Remove the now-meaningless runner flags: `--tunnel-peer`, `--tunnel-endpoint-pubkey`, and the
  opaque-Ygg `--tunnel-endpoint`. Add clear config on `r1sd` (`--tunnel-enabled`,
  `--tunnel-interface ygg0`, `--tunnel-port 4242`) and keep `r1s serve --tunnel`. Public Ygg peers
  are entirely the system `yggdrasil` service's concern, never an `r1s` config.
- Keep the container side ([`Allocator.DialTunnelTarget`](../../internal/allocator) →
  containerd `DialExecution`) unchanged; the tunnel never falls back into the allocator namespace.
- Run `go mod tidy` and confirm direct dependencies `yggdrasil-go`, `ironwood`, and `gologme/log`
  disappear unless used elsewhere; Reticulum-Go remains the tunnel's only networking/crypto
  dependency.
- Compare the old embedded-Ygg tunnel and the new RNS-over-Ygg transport with a recorded benchmark
  before the old transport is finally removed.
- Add a two-stack e2e/live test scheme and verify the acceptance matrix below.

## Completion criteria

- No `import "github.com/yggdrasil-network/yggdrasil-go"` and no `import ironwood` in the repo.
- No Ygg-derived keys and no Ygg public keys in the control protocol; no tunnel grants; no custom
  packet mux/framing; no own DATA/EOF/WINDOW protocol.
- Tunnel data flows only through the separate private RNS transport over system Yggdrasil; the main
  RNS is used exclusively as the control plane.
- `r1s tunnel <execution> --port <host>:<container>` keeps its current behavior.
- Container-side namespace isolation is retained.
- `go build ./...`, `go vet ./...`, `go test -race ./...`, and `make check` pass.

## Tasks

- [F21-01 — Private tunnel RNS transport stack](./f21-01-tunnel-rns-stack.md)
- [F21-02 — Control-plane advertisement and tunnel Open protocol](./f21-02-tunnel-open-protocol.md)
- [F21-03 — Allocator authorization and container splice](./f21-03-allocator-auth-splice.md)
- [F21-04 — Client edge and CLI/config cleanup](./f21-04-client-edge-cli-cleanup.md)
- [F21-05 — Benchmark and live acceptance](./f21-05-benchmark-live-acceptance.md)
- [F21-06 — Remove embedded Ygg adapter, key machinery, and dependencies](./f21-06-remove-ygg-adapter.md)

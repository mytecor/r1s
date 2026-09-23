# F21-06 — Roll back private RNS tunnel and retain Ygg

**Status:** ⏳ Planned — F21-05 recorded a no-go for the private RNS data plane.

## Outcome

The embedded Yggdrasil adapter remains the execution-tunnel data plane. Experimental F21 code and
protocol surface that exist only for the rejected private RNS-over-system-Ygg path are removed or
reverted without changing the established F19/F20 tunnel UX, authorization, or container namespace
isolation.

## Scope

- Keep [`internal/tunnel/yggdrasil`](../../internal/tunnel/yggdrasil), `yggdrasil-go`, Ironwood,
  the authenticated mesh pair, multiplexed tunnel streams, Ygg-derived edge identity, peer-key
  pinning, and the existing grant/preamble/accept flow.
- Keep `r1s tunnel <execution> --port <host>:<container>`, the service-backed `LocalTunnel` API,
  client-managed target ports, and allocator-side `DialExecution` unchanged.
- Remove the experimental [`internal/tunnel/rns`](../../internal/tunnel/rns) data-plane package
  after retaining any transport-independent regression knowledge in tests or roadmap notes.
- Remove `TunnelOpen`, `TunnelOpenResult`, private tunnel destination advertisement, and related
  generated/schema fields if they have no user outside the rejected RNS path. Preserve Protobuf
  compatibility according to the repository rules; do not reuse field numbers.
- Remove benchmark-only connector wiring only after the final F21-05 record remains preserved in
  [F21-05](./f21-05-benchmark-live-acceptance.md).
- Reconcile README, architecture, CLI help, flags, and roadmap text with the retained embedded-Ygg
  implementation. Do not introduce a system-Ygg/raw-TCP replacement in this rollback.
- Run `go mod tidy` only for dependencies made unreachable by the rollback; do not remove the Ygg
  dependency graph used by the production tunnel adapter.

## Acceptance

- `internal/tunnel/yggdrasil` remains the selected client and allocator tunnel transport.
- The private RNS tunnel package and its unused advertisement/Open protocol surface are absent.
- Owner authorization, peer-key pinning, grant consumption, concurrent streams, half-close,
  classified teardown, and container-network-namespace isolation retain their existing tests.
- No tunnel application byte crosses the RNS control plane or an RNS `Channel`/`Buffer`.
- The F21-05 no-go measurements remain recorded as the reason for the rollback.
- `go build ./...`, `go vet ./...`, `go test -race ./...`, and `make check` pass.

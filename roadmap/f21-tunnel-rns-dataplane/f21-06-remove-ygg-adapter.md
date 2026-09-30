# F21-06 — Roll back private RNS tunnel and retain Ygg

**Status:** ✅ Complete — F21-05 recorded a no-go for the private RNS data plane and F21-06
rolled back the experimental RNS tunnel package, the benchmark harness, and the unused
`TunnelOpen`/`TunnelOpenResult`/`TunnelOpenError` protocol surface, retaining the embedded
Yggdrasil data plane.

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
- Removed the experimental `internal/tunnel/rns` data-plane package after retaining its
transport-independent regression knowledge in roadmap notes ([F21-05](./f21-05-benchmark-live-acceptance.md)).
- Removed `TunnelOpen`, `TunnelOpenResult`, `TunnelOpenError`, and the private tunnel destination
  advertisement slice: they had no user outside the rejected RNS path. Protobuf compatibility
  follows the repository's additive rule; the removed names are retired and never reused (no
  field numbers were consumed because the messages were separate types outside the Envelope
  oneof).
- Removed the benchmark-only connector wiring after the final F21-05 record was preserved in
  [F21-05](./f21-05-benchmark-live-acceptance.md).
- Deleted the `internal/tunnel/benchmark` old-vs-new harness together with the RNS connector.
- Ran `go mod tidy` for dependencies made unreachable by the rollback; the Ygg
dependency graph used by the production tunnel adapter is retained.

## Acceptance

- `internal/tunnel/yggdrasil` remains the selected client and allocator tunnel transport.
- The private RNS tunnel package and its unused advertisement/Open protocol surface are absent.
- Owner authorization, peer-key pinning, grant consumption, concurrent streams, half-close,
  classified teardown, and container-network-namespace isolation retain their existing tests.
- No tunnel application byte crosses the RNS control plane or an RNS `Channel`/`Buffer`.
- The F21-05 no-go measurements remain recorded as the reason for the rollback.
- `go build ./...`, `go vet ./...`, `go test -race ./...`, and `make check` pass.

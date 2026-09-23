# F21-05 — Benchmark and live acceptance

**Status:** 🔄 In progress — the old-vs-new benchmark harness (both connectors, the shared
workload suite, host record, and the two-stack connect/concurrent/smoke legs) is in
[`internal/tunnel/benchmark`](../../internal/tunnel/benchmark). The first recorded run surfaced a
**blocker**: the new RNS data plane cannot sustain a single-stream transfer that outlives ~10s
(see note below), so the go/no-go before F21-06 is currently **no-go** until it is fixed and the
full rows can be recorded.

## Outcome

A recorded benchmark compares the old embedded-Ygg tunnel and the new RNS Link + `Channel`/`Buffer`
transport (over Backbone/TCP over the system `yggdrasil` daemon), and a two-stack e2e/live scheme
proves the target data plane end-to-end. The benchmark must complete **before** the old transport is
finally removed in F21-06.

## Scope

- **Benchmark harness** (old vs new): runners for both transports measuring at minimum
  1 MiB, 10 MiB, a single TCP stream, 10 concurrent streams, and an HTTP request/response latency.
  Run both transports on the same host/environment; record numbers and host/environment/version in
  the task notes or a referenced run record.
- **Two-stack e2e/live scheme**: two private Reticulum stacks over a Backbone/TCP pair (and, where a
  real system `yggdrasil` daemon + homelab is available, through the live procedure in
  [F7-02](../f7-verification/f7-02-live-regression.md)) verifying:
  - owner can open a tunnel to their own execution;
  - another RNS identity gets `unauthorized`;
  - tunnel to a stopped/completed execution is rejected;
  - `port == 0` is rejected;
  - concurrent local TCP connections work independently;
  - closing one Link does not affect the others;
  - half-close client→container propagates EOF;
  - a payload larger than the RNS MTU passes through `Buffer`;
  - loss of the system Ygg/backbone connection closes the tunnel;
  - the tunnel Reticulum transport has only the Ygg interface;
  - a tunnel packet cannot be sent through a control-plane RNS interface;
  - `DialExecution` still cannot dial into the allocator's own namespace.

## Acceptance

- All benchmark rows exist for both the old and the new transport with the same methodology; the
  measured overhead of the RNS Link/Channel/Buffer path is explicitly recorded and reviewed for
  go/no-go before F21-06.
- The live/e2e matrix above passes under `go test -race` (with the live leg run per
  [F7-02](../f7-verification/f7-02-live-regression.md) when a real daemon is available); a skipped
  live gate is not a pass.
- `go build ./...`, `go vet ./...`, `go test -race ./...`, and `make check` pass.

## Notes

- **Blocker found by the first recorded run (2026-09-23):** a sustained 10 MiB single-stream
  transfer over the private tunnel RNS transport fails with `link not ready` at exactly ~10.0s;
  the client Link flips ACTIVE → STALE at that instant while the allocator stays ACTIVE. Root
  cause is Reticulum-Go's keepalive/staleness floor (`keepalive` = `KeepaliveMinSec` = 5s on
  low-RTT links, `staleTime` = 10s) not the Backbone evWrite race. The BackendGo workaround does
  not cover it. Recorded in [BACKLOG entry 10](../BACKLOG.md) with the measured curve (1 MiB
  ~2.3s, 4 MiB ~6.7s, 10×1 MiB concurrent ~9.4s all pass; 10 MiB dies at ~10s) and a gated
  regression test (`R1S_TEST_SUSTAINED_TUNNEL=1`) in
  [`internal/tunnel/rns`](../../internal/tunnel/rns). No benchmark rows can be recorded at the
  acceptance sizes until the transfer survives >10s — this is the F21-06 go/no-go gate.

- If Link establishment turns out to be too expensive, returning multiplexing is a future, separate
  optimization — recorded in [BACKLOG.md](../BACKLOG.md); it is not a blocker for this feature.

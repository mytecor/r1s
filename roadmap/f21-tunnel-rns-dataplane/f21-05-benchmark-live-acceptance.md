# F21-05 — Benchmark and live acceptance

**Status:** ⏳ Planned

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

- If Link establishment turns out to be too expensive, returning multiplexing is a future, separate
  optimization — recorded in [BACKLOG.md](../BACKLOG.md); it is not a blocker for this feature.

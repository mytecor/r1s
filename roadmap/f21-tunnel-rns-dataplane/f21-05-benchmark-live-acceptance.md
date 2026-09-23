# F21-05 — Benchmark and live acceptance

**Status:** ✅ Complete — the full old-vs-new rows are measured on the same host/methodology and
the decision is **no-go for the private RNS tunnel data plane**. F21-06 retains Ygg and rolls back
the experimental RNS path. The sustained >10s correctness blocker is closed and its regression
gate passes, but the optimized RNS path remains 26.7–39.9× slower in the representative benchmark
rows. The old-vs-new harness is in
[`internal/tunnel/benchmark`](../../internal/tunnel/benchmark).

## Status history

- **2026-09-23 (reported):** a sustained 10 MiB single-stream transfer over the private tunnel RNS
data plane died with `link not ready` at ~10.0s (client Link → STALE). Go/no-go before F21-06 was
**no-go**.
- **2026-09-23 (fixed):** the compat adapter's per-edge liveness beacon (see note below, BACKLOG
decisions 10/20) keeps both Links out of the v1.2.0 staleness watchdog window; the 10 MiB transfer
now completes byte-for-byte. The gated test
`R1S_TEST_SUSTAINED_TUNNEL=1 go test ./internal/tunnel/rns/ -run TestSustainedTransferOutlivesStaleTime -count=1`
passes.
- **2026-09-23 (decision):** even after compatible workarounds disable automatic bzip2 and reduce
  the Go readiness poll from 5ms to 100µs, the RNS path reaches only 4.80 MB/s for 10 MiB and 5.70
  MB/s for 10 concurrent streams, versus 159.25 and 227.16 MB/s on Ygg. The remaining costs are
  inherent to the Python-compatible Channel/Buffer path, so the F21-06 decision is **no-go**.

## Outcome

A recorded benchmark compares the embedded-Ygg tunnel and the experimental RNS Link +
`Channel`/`Buffer` transport over Backbone/TCP. Its result selects Ygg for the production tunnel
and prevents the removal that the original F21-06 plan proposed.

## Scope

- **Benchmark harness** (old vs new): runners for both transports measuring at minimum
  1 MiB, 10 MiB, a single TCP stream, 10 concurrent streams, and an HTTP request/response latency.
  Run both transports on the same host/environment; record numbers and host/environment/version in
  the task notes or a referenced run record.
- **Prototype verification scheme**: two private Reticulum stacks over a Backbone/TCP pair verify
  the following deterministic behavior. The originally planned real-system-Ygg production live
  leg was cancelled after the benchmark selected no-go; a rejected data plane is not promoted by
  completing additional live acceptance.
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

- All benchmark rows exist for both transports with the same methodology, the RNS overhead is
  explicitly recorded, and the F21-06 decision is no-go.
- The deterministic two-stack matrix remains regression coverage for the experimental findings.
  The production live leg is cancelled rather than counted as a pass because F21-06 removes the
  prototype instead of deploying it.
- `go build ./...`, `go vet ./...`, `go test -race ./...`, and `make check` pass.

## Measurements

The recorded old-vs-new rows, both transports run on the same host with the same methodology
([`DefaultSuite`](../../internal/tunnel/benchmark/bench.go) sizes: 1 MiB / 10 MiB single-stream
transfers, 200 HTTP round-trips over an established connection, 10 concurrent × 1 MiB streams,
20 fresh-connection opens). Recorded 2026-09-23 by
`R1S_TEST_F21_BENCHMARK=recorded go test ./internal/tunnel/benchmark/ -run TestRecordedBenchmark \
-count=1` (unraced; see the Notes for why the recorded run must not be `-race`ed).

Host: `darwin/arm64 | cpu=8 | go=1.27.1 | commit=1b385c1` plus the working-tree
Reticulum-Go compatibility workarounds recorded below.

| row | Ygg (old embedded) | RNS (new private) | ratio (RNS/Ygg) |
| --- | ---: | ---: | ---: |
| Transfer 1 MiB | 8.0 ms (125.46 MB/s) | 213.2 ms (4.69 MB/s) | 26.7× |
| Transfer 10 MiB | 62.8 ms (159.25 MB/s) | 2085.5 ms (4.80 MB/s) | 33.2× |
| HTTP round-trip | 144 µs | 749 µs | 5.2× |
| Concurrent 10×1 MiB | 44.0 ms total (227.16 MB/s) | 1754.9 ms total (5.70 MB/s) | 39.9× |
| Open (new connection) | 118 µs | 288 µs | 2.4× |

Raw rows as emitted by the driver:

```
BenchmarkYgg/Transfer/1MiB-8          1    7970875.00 ns/op
  1048576 bytes        125.46 MB/s
BenchmarkYgg/Transfer/10MiB-8          1   62794708.00 ns/op
  10485760 bytes        159.25 MB/s
BenchmarkYgg/HTTP-8          1     143728.15 ns/op
BenchmarkYgg/Concurrent-8          1    4402262.50 ns/op
  10485760 bytes        227.16 MB/s
BenchmarkYgg/Open-8          1     118120.85 ns/op

BenchmarkRNS/Transfer/1MiB-8          1  213162875.00 ns/op
  1048576 bytes          4.69 MB/s
BenchmarkRNS/Transfer/10MiB-8          1  2085492209.00 ns/op
  10485760 bytes          4.80 MB/s
BenchmarkRNS/HTTP-8          1     749447.69 ns/op
BenchmarkRNS/Concurrent-8          1  175487712.50 ns/op
  10485760 bytes          5.70 MB/s
BenchmarkRNS/Open-8          1     287581.20 ns/op
```

The compat writer now selects the standard uncompressed `StreamDataMessage` representation for the
tunnel data plane. Python RNS already accepts that representation, so this is a sender policy and
not a wire-format fork. It avoids Reticulum-Go v1.2.0's three bzip2 probes for every payload over 32
bytes while upstream [issue #18](https://github.com/Quad4-Software/Reticulum-Go/issues/18) tracks a
supported compression-policy API. The compat layer also replaces Channel's 5ms `WaitReady` poll
with a reusable 100µs timer while upstream
[issue #19](https://github.com/Quad4-Software/Reticulum-Go/issues/19) tracks an event-driven API.
Against the first recorded rows the two workarounds make RNS 11.2× faster at 1 MiB, 8.4× faster at
10 MiB, 8.4× faster for HTTP round-trips, and 5.1× faster for the concurrent workload. Shortening
the poll alone improves the 10 MiB row by another 20%, from 4.00 to 4.80 MB/s; reducing it further
to 10µs produced no material improvement, showing that per-packet proofs, IFAC work, small-MDU
packet processing and Backbone writes are now the limit. The concurrent row reports total wall
time on both sides; the previous table mixed per-stream `ns/op` with total time and overstated its
ratio by 10×.

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
  **Resolved 2026-09-23** (Status above): the compat adapter's per-edge liveness beacon closes it.
  After disabling automatic compression for tunnel writes, 10 MiB completes in ~2.5s and no longer
  crosses `staleTime`; the gate therefore sends 64 MiB and passes in ~12.7s, preserving the same
  >10s liveness assertion at the faster data rate. Run this high-packet-volume gate explicitly and
  without `-race`; it remains excluded from `make check`.

- Link establishment was not the deciding cost. The small-MDU reliable Channel path dominates
  steady-state transfer, so multiplexing Links would not reverse the no-go decision.

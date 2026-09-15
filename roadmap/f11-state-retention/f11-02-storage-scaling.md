# F11-02 — Measure and reduce persistence cost

**Status:** ✅ Implemented; reproducible benchmarks added and measured on 2026-09-15; decision recorded (keep the whole-snapshot design)

## Outcome

Persistence remains predictable as execution history grows.

## Scope

- Benchmark transition latency, snapshot size, and lock time at increasing history sizes.
- Use measurements to decide whether per-record bbolt transactions are needed.
- If changing layout, include versioned migration, failure recovery, and rollback documentation.

## Acceptance

- Benchmarks report history size and transition cost reproducibly. ✅
- Any new layout preserves atomic assignment, release intent, and replay protection under failure.
  (Not applicable: the existing whole-snapshot layout is kept; the decision below documents why.)
- Run `make check` and the feature-specific checks described above. ✅

## What was added

`internal/allocator/scaling_bench_test.go` benchmarks the allocator's persistence path against the
real bbolt store at retained-history sizes 0 / 100 / 1000 / 4000 (the realistic steady-state ceiling
under the `maxRecords` guard — see below). Each allocator is rebuilt from scratch so retained
state is the only variable; history is filled in memory and attached to a bbolt store with one
snapshot commit so per-cycle fsync during warm-up is excluded from the measurement.

- `BenchmarkTransition_FullCycle_Memory` / `_Bbolt` — one full request→offer→assign→complete cycle
  (three persists per cycle) with and without a bbolt store: the per-workload persistence cost.
- `BenchmarkSnapshot_Size` — serialized snapshot size under `persistLocked` plus JSON-marshal cost.
- `BenchmarkSnapshot_CommitTime` — one `persistLocked` against bbolt (JSON marshal + one
  transaction), i.e. the lock-hold cost of a transition.
- `BenchmarkRestart_Load` — reopening the bbolt file and reconstructing in-memory state (startup cost).

`scripts/storage-bench.sh` runs all of them at a fixed `-benchtime` (default `3x`) and prints a
reproducible header (host, commit, Go version) so results can be compared across machines and runs.

## Measurements (2026-09-15, `Darwin arm64`, Go 1.27.1, containerd test host not required)

| retained cycles | snapshot size | JSON marshal | bbolt commit | full cycle (memory) | full cycle (bbolt) | restart load |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 384 B | 0.0008 ms | 7.3 ms | 26 µs | 72 ms | 0.09 ms |
| 100 | 1.4 MB | 0.8 ms | 9.3 ms | 52 µs | 74 ms | 1.0 ms |
| 1000 | 12.6 MB | 6.2 ms | 13.3 ms | 111 µs | 107 ms | 7.4 ms |
| 4000 | 41.2 MB | 18.6 ms | 22.6 ms | 299 µs | 176 ms | 20.0 ms |

All values are three-iteration means from `scripts/storage-bench.sh 3x`.

## Analysis and decision

- **Snapshot size scales linearly** (roughly 10 KB of JSON per retained offer+execution+replay
  entry), so the allocator-side JSON-marshal cost under the mutex is O(N): ~19 ms at 4000 cycles.
- **The bbolt commit is fsync-dominated** and grows modestly with size (~7→23 ms) because it writes
  one whole blob per transaction.
- **Full cycle with bbolt is 72→176 ms** as history fills; the whole-snapshot rewrite dominates at
  the retention ceiling.
- The `maxRecords` guard bounds realistic steady state: `2*offers + tombstones + 2 <= maxRecords`
  (offers persist until `Sweep`), so under the default `DefaultMaxRecords=10000` the ceiling is
  roughly **4000 retained cycles** before collection. The 4000 bucket is that worst case.

**Decision: keep the whole-snapshot design; no per-record bbolt transactions now.** Per-record
transactions would lower each transition to O(delta) writes, but they complicate the atomicity the
protocol needs (assignment and release intent must commit together, and replay context must be
reconstructible across restart) at a retained-history ceiling where the measured worst case is
~176 ms per cycle on this hardware — well within the project's single-node, capacity-limited model.
The benchmarks are kept as the reproducible baseline: if retained history later grows beyond the
guard ceiling or per-transition cost becomes a bottleneck, the measurements in this file are the
reference for revisiting a per-record layout (with a versioned migration, as the task scope
requires).

## Notes

F11-01 (retention contract) is implemented; this task closes the storage-scaling milestone's
measurement half. The live Linux leg (crash/store-failure injection during `Sweep`) remains tracked
in [F11-01](./f11-01-retention-contract.md) and runs on the [F7-02](./../f7-verification/f7-02-live-regression.md)
runner once it exists.

Track unresolved cross-feature decisions in [BACKLOG.md](../BACKLOG.md).

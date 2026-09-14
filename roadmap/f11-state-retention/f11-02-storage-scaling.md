# F11-02 — Measure and reduce persistence cost

**Status:** ⏳ Planned

## Outcome

Persistence remains predictable as execution history grows.

## Scope

- Benchmark transition latency, snapshot size, and lock time at increasing history sizes.
- Use measurements to decide whether per-record bbolt transactions are needed.
- If changing layout, include versioned migration, failure recovery, and rollback documentation.

## Acceptance

- Benchmarks report history size and transition cost reproducibly.
- Any new layout preserves atomic assignment, release intent, and replay protection under failure.
- Run `make check` and the feature-specific checks described above.

## Notes

Track unresolved cross-feature decisions in [BACKLOG.md](../BACKLOG.md).

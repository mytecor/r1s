# F11 — Bounded durable history

Corresponds to the [F11 milestone](../../ROADMAP.md#f11-bounded-durable-history).

**Status:** 🚧 Implemented; retention acceptance tests pass; F11-02 storage benchmarks added and measured (2026-09-15)

## Outcome

- Finished executions and obsolete offers are collected without allowing old commands to restart work.
- Persistence remains predictable as execution history grows; reproducible benchmarks measure the cost and record the keep-whole-snapshot decision in [F11-02](./f11-02-storage-scaling.md).

## Dependencies

F5, F6, F8. See [ROADMAP.md](../../ROADMAP.md).

## Scope and completion criteria

Complete the behavior and acceptance checks in each task below.

## Tasks

- [F11-01 — Retention and replay tombstones](./f11-01-retention-contract.md)
- [F11-02 — Measure and reduce persistence cost](./f11-02-storage-scaling.md) ✅ implemented and measured

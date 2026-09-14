# F8-02 — Durable execution revisions

**Status:** 🚧 Implemented; direct acceptance tests pending

## Outcome

Execution state ordering survives clock rollback and process restart.

## Scope

- Add an additive monotonic revision to ExecutionState and durable allocator records.
- Increment revision on observable state changes; keep occurred_at for presentation.
- Define compatibility for legacy states without revisions and reject contradictory equal revisions.

## Acceptance

- A newer revision is accepted after clock rollback.
- Older and reordered states do not regress client state.
- Revisions survive allocator and client restarts; terminal state stays terminal.
- Run `make check` and the feature-specific checks described above.

## Notes

Track unresolved cross-feature decisions in [BACKLOG.md](../BACKLOG.md).

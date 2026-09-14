# F8-02 — Durable execution revisions

**Status:** 🚧 Implemented; unit and protocol acceptance tests pass; live verify pending

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

## Verification status

Covered by `internal/client/acceptance_test.go` (`TestClockRollbackPreservesRevisionOrdering`,
`TestContradictoryEqualRevisionRejected`):

- A later monotonic revision is accepted even when `occurred_at` goes backwards (clock rollback).
- Older and reordered states never regress the client's durable snapshot.
- Contradictory equal revisions are rejected; an identical equal revision is accepted as a duplicate.
- Revisions are durable in allocator/client state and survive restart (covered by the allocator
  restart tests and persisted `revision`).


## Notes

Track unresolved cross-feature decisions in [BACKLOG.md](../BACKLOG.md).

# F11-01 — Retention and replay tombstones

**Status:** 🚧 Implemented; retention acceptance tests pass

## Outcome

Finished executions and obsolete offers are collected without allowing old commands to restart work.

## Scope

- Define retention defaults and limits, expiry origin, clock behavior, and explicit expired-result responses.
- Separate removable result bodies from minimal replay-prevention records.
- Define the supported replay horizon before bounding tombstones; arbitrary delayed assignment must not become a new execution.
- Make cleanup transactional, observable on store failure, and restart-safe.

## Acceptance

- Results remain available for the promised interval and are removed afterward.
- Old assignments replayed after cleanup cannot start another workload.
- Crash and store-failure injection during cleanup preserve capacity and authority.
- Run `make check` and the feature-specific checks described above.

## Verification status

Covered by `internal/allocator/acceptance_test.go` (`TestCollectedResultCannotRestartWork`,
`TestTombstonesSurviveRestart`) and `internal/allocator/allocator_test.go`
(`TestReplayRetentionIsBounded`):

- A finished execution is collected after its retention; capacity returns.
- An old assignment replayed after cleanup returns `EXPIRED` and cannot start another workload.
- Tombstones survive allocator restart, so a restarted allocator still refuses the collected command
  and keeps the slot free.

Still to verify on a live Linux runner: crash and store-failure injection during `Sweep` preserve
capacity and authority (the store-failure path is exercised locally, not under a real crash).


## Notes

Track unresolved cross-feature decisions in [BACKLOG.md](../BACKLOG.md).

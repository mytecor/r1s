# F11-01 — Retention and replay tombstones

**Status:** ✅ Implemented; retention acceptance tests and live crash injection pass

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

Live verification (2026-09-15, `mytecor-homelab`, containerd 2.3.4 / runc 1.4.3 / Go 1.26.7) is covered by
`internal/acceptance/live_quota_replay_test.go` (`TestLiveSweepCrashPreservesCapacityAndAuthority`),
which adds a `--sweep-interval` flag to `r1sd` to drive cleanup deterministically: a short-retention
workload completes, the daemon is SIGKILLed while the terminal execution is still pending collection
(hard crash inside the bounded-history `Sweep` window), and a restart from the same store reconciles
without duplicating work, collects the result into a durable tombstone, refuses the replayed
assignment with `EXPIRED` (never a new container), and admits a fresh request — capacity and
authority are preserved across a real crash. Store-failure injection during `Sweep` remains
deterministically covered (rollback to pre-sweep state), since a live bbolt store cannot be failed
injectively; that limitation is recorded in [BACKLOG.md](../BACKLOG.md).


## Notes

Track unresolved cross-feature decisions in [BACKLOG.md](../BACKLOG.md).

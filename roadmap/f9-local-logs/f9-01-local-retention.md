# F9-01 — Bounded local container logs

**Status:** 🚧 Implemented; unit acceptance tests pass; live Linux restart pending

## Outcome

The allocator retains bounded stdout/stderr locally, including after a task fails or the daemon restarts.

## Scope

- Replace discarded IO with local execution-owned log storage behind the runtime boundary.
- Define byte caps, stream separation, truncation metadata, directory permissions, and retention.
- Do not attach logs to lifecycle state, errors, inspect, or result responses.
- No completion, failure, cancellation, reconnect, or background retry can initiate log transfer.

## Acceptance

- A failed workload's stdout/stderr remains locally readable after allocator restart.
- A noisy workload cannot exceed the configured local storage budget.
- A transport spy observes zero log bytes during completion, failure, inspect, result, and reconnection.
- Run `make check` and the feature-specific checks described above.

## Verification status

Covered by `internal/logstore/store_test.go` (`TestCaptureBoundedRetentionAndTruncation`,
`TestReadOffsetValidationAndRemove`, `TestReserveAfterRestartKeepsLimitAndBudget`) and
`internal/allocator/acceptance_test.go` (`TestLogsOnlyByExplicitOwnerRequest`):

- A noisy stream is bounded by its per-stream cap and marked truncated; reads past the cap are EOF.
- Offsets beyond retained data and unknown streams are explicit conflicts; remove makes logs missing.
- Reservations and their aggregate budget survive a new store over the same directory.
- Lifecycle traffic (request, assign, inspect) never reads the log store; only an explicit owner
  request does. The response is checksummed and never carries execution state.

Still to verify on a live Linux runner: retained stdout/stderr stay readable after a real `r1sd`
restart with the containerd log-writer shim, and a transport spy observes zero log bytes during
completion, failure, inspect, result, and reconnection.


## Notes

Track unresolved cross-feature decisions in [BACKLOG.md](../BACKLOG.md).

# F8-01 — Explicit command errors

**Status:** 🚧 Implemented; unit and protocol acceptance tests pass; live verify pending

## Outcome

Clients distinguish authenticated allocator rejection from missing responses.

## Scope

- Add an additive correlated error response with stable codes and retry semantics.
- Persist replay responses; validate authenticated sender and command correlation on the client.
- Surface refusals in the CLI without changing asynchronous assignment.
- An assignment timeout remains ambiguous and never authorizes assignment to a second allocator.
- Error detail contains bounded diagnostics, never container stdout/stderr.

## Acceptance

- Capacity rejection is observable without waiting for a network timeout.
- Unknown and unauthorized commands cannot disclose another client's execution data.
- Duplicate rejection remains stable after restart; transport loss cannot produce duplicate execution.
- Run `make check` and the feature-specific checks described above.

## Verification status

Covered by `internal/allocator/acceptance_test.go` (`TestCapacityRejectionReturnsCorrelatedCommandError`,
`TestUnauthorizedAndUnknownCommandsAreOpaque`) and `internal/protocol/feedback_test.go`
(`TestCommandErrorValidation`):

- A capacity rejection is a correlated `CommandError` with code `CAPACITY` and retry flag, and the same
  rejection replays stably without consuming capacity or starting work.
- Unknown and unauthorized commands answer an opaque `NOT_FOUND` and never disclose another client's
  execution state; workload output never appears in an error detail.
- The wire validation rejects unknown error codes, missing correlation, and oversized detail.

Still to verify on a live Linux runner: no duplicate execution under injected transport loss while a
rejection is in flight (the deterministic replay tests cover the allocator side; the CLI aggregates
rejections without unsafe failover).


## Notes

Track unresolved cross-feature decisions in [BACKLOG.md](../BACKLOG.md).

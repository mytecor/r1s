# F1-04 — Complete protocol foundations

**Status:** Planned

## Outcome

The initial domain core is safe under replay, expiration, runtime failure, and concurrent delivery.

## Scope

- Add explicit message-level replay tracking and bounded retention.
- Test duplicate request, assignment, and cancellation delivery.
- Test offer expiry and capacity release with an injected clock.
- Define completion/failure callbacks from the runtime into allocator state.
- Add race-enabled tests.

## Acceptance

- `go test -race ./...` passes.
- All lifecycle transitions have positive and authorization failure coverage.

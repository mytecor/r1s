# F13-02 — Route CLI workflows through the local API

**Status:** ✅ complete; service-backed CLI acceptance test passes

## Outcome

The existing CLI can use the persistent local service without losing an explicit direct mode for
bootstrap, recovery, and simple deployments.

## Scope

- Add socket selection and clear daemon-unavailable errors.
- Preserve command output and exit semantics for request, inspect, cancel, list, result, and logs.
- Document service startup, socket ownership, direct mode, and migration behavior.

## Acceptance

- CLI acceptance tests run the same workflow through direct and service-backed modes.
- Failure to reach the socket never silently creates a second client identity or assignment.
- `make check` passes.

## Notes

Whether daemon mode becomes the CLI default is an implementation-time compatibility decision and
must be recorded in [BACKLOG.md](../BACKLOG.md) until resolved.

## Implementation

- The global `--socket <path>` flag routes `request`, `list`, `inspect`, `result`, `cancel`, and
  `logs` through `internal/localapi` (a thin gRPC client) to a running `r1s serve`. Direct mode
  (no `--socket`) remains the default and the only way to run `serve` or `cluster`.
- An unreachable socket is a hard error (`local r1s service is not running at <path>`); the CLI
  never falls back to building a second client identity or creating an assignment it cannot verify.
- Command output and exit semantics are preserved: `localCLI` reuses the same JSON decoder, flag
  parsing, and presentation as direct mode.
- Acceptance: `TestServiceBackedCLIMatchesDirectWorkflow` in `cmd/r1s` drives the full
  request → list → inspect → cancel workflow through `--socket` against a real
  `client.Client` + `allocator.Allocator` over an in-memory transport served behind a real local API
  socket, and confirms an unreachable socket errors cleanly.

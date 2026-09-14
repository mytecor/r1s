# F8-01 — Explicit command errors

**Status:** 🚧 Implemented; direct acceptance tests pending

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

## Notes

Track unresolved cross-feature decisions in [BACKLOG.md](../BACKLOG.md).

# F13-02 — Route CLI workflows through the local API

**Status:** ⏳ Planned

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

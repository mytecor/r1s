# F11-01 — Retention and replay tombstones

**Status:** 🚧 Implemented; acceptance tests pending

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

## Notes

Track unresolved cross-feature decisions in [BACKLOG.md](../BACKLOG.md).

# F10-01 — Allocator resource profiles

**Status:** 🚧 Implemented; acceptance tests and live Linux enforcement pending

## Outcome

Allocator-defined resource classes enforce CPU, memory, and process limits.

## Scope

- Map local class profiles through runtime-neutral resource limits into the containerd adapter.
- Validate profiles before advertising capacity and preserve execution profiles across restart.
- The client cannot override local limits; define whether GPU classes need explicit device allocation.

## Acceptance

- A live Linux workload is subject to its configured memory, CPU, and process limits.
- Invalid profiles fail startup; existing executions retain their admitted limits after recovery.
- Run `make check` and the feature-specific checks described above.

## Notes

Track unresolved cross-feature decisions in [BACKLOG.md](../BACKLOG.md).

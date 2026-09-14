# F10 — Local admission and resource limits

Corresponds to the [F10 milestone](../../ROADMAP.md#f10-local-admission-and-resource-limits).

**Status:** 🚧 Implemented; acceptance tests and live Linux enforcement pending

## Outcome

- Allocator-defined resource classes enforce CPU, memory, and process limits.
- An allocator controls which clients can reserve capacity and how much they can reserve.

## Dependencies

F3, F8. See [ROADMAP.md](../../ROADMAP.md).

## Scope and completion criteria

Complete the behavior and acceptance checks in each task below.

## Tasks

- [F10-01 — Allocator resource profiles](./f10-01-resource-profiles.md)
- [F10-02 — Authenticated admission and quotas](./f10-02-identity-quotas.md)

# F8 — Protocol feedback and state revisions

Corresponds to the [F8 milestone](../../ROADMAP.md#f8-protocol-feedback-and-state-revisions).

**Status:** 🚧 Implemented; unit and protocol acceptance tests pass; live verify pending

## Outcome

- Clients distinguish authenticated allocator rejection from missing responses.
- Execution state ordering survives clock rollback and process restart.

## Dependencies

F1, F2, F4, F5. See [ROADMAP.md](../../ROADMAP.md).

## Scope and completion criteria

Complete the behavior and acceptance checks in each task below.

## Tasks

- [F8-01 — Explicit command errors](./f8-01-command-errors.md)
- [F8-02 — Durable execution revisions](./f8-02-state-revisions.md)

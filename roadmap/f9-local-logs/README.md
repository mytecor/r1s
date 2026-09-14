# F9 — Local logs and explicit retrieval

Corresponds to the [F9 milestone](../../ROADMAP.md#f9-local-logs-and-explicit-retrieval).

**Status:** ✅ Implemented; live Linux restart verified on `mytecor-homelab` on 2026-09-15

## Outcome

- The allocator retains bounded stdout/stderr locally, including after a task fails or the daemon restarts.
- An execution owner can separately request a bounded range of locally retained logs.

## Dependencies

F3, F4, F8, and the F11 retention contract. See [ROADMAP.md](../../ROADMAP.md).

## Scope and completion criteria

Complete the behavior and acceptance checks in each task below.

## Tasks

- [F9-01 — Bounded local container logs](./f9-01-local-retention.md)
- [F9-02 — Explicit authenticated log retrieval](./f9-02-requested-retrieval.md)

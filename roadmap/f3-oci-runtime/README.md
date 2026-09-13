# F3 — OCI runtime

## Outcome

Selected executions run through containerd with durable metadata and restart reconciliation.

## Completion criteria

- Images can be pinned by digest and pulled safely.
- Start and stop are idempotent by execution ID.
- Deadlines and maximum runtime are enforced locally.
- Allocator restart reconciles stored state with containerd state.

## Tasks

- [F3-01 — Integrate containerd](./f3-01-containerd-adapter.md)


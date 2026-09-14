# F3 — OCI runtime

**Status:** In progress

## Outcome

Selected executions run through containerd with durable metadata and restart reconciliation.

## Completion criteria

- Images can be pinned by digest and pulled safely.
- Start and stop are idempotent by execution ID.
- Deadlines and maximum runtime are enforced locally.
- Allocator restart reconciles stored state with containerd state.

## Tasks

- [F3-01 — Integrate containerd](./f3-01-containerd-adapter.md)
- [F3-02 — Persist and reconcile allocator state](./f3-02-reconciliation.md)

## Implementation

- [`internal/runtime/containerd`](../../internal/runtime/containerd/) implements digest-pinned image
  pulls, OCI spec construction, idempotent start and stop, exit reporting, cleanup, and local policy
  deadlines in the isolated `r1s` containerd namespace.
- [`cmd/r1sd`](../../cmd/r1sd/) connects to containerd at startup and exposes address, namespace, and
  snapshotter configuration.
- Unit tests exercise the adapter state machine without a daemon; a gated live harness covers a
  real fixture workload and cancellation.

## Remaining

- Run the gated lifecycle harness against a Linux containerd installation and record the result in
  [F3-01](./f3-01-containerd-adapter.md).
- Choose the durable allocator store and implement restart reconciliation in
  [F3-02](./f3-02-reconciliation.md).

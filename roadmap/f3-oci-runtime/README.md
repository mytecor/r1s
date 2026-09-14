# F3 — OCI runtime

**Status:** Complete

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
  real fixture workload, cancellation, running-task reattachment, and offline completion recovery.
- [`internal/store/bolt`](../../internal/store/bolt/) commits versioned allocator snapshots in a
  local bbolt database bound to the allocator identity.
- `r1sd` restores offers, executions, capacity, and replay records before accepting messages, then
  reconciles non-terminal executions without restarting missing workloads.

## Live verification

- On 2026-09-14, `TestContainerdFixtureLifecycle` and `TestContainerdFixtureRecovery` passed on
  `mytecor-homelab` (NixOS Linux x86_64) using Go 1.26.7, containerd 2.3.4, runc 1.4.3, and a
  digest-pinned Alpine fixture.

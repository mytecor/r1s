# F5 — Partition recovery

**Status:** Complete

## Outcome

The complete system tolerates duplicate messages, process restarts, and temporary loss of client
connectivity without duplicate execution or premature termination.

## Completion criteria

- An assigned workload completes while its client is offline.
- The client later discovers final state and retrieves the retained result.
- Replayed assignment and cancellation messages are harmless.
- Allocator and client restarts recover from durable state.

## Tasks

- [F5-01 — Add partition recovery acceptance](./f5-01-recovery-acceptance.md)

## Implementation

- [`internal/acceptance`](../../internal/acceptance/) contains a gated live harness using a real
  loopback RNS Channel, a separate `r1sd` process, durable bbolt state for both peers, and a real
  containerd task.
- The harness repeats request, assignment, and cancellation envelopes and directly verifies that
  only one labelled container exists for each execution.
- It disconnects the client, restarts `r1sd` while the task is running, waits for completion while
  the client remains offline, restarts the client, replays the exact durable assignment, and
  retrieves the retained terminal state through a fresh inspect.

## Verification

```sh
RUN_PARTITION_RECOVERY=1 \
R1S_CONTAINERD_TEST_IMAGE='registry.example/image@sha256:...' \
go test ./internal/acceptance/ -run TestPartitionRecovery -v
```

The test skips without its gate, so `make check` remains portable.

## Live verification

- On 2026-09-14, `TestPartitionRecovery` passed on `mytecor-homelab` in 24.12 seconds using Go
  1.26.7, containerd 2.3.4, runc 1.4.3, and a digest-pinned Alpine fixture.
- The run observed exactly one labelled container across duplicate assignment delivery, kept it
  running while the client was offline and `r1sd` restarted, recovered exit code 23 after client
  restart, and safely replayed cancellation for a second workload.

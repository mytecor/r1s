# F5-01 — Add partition recovery acceptance

**Status:** Complete

## Outcome

Create an end-to-end harness that interrupts RNS connectivity and restarts both sides during a real
container execution.

## Acceptance

- The workload starts exactly once.
- It is not stopped by client disconnection.
- Its terminal state and result are available after reconnection.

## Implemented

- A gated harness builds and runs `r1sd` as a separate process connected to an in-process client
  through a real loopback RNS UDP/Channel pair.
- The client and allocator use persistent identities and separate bbolt stores. The harness closes
  and reconstructs the client, and terminates and restarts the allocator process from the same
  durable state.
- Containerd is observed independently by execution label: duplicate request and assignment
  delivery leaves exactly one running task, allocator restart reattaches to it, and assignment
  replay after offline completion does not recreate it.
- A fresh inspect retrieves the completed phase and expected exit code after client reconnection.
  A second real workload verifies that a stable cancellation envelope is harmless when delivered
  twice.

## Verification

```sh
RUN_PARTITION_RECOVERY=1 \
R1S_CONTAINERD_TEST_IMAGE='registry.example/image@sha256:...' \
go test ./internal/acceptance/ -run TestPartitionRecovery -v
```

The live run requires Linux and a reachable containerd daemon. `CONTAINERD_ADDRESS` and
`CONTAINERD_SNAPSHOTTER` select non-default daemon settings.

## Live verification

- On 2026-09-14, the gated test passed on `mytecor-homelab` in 24.12 seconds with Go 1.26.7,
  containerd 2.3.4, runc 1.4.3, and the digest-pinned Alpine fixture.
- The observed workload stayed running through client disconnection and allocator restart,
  completed offline with exit code 23, and was retrieved after client restart. No replay recreated
  the completed container, and duplicate cancellation of the second workload remained harmless.

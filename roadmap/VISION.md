# r1s high-level plan

## Goal

r1s becomes a decentralized execution fabric in which intermittently connected owners can run OCI
workloads on independent allocators over RNS. The system has no mandatory central service, global
scheduler, membership database, or cluster-wide desired state.

The goal is reached when two independently configured RNS nodes can discover one another, negotiate
capacity, start a container, survive an owner network partition, and later deliver the final state
and result without duplicate execution.

## Milestones

| Milestone | Depends on | Complete when | Feature |
| --- | --- | --- | --- |
| 1. Protocol foundation | — | Versioned messages, allocator transitions, and adapter contracts pass deterministic tests | [F1](./f1-protocol-foundation/README.md) |
| 2. RNS transport | F1 | Two Go processes discover and exchange authenticated envelopes through Reticulum-Go | [F2](./f2-rns-transport/README.md) |
| 3. OCI execution | F1 | An assigned workload starts and stops through containerd with reconciliation after restart | [F3](./f3-oci-runtime/README.md) |
| 4. Owner workflow | F2, F3 | A CLI can request, select, inspect, cancel, and retrieve a result | [F4](./f4-owner-workflow/README.md) |
| 5. Partition recovery | F2–F4 | End-to-end tests prove replay safety, network-partition survival, and result recovery | [F5](./f5-partition-recovery/README.md) |

## Invariants

- RNS identity is the authorization root for remote actions.
- An owner disconnect never stops an assigned execution by itself.
- No component requires a globally consistent registry or scheduler.
- Control messages remain small, versioned, and asynchronous.
- OCI/runtime details do not leak into transport code.
- Each milestone ends in a buildable, tested state.


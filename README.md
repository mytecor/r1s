# r1s

r1s is a decentralized OCI workload execution fabric over the Reticulum Network Stack (RNS).
Owners publish workload demand, independent allocators offer local capacity, and an owner selects
an execution directly. There is no cluster-wide API server, scheduler, registry, or global state.

The name follows the same contraction pattern as Kubernetes → k8s: Reticulum Network Stack → r1s.

## Status

r1s is currently a documentation and repository-infrastructure baseline. The architecture,
protocol direction, milestones, and implementation tasks are defined, but production code and the
wire schema have not been started yet.

## Design principles

- **RNS-native:** discovery uses announces and control messages use authenticated RNS links.
- **No global control plane:** each owner controls its tasks; each allocator controls local capacity.
- **Asynchronous protocol:** Protobuf messages are carried over RNS without imposing HTTP/2 or RPC
  semantics on the network.
- **OCI workloads:** the core describes generic images, commands, environment, and execution policy.
- **Partition tolerant:** an owner disconnect is not a lifecycle event. Assigned work continues until
  completion, explicit cancellation, deadline, or maximum runtime.
- **Replaceable adapters:** network and runtime implementations sit behind small Go interfaces.

## Control flow

```mermaid
sequenceDiagram
    participant Owner
    participant RNS as RNS fabric
    participant Allocator
    participant Runtime as OCI runtime

    Owner->>RNS: ExecutionRequest
    RNS->>Allocator: ExecutionRequest
    Allocator->>Allocator: Reserve capacity
    Allocator->>RNS: ExecutionOffer
    RNS->>Owner: ExecutionOffer
    Owner->>RNS: ExecutionAssign
    RNS->>Allocator: ExecutionAssign
    Allocator->>Runtime: Start workload
    Runtime-->>Allocator: Started
    Allocator->>RNS: ExecutionState
    RNS->>Owner: ExecutionState
```

An offer reserves a bounded local slot. Workload start happens only after the owner selects that
offer, avoiding speculative image pulls on every allocator that sees a request.

## Repository checks

The current check verifies internal documentation links:

```sh
make check
```

Go 1.26.5 is recorded in [go.mod](./go.mod) as the initial language baseline. Build, generation,
test, and release tooling will be added with the feature that first needs each tool.

## Documentation

- [AGENTS.md](./AGENTS.md) — repository rules for automated contributors.
- [ARCHITECTURE.md](./ARCHITECTURE.md) — component boundaries, authority, and lifecycle.
- [ROADMAP.md](./ROADMAP.md) — entry point to the plan, features, and implementation tasks.
- [CONTRIBUTING.md](./CONTRIBUTING.md) — development and verification workflow.
- [`roadmap/`](./roadmap/README.md) — vision, features, tasks, and open decisions.

## License

r1s is available under the [MIT License](./LICENSE).

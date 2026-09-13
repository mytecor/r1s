# r1s architecture

## Purpose

r1s runs OCI workloads across intermittently connected nodes without a central scheduler or a
cluster-wide source of truth. RNS supplies identity, addressing, discovery, routing, and encrypted
links. r1s supplies workload demand, local allocation, assignment, and execution lifecycle.

## Vocabulary

| Term | Meaning | Authority |
| --- | --- | --- |
| Owner | Participant that creates a request and chooses an offer | Its own requests and executions |
| Allocator | Node-local service that advertises and reserves capacity | Local capacity and runtime |
| Workload | Infrastructure-neutral OCI image, command, environment, and policy | Immutable request data |
| Offer | Bounded reservation proposed by one allocator | Issuing allocator |
| Execution | One selected, locally running workload instance | Owner for commands; allocator for mechanics |

The core deliberately does not define agents, teams, prompts, CI jobs, or application-level event
hierarchies. Those are workloads or protocols layered on top.

## Components

```mermaid
flowchart LR
    Protocol[Versioned Protobuf protocol]
    Owner[Owner logic]
    Fabric[RNS fabric<br/>announce + Link/Channel]
    Allocator[Allocator core]
    Runtime[OCI runtime]
    Containerd[containerd]

    Protocol -. defines messages .-> Owner
    Protocol -. defines messages .-> Allocator
    Owner <--> Fabric
    Fabric <--> Allocator
    Allocator --> Runtime
    Runtime --> Containerd
```

The source boundaries are:

```text
api/proto/r1s/v1/       versioned wire schema
internal/protocol/      message validation and compatibility
internal/allocator/     offers, capacity, assignment, authorization
internal/transport/     transport boundary and RNS adapter
internal/runtime/       runtime boundary and containerd adapter
```

The protocol, allocator, transport contract and in-memory adapter, runtime contract, and initial RNS
adapter are present. Python-reference RNS interoperability and the containerd production adapter are
introduced or completed by later milestone work.

## Commands

All executable entry points use the standard Go `cmd/<binary>/` layout. Command packages perform
configuration, dependency wiring, process lifecycle, and presentation only; protocol, allocator,
transport, runtime, and owner behavior remains in reusable packages.

| Binary | Source | Purpose | Introduced by |
| --- | --- | --- | --- |
| `r1sd` | `cmd/r1sd/` | Long-running allocator service connected to RNS; uses an unavailable-runtime boundary until F3 | F2, completed by F3 |
| `r1s` | `cmd/r1s/` | Owner CLI for request, list, inspect, cancel, and result operations | F4 |

Build-time tools such as `protoc-gen-go` are not r1s commands and are not shipped as system
binaries.

## Distributed authority

r1s has no globally consistent state:

- an owner knows its requests, received offers, and selected executions;
- an allocator knows only its capacity, offers, and local executions;
- an execution knows its immutable workload and owner;
- RNS routes between identities and destinations but does not become a durable job queue.

Conflicts are resolved by narrow authority rather than consensus. Only the request owner may assign
or cancel its execution. Only the allocator may claim its local capacity or report local runtime
state.

The RNS adapter must populate `Envelope.sender` from the authenticated link identity. A remote peer
must not be allowed to assert an arbitrary sender by serializing different bytes in the envelope.

## Protocol

The planned source of truth is a versioned `r1s.v1` Protobuf schema. Every message will be wrapped
in an `Envelope` with a unique ID, authenticated sender, correlation ID, timestamp, and a `oneof`
payload. The package version will be part of the Protobuf namespace and import path.

The initial exchange is:

1. An owner publishes `ExecutionRequest` with an OCI workload and finite policy.
2. An allocator with free capacity creates a time-bounded `ExecutionOffer`.
3. The owner sends `ExecutionAssign` to exactly one allocator.
4. The allocator starts the workload and publishes `ExecutionState` changes.
5. The owner may send `ExecutionCancel`; the allocator verifies the authenticated sender.

Offers reserve capacity but do not start the workload. This prevents every allocator from pulling
and starting the same image before the owner makes a selection.

Protobuf evolution is additive: existing field numbers are never reused, removed fields are
reserved, and unknown fields must remain safe to ignore.

## Lifecycle under disconnection

Connection state never determines execution lifetime. After assignment, an execution continues
autonomously through a network partition. It ends only when one of these explicit conditions occurs:

- the workload completes or fails;
- the authenticated owner cancels it;
- its deadline or maximum runtime is reached;
- a future local policy explicitly rejects or evicts it.

Completed results may be retained for `result_retention` so an owner can retrieve them after
reconnecting. Result transfer and persistence are not implemented in the first slice.

## Transport boundary

The RNS implementation will use announces only for small discovery descriptors. Protobuf control
messages will travel over authenticated Links, preferably with Channel semantics for ordered,
reliable delivery. Large artifacts belong in RNS Resources or an external artifact plane, not in
announces or small control envelopes.

An in-memory transport will implement the same interface for deterministic tests; it will not be a
simulation of routing, cryptography, or link behavior.

## Runtime boundary

The runtime interface will accept a stable execution ID and require idempotent start and stop.
The first production adapter targets containerd and OCI. VM or microVM backends may be added without
changing the control protocol, but they are not part of the initial milestone.

## Persistence and recovery

Durable allocator state, replay protection across restarts, result storage, and reconciliation with
containerd are open work tracked in [BACKLOG.md](./roadmap/BACKLOG.md).

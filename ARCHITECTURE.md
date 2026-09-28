# r1s architecture

## Purpose

r1s runs OCI workloads across intermittently connected nodes without a central scheduler or a
cluster-wide source of truth. RNS supplies identity, addressing, discovery, routing, and encrypted
links. r1s supplies workload demand, local allocation, assignment, and execution lifecycle.

## Vocabulary

| Term | Meaning | Authority |
| --- | --- | --- |
| Client | `r1s` process that sends commands to allocators and chooses an offer | Its own requests and executions |
| Allocator | Node-local service that advertises and reserves capacity | Local capacity and runtime |
| Workload | Infrastructure-neutral OCI image, command, environment, and policy | Immutable request data |
| Offer | Bounded reservation proposed by one allocator | Issuing allocator |
| Execution | One selected, locally running workload instance | Client for commands; allocator for mechanics |
| Cluster key | Shared 256-bit membership secret distributed as a join token | Every holder is a cluster member |
| Cluster ID | Public domain-separated hash of the cluster key | Discovery label only; grants no access |

The core deliberately does not define agents, teams, prompts, CI jobs, or application-level event
hierarchies. Those are workloads or protocols layered on top.

## Components

```mermaid
flowchart LR
    Protocol[Versioned Protobuf protocol]
    Client[Public Run Controller API]
    Broker[Local cluster authority broker]
    Realm[meshbus realm membership]
    Fabric[RNS control plane<br/>announce + Link/Channel]
    Allocator[Allocator core]
    Runtime[OCI runtime]
    Containerd[containerd]

    Protocol -. defines messages .-> Client
    Protocol -. defines messages .-> Allocator
    Client <--> Broker
    Broker <--> Fabric
    Client <--> Fabric
    Fabric --> Realm
    Fabric <--> Allocator
    Allocator --> Runtime
    Runtime --> Containerd
```

The source boundaries are:

```text
api/proto/r1s/v1/       versioned wire schema
client/                  public ephemeral Run Controller API
github.com/mytecor/meshbus
                         external realm, peer messaging, pub/sub, Node, and Reticulum adapter module
internal/broker/         local credential isolation and per-run transport endpoints
internal/protocol/      message validation and compatibility
internal/client/         in-memory request state, offer selection, and observed execution state
internal/cluster/        r1s cluster naming, join token, credential store, and realm selection
internal/allocator/     offers, capacity, assignment, authorization
internal/transport/     transport boundary and r1s-specific meshbus adaptation
internal/runtime/       runtime boundary and containerd adapter
```

The public client composes the protocol-neutral in-memory client core with either the RNS adapter
directly or a broker-backed endpoint. The
protocol, allocator, transport contract and in-memory adapter, runtime contract, RNS adapter,
and containerd adapter are present. Python-reference RNS discovery interoperability and reliable
Channel envelope delivery (including recovery from injected packet loss) are proven via a gated live
harness. Durable allocator state, restart reconciliation, and the complete partition
recovery acceptance harness are implemented. The live recovery harness remains gated for a Linux
host with containerd and has passed on the project test host.

## Messaging and realm boundary

The reusable messaging layer is named `meshbus`. Its `realm` package derives a public realm ID and
creates mutual membership proofs from a shared key. Its direct-message contract carries only an
opaque payload paired with an immutable transport-authenticated `PeerID`. Both remain independent
of Reticulum-Go, the r1s Protobuf schema, allocators, workloads, and leases.
The r1s `cluster` remains the product-facing name for a meshbus realm plus r1s policy. It uses the
standard meshbus realm ID and authentication domains without an r1s compatibility profile. The
cutover intentionally changes public IDs and link proofs; old credential filenames and old nodes
are not wire-compatible. Join tokens still carry the same shared key and can be joined again into
the new realm credential store.

The intended boundary is:

```text
Reticulum-Go adapter
        ↓
meshbus: realm + discovery + peer sessions + direct messaging + pub/sub
        ↓
r1s: allocator discovery + execution protocol + placement + leases
```

Broadcast is not a separate application primitive. `meshbus.Bus.Publish` creates a bounded event
and fans it out once to a snapshot of authenticated peer destinations. The bus uses exact local
topic subscriptions, 128-bit event IDs, receive-bounded TTL, bounded duplicate suppression,
bounded per-subscription queues, a subscription cap, a peer cap, and fixed fan-out concurrency. It
has no forwarding, persistence, replay, consumer groups, offsets, or exactly-once claim. Announces
remain presence/discovery only. r1s request/offer/assign messages keep their protocol semantics and
use generic direct messaging without becoming pub/sub events. The staged extraction is tracked in
[F24](./roadmap/f24-meshbus-extraction/README.md).

The RNS session path performs Link identity and realm authentication, then emits a meshbus
`ReceivedMessage`. A separate r1s adapter unmarshals and validates the Protobuf envelope and always
replaces its serialized sender with `ReceivedMessage.Sender`; malformed r1s payloads never reach the
allocator or client handler. Outbound r1s envelopes are encoded above the same opaque
`SendMessage` boundary. This keeps peer authority below protocol semantics without changing the
existing Channel message type or bytes.

The generic event wire form is versioned independently with the `MBE` v1 marker. It carries event
ID, topic, publication time, TTL, content type, and opaque payload, but never a sender. On receive,
the sender is taken exclusively from the enclosing authenticated direct message. r1s does not
instantiate the event bus, so the existing r1s Protobuf wire protocol and behavior are unchanged.
The staged extraction fixed the transport-independent peer
discovery contract (`PeerDirectory`) — a bounded, copy-safe directory of authenticated
`PeerID`s with advisory metadata, named a directory rather than a cluster
because the realm is a security boundary while the directory is only observable network state.
F24-05 extracts the reusable public `meshbus/rns` adapter
(it owns the generic RNS machinery — identity, destinations, announces, Links, realm
authentication, Channels, direct delivery, session reuse, peer routes, pre-auth buffering,
PeerDirectory integration, and the single bounded `meshbus.v1` presence format, exposing
peer-addressed `SendMessage`, `ReceivedMessage`, and peer snapshots without importing r1s),
F24-06 moved r1s onto that adapter (only r1s-specific adaptation stays in `internal/transport/rns`),
F24-07 added the cohesive application-facing `Node` API, and F24-08 through F24-12 made routes
adapter-private, established `github.com/mytecor/meshbus` as an independent external module, and
added wire compatibility and external-consumer tests. `Node` owns bounded candidate and
authenticated peer directories, transport lifecycle, stale-peer expiry, direct messages, and
`Bus`. An RNS announce creates only a candidate; successful realm proof promotes it into `Peers`
and pub/sub fan-out. Publishing delivers locally by default and returns attempted/delivered/failed
counts for remote best-effort sends. `meshbus/rns.NewNode` supplies the Reticulum-backed
constructor. Meshbus remains a small brokerless primitive.

## Commands

All executable entry points use the standard Go `cmd/<binary>/` layout. Command packages perform
configuration, dependency wiring, process lifecycle, and presentation only; protocol, allocator,
transport, runtime, and client behavior remains in reusable packages.

| Binary | Source | Purpose | Introduced by |
| --- | --- | --- | --- |
| `r1sd` | `cmd/r1sd/` | Allocator service and cluster bootstrap CLI | F2, F3, F12 |
| `r1s` | `cmd/r1s/` | Client `run` and cluster bootstrap CLI | F4, F12, F22 |

Since F22-07 removed the legacy client control plane, the `r1s` binary has a single run-oriented
frontend plus cluster membership and authority-context commands. The frontend uses the public
[`client`](./client) Run Controller API; it does not contain a second orchestration implementation.
`r1s cluster use` starts a foreground per-user authority broker that retains one selected cluster
key; `-d` explicitly detaches it. Each connected controller gets a fresh broker-owned RNS endpoint
and keeps its own in-memory client engine; the broker owns no request, lease, reschedule, log,
tunnel, or desired state. The controller owns discovery, deterministic offer selection, loser
release, assignment,
authenticated inspection, lease renewal, conclusive-loss rescheduling, and cancellation. CLI log
tailing and local port binding adapt the controller's explicit `Logs` and `OpenTunnel` operations.
Terminal-record retention on the allocator is an operator policy, not workload input.

Build-time tools such as `protoc-gen-go` are not r1s commands and are not shipped as system
binaries.

## Local client API

There is no local client API service or watch journal; the client is an ephemeral in-memory process
that owns one run for its lifetime and keeps nothing across restart. The authority broker socket
(`r1s cluster use`) is not an application API: it forwards authenticated transport events for one
fresh endpoint per connection and exposes no run operations or run state.

Applications that need r1s do not shell out to the CLI. They import [`client`](./client), become an
RNS participant themselves, and communicate with allocators either through directly opened
credentials or the current authority broker. A service that accepts run commands or owns run state
for other applications would recreate an authority and lifecycle boundary and is deliberately not
provided.

## Execution and deployment layers

The unit managed by r1s is one immutable execution of a logical run, created by `r1s run`. The
run engine owns the lease, observes terminal state, and re-requests the recorded workload as the
next attempt on authenticated evidence of conclusive loss. A single execution is therefore not a
deployment declaration.

The closest thing to desired state is the run-lifetime lease held by the in-memory run engine:
a lost lease converts into a re-request of the
recorded workload, so one run heals across restarts of the previous execution without becoming a
deployment declaration. A manifest-driven deployment layer is deferred and would have stayed a
client-side layer over the same
execution operations, without deployment messages in the RNS protocol, allocator-owned desired
state, a global scheduler, or a cluster-wide source of truth.

This boundary also preserves the lifetime rule below: losing a controller connection never stops
an assigned execution.

## Distributed authority

r1s has no globally consistent state:

- a client knows its requests, received offers, and selected executions;
- an allocator knows only its capacity, offers, and local executions;
- an execution knows its immutable workload and client;
- RNS routes between identities and destinations but does not become a durable job queue.

Conflicts are resolved by narrow authority rather than consensus. Only the authenticated client
that created a request may assign or cancel its execution. Only the allocator may claim its local
capacity or report local runtime state.

Placement extends the client's side of this boundary without creating a scheduler. Allocators advertise bounded capabilities (OS, architecture,
runtime, devices, resource profiles, operator labels) in offers and a compact RNS announce summary;
clients express exact-match constraints and only compatible allocators receive the request. The
advertisement is never the authority: incompatible requests are rejected by the allocator before any
capacity is reserved, and assignment re-validates placement against current local state so a stale or
false advertisement yields an explicit `INCOMPATIBLE` rejection, never an invalid start.

The RNS adapter must populate `Envelope.sender` from the authenticated link identity. A remote peer
must not be allowed to assert an arbitrary sender by serializing different bytes in the envelope.

Cluster membership is a separate transport-boundary authorization step implemented through the
transport-independent [`meshbus/realm`](https://github.com/mytecor/meshbus/tree/v0.1.0/realm)
primitive. A participant loads a
random 256-bit `ClusterKey` from `~/.config/r1s/realms/<cluster-id>` and derives the public
identifier as `SHA-256("meshbus-realm-id-v1" || ClusterKey)`. `cluster init` and `cluster join` write
credentials atomically with owner-only permissions; `cluster list` exposes only their public IDs.
Allocator runtime selection and `r1s cluster use` require a full ID or unique hexadecimal prefix
and never accept a join token.
The legacy single credential file is not an implicit default or migration source.

Allocators publish bounded `meshbus.v1` presence containing the public realm ID and compact r1s
capacity/placement/tunnel metadata. The meshbus adapter rejects foreign realm presence before the
r1s discovery projection runs. The key and join token are never announced or placed in protobuf
envelopes. One allocator process and one local authority broker select exactly one cluster; realm
ID and key remain outside workload data and `ExecutionRequest`. Each broker
connection creates one fresh ephemeral RNS identity. Its calling run process owns the corresponding
controller lifecycle, while the key remains confined to the broker.

After an RNS Link authenticates the peer identities, both sides exchange fresh nonces and prove
knowledge of the cluster key with
`HMAC-SHA256(ClusterKey, "meshbus-realm-auth-v1" || nonce || challenger_identity || responder_identity)`.
No control envelope is delivered until the peer's proof succeeds. This makes the verified RNS
sender authoritative for identity and the cluster proof authoritative for baseline membership.
Allocator-local admission and quotas may further restrict individual identities; they never replace
transport-authenticated sender authority.

## Protocol

The planned source of truth is a versioned `r1s.v1` Protobuf schema. Every message will be wrapped
in an `Envelope` with a unique ID, authenticated sender, correlation ID, timestamp, and a `oneof`
payload. The package version will be part of the Protobuf namespace and import path.

The initial exchange is:

1. A client publishes `ExecutionRequest` with an OCI workload, resource class, and constraints.
2. An allocator with free capacity creates a time-bounded `ExecutionOffer`.
3. The client sends `ExecutionAssign` to exactly one allocator.
4. The allocator starts the workload under a durable client-held lease and publishes
   `ExecutionState` changes.
5. The client renews the lease with `ExecutionLeaseRenew` and may send `ExecutionCancel`; the
   allocator verifies the authenticated sender either way.

```mermaid
sequenceDiagram
    participant Client
    participant RNS as RNS fabric
    participant Allocator
    participant Runtime as OCI runtime

    Client->>RNS: ExecutionRequest
    RNS->>Allocator: ExecutionRequest
    Allocator->>Allocator: Reserve capacity
    Allocator->>RNS: ExecutionOffer
    RNS->>Client: ExecutionOffer
    Client->>RNS: ExecutionAssign
    RNS->>Allocator: ExecutionAssign
    Allocator->>Runtime: Start workload
    Runtime-->>Allocator: Started
    Allocator->>RNS: ExecutionState
    RNS->>Client: ExecutionState
```

After either side restarts, the client may send `ExecutionInspect` to the selected allocator. The
allocator verifies the authenticated client and returns its latest durable `ExecutionState`. This
also supplies the first retained-result contract: terminal phase, detail, and exit code.

Offers reserve capacity but do not start the workload. This prevents every allocator from pulling
and starting the same image before the client makes a selection.

The client atomically stores its chosen assignment and stable `ExecutionOfferRelease` intents for
known losing offers. Each losing allocator verifies the authenticated owner and acknowledges a
released, expired, or assigned outcome. Only an outstanding reservation returns capacity; an
assigned execution is never changed by release. Late offers observed after selection get their own
durable release intent. Unknown release payloads on older allocators can be ignored safely because
the original offer TTL still bounds the reservation. New durable released-offer states require a
binary that understands them; an older allocator refuses that state rather than reopening capacity.

Protobuf evolution is additive: existing field numbers are never reused, removed fields are
reserved, and unknown fields must remain safe to ignore.

## Lifecycle under disconnection

Connection state never determines execution lifetime. After assignment, an execution continues
autonomously through a network partition. It ends only when one of these explicit conditions
occurs:

- the workload completes or fails;
- the authenticated client cancels it;
- its client-held lease expires without renewal and the allocator's lease sweep evicts it;
- a future local policy explicitly rejects or evicts it.

Every running execution carries a durably persisted lease held by the authenticated client. The
allocator grants an initial lease at assignment (10 minutes by default) and extends it on each
authenticated `ExecutionLeaseRenew` from the execution owner, bounded locally. Renewal is
idempotent and replay-safe and returns only the new expiry. A lease outlives any partition shorter
than its duration: connection state still never determines lifetime.

One logical run has a random 128-bit `run_id` encoded as 32 lowercase hexadecimal characters.
Each fresh announcement increments its positive `attempt` and receives a new `request_id` and
`execution_id`; an execution never migrates between allocators. Allocators persist this correlation
with the execution and expose it to the workload as container labels `io.r1s.run-id` and
`io.r1s.attempt` and environment variables `R1S_RUN_ID` and `R1S_ATTEMPT`. These values support
application-level fencing and idempotency but grant no authority: the transport-authenticated
sender remains the owner.

Rescheduling is at-least-once. An authenticated `EXPIRED`/`NOT_FOUND` or observed lease-expiry
terminal state may advance the attempt; a timeout or one missing renewal acknowledgement does not.
Because a renewal can succeed while its acknowledgement is lost, a conservative future
lease-loss threshold may allow two attempts to overlap. r1s introduces no coordinator and makes no
exactly-once claim.

`r1s run` holds the lease for the lifetime of the command: the run engine is the renewal loop and
re-requests the recorded workload whenever the lease is lost, advancing to the next attempt. The
intent lives only in the in-memory client; process restart cannot reclaim the old execution, whose
allocator-enforced lease expires independently. A renewal never attaches logs, results,
or other payload; it returns only the new expiry. Evicted executions commit terminal state with a
stable lease-expiry reason that is distinguishable from a client cancellation.

Terminal metadata is durably retained by the allocator so a client can retrieve it after
reconnecting. The allocator-configured retention deadline, replay-safe tombstones, and bounded
local log storage are enforced by the allocator.

Container stdout/stderr belongs in local allocator storage. Logs are transferred only after an
explicit request from the authenticated execution owner. Completion, failure, cancellation,
reconnection, inspection, and retrieval must never automatically send logs or attach log tails to
execution state or error details. A failed container changes lifecycle metadata only; the client
may separately request its logs when needed. A log request bounds the stream, offset, and byte
count; disconnection ends that transfer without affecting execution. The same explicit-request
rule stays if a future mechanism ever carries a requested log range outside the client command.

## Transport boundary

The RNS implementation uses announces only for small discovery descriptors. Protobuf control
messages travel over authenticated Links with Channel semantics for ordered, reliable delivery.
RNS is not the bulk-transfer path: it carries control envelopes and discovery descriptors, never
application bytes.

Production `r1s` and `r1sd` endpoints attach only as clients to an already-running Reticulum shared
instance at the Reticulum-Go platform default (the Linux abstract Unix socket, or the supported TCP
default on other platforms). Failure to connect is fatal and never elects r1s as the shared-instance
server. Explicit standalone transports remain confined to deterministic and live test harnesses.

Execution-tunnel application bytes use the dedicated embedded Yggdrasil adapter. RNS authorizes
and transports the bounded owner-authenticated open exchange; the resulting peer-key-pinned Ygg
mesh pair carries the multiplexed TCP streams. Application bytes never move onto the RNS control
plane.

Application data transfer outside the execution tunnel remains out of scope until separately
designed.

An in-memory transport will implement the same interface for deterministic tests; it will not be a
simulation of routing, cryptography, or link behavior.

Allocator RNS endpoints announce capacity. Client endpoints are passive: they discover those
announces and establish authenticated Links without advertising fake allocator capacity.
Allocator presence carries the public realm ID plus bounded r1s advisory metadata. Foreign-realm
presence is ignored, and direct links still require mutual realm-key proof before their Channels
can carry protobuf control envelopes.

An endpoint identity may come from an existing or newly created 64-byte RNS identity file, or from
private identity bytes encoded as hex, Base32, or Base64 and imported through Reticulum-Go's
`rnsutil.ImportPrivateIdentity`. Existing files take precedence. Inline identity material is used
without being written to disk and is never included in diagnostic labels or derived filenames.

OCI image distribution is outside the r1s protocol. A workload continues to name a
digest-pinned image, and containerd may fetch it from any standard OCI registry reachable through
the Internet, a LAN, a mirror, or local cache. r1s does not implement an image transport or
registry protocol.

Container stdout and stderr remain allocator-local. Only an explicit request from the authenticated
execution owner may transfer a bounded range of logs; completion, failure, inspection, result
retrieval, and reconnection never do so implicitly.

## Runtime boundary

The runtime interface accepts a stable execution ID plus its run ID and attempt, and requires
idempotent start and stop. The
containerd adapter isolates metadata in an r1s namespace, requires digest-pinned images, derives
container IDs from execution IDs, and verifies stored identity/specification labels before reuse.
VM or microVM backends may be added without changing the control protocol, but they are not part of
the initial milestone.

Execution-port access uses the optional runtime `PortDialer` interface. The allocator validates
an active registry-issued session and its allowed port before each dial. The containerd adapter
verifies execution labels and the live task, pins its Linux network namespace, and connects to
container loopback on a dedicated OS thread. Host-network namespaces and non-local containerd
endpoints are rejected. The host namespace is restored before the thread is reused; a restoration
failure discards the thread. Fresh container loopback is brought up inside that namespace.

A terminal execution closes the session's revocation signal. The edge closes its mesh pair,
all streams, pending dials, and target sockets. Ending a mesh connection releases only its own
registry session, allowing a later owner open without affecting execution lifetime. Pair-level close
frames preserve the reason at the remote handshake and active streams. Frame sizing includes
the seven-byte header both in the packet MTU budget and the receive buffer.

## Persistence and recovery

The allocator stores one versioned state snapshot in a transactional bbolt database after every
accepted transition. The snapshot is bound to the allocator's authenticated identity and contains
offers, assignments, execution state, replay records, the durable client-held lease expiry for
every running execution, and the original start time.

At startup, `r1sd` restores capacity accounting and reconciles non-terminal records with containerd
before accepting transport messages. Matching running or stopped tasks regain completion
monitoring without being restarted, and an execution whose persisted lease is still valid keeps
running; one whose lease already expired is evicted instead of revived. A missing task or
mismatched execution/specification label becomes a terminal failure; a persisted cancellation is
completed idempotently. Terminal state is committed before containerd metadata is removed so a
store failure leaves a stopped task available for the next recovery attempt.

Result storage beyond terminal metadata remains open.

The client keeps no durable state: it does not persist requests, offers, assignments, or observed
state anywhere on disk. The run engine holds all of it in memory for the run's lifetime; a client
process restart starts fresh authority and cannot reclaim an old execution, whose allocator-enforced
lease expires independently. Inspection is the explicit authenticated read that recovers terminal
metadata from the allocator after a reconnect.

The gated end-to-end recovery harness runs `r1sd` as a separate process over a loopback RNS UDP
pair. It disconnects the client, restarts the allocator while the labelled containerd task remains
running, waits for offline completion, reconnects a fresh ephemeral client, replays its assignment
against the allocator's durable state, and retrieves terminal metadata with a fresh inspection. A
second real workload proves repeated request and cancellation envelopes remain idempotent.

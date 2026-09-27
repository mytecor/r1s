# F24. Brokerless meshbus extraction

Corresponds to [F24 in the roadmap](../../ROADMAP.md#f24-brokerless-meshbus-extraction).

**Status:** 🚧 In progress

## Outcome

`meshbus` is a reusable brokerless messaging layer below r1s. It owns secure realm membership,
peer discovery and sessions, authenticated direct messages, and best-effort pub/sub. r1s keeps all
allocator, execution, placement, lease, log, tunnel, and application-authorization semantics.

At the r1s boundary, `cluster` means a named and locally managed meshbus realm plus r1s policy. The
user-facing `r1s cluster` commands and credential format do not change merely because the primitive
moves down a layer.

## Scope

- Extract public realm ID and mutual proof primitives without importing Reticulum-Go or r1s types.
- Preserve existing r1s cluster IDs and link proofs through explicit compatibility domains.
- Separate raw authenticated peer delivery from the r1s Protobuf envelope and validator.
- Keep RNS announces limited to bounded presence/discovery descriptors.
- Add direct messaging whose received sender is supplied only by the authenticated session.
- Add local topic subscriptions and best-effort publish fan-out to known realm peers.
- Bound event size, TTL, deduplication memory, queues, and handler concurrency.

## Non-goals

- Allocator roles, capacity, placement, workload permissions, or scheduler behavior.
- Durable events, replay, consumer groups, offsets, or exactly-once delivery.
- Carrying container stdout/stderr, tunnel bytes, OCI images, or other bulk data.
- Replacing the r1s request/offer/assign lifecycle with pub/sub events.
- A separate `meshbus` CLI during the initial extraction.

## Authority and lifetime

- Realm proof establishes only that the transport-authenticated peer holds the realm key.
- A sender copied from a serialized message is never authoritative.
- r1s remains responsible for deciding whether an authenticated realm peer may mutate an execution.
- Transport disconnect never terminates an execution; the durably persisted, explicitly renewed
  lease remains the lifetime boundary.
- Logs remain allocator-local and move only after the existing explicit authenticated log request.

## Delivery baseline

The first pub/sub version is intentionally small: no persistence or replay, reliable Channel
delivery while a link is usable, reconnect on later sends, publisher fan-out to known peers, event
IDs, bounded deduplication, and TTL. Handlers must tolerate duplicate delivery. Subscription
advertisement and multi-hop gossip are deferred until scale requires them.

## Tasks

- [F24-01 — Extract the secure realm primitive](./f24-01-realm-primitive.md)
- [F24-02 — Extract authenticated direct messages](./f24-02-direct-messages.md)
- [F24-03 — Add bounded pub/sub fan-out](./f24-03-pubsub.md)
- F24-04 — Extract generic realm discovery and the public RNS adapter (planned)

## Completion criteria

- The generic meshbus packages import no r1s protocol, allocator, client, runtime, or command code.
- r1s control messages continue to pass the same authenticated-sender and cluster-membership tests.
- Foreign realms are rejected before application delivery.
- Pub/sub bounds and duplicate-delivery behavior have deterministic contract tests.
- `make check` passes.

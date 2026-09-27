# F24-05 — Extract reusable Reticulum meshbus adapter

**Status:** ⏳ Planned

## Outcome

Create a reusable Reticulum adapter for meshbus that owns RNS identity, destinations, announces,
Links, realm authentication and Channel-based direct delivery. The adapter must not know about r1s
envelopes, allocators, executions or placement. It becomes the reusable transport implementation
underneath r1s.

## Context

Generic realm authentication and direct-message contracts already exist in:

- `meshbus/realm`
- meshbus direct-message API

The Reticulum-specific implementation still lives under `internal/transport/rns`. This task
physically extracts it into a public adapter package.

## Scope

Create a public adapter package, preferably `meshbus/rns`. Move or refactor the generic parts of
the existing RNS endpoint into it:

- RNS identity handling;
- Destination creation;
- announce registration;
- Link establishment;
- mutual realm proof;
- Channel creation;
- authenticated direct-message delivery;
- connection/session reuse;
- peer route lookup;
- bounded pre-authentication buffering;
- generic peer discovery integration.

Expose meshbus primitives rather than r1s envelopes. The adapter must provide enough functionality
for:

- `SendMessage(PeerID, payload)`;
- authenticated incoming `ReceivedMessage`;
- peer discovery / `PeerDirectory` updates;
- listing currently known peers for Bus fan-out.

### Presence format

Define a small bounded meshbus presence descriptor for new meshbus applications. It should contain
only generic information needed for realm discovery. Do not put pub/sub events into announces; do
not advertise subscriptions; RNS announces remain presence/discovery only. Keep the existing r1s
announce wire format working through a compatibility layer; do not silently break existing r1s
discovery interoperability.

## Constraints

- `meshbus/rns` may import Reticulum-Go and meshbus.
- It must not import r1s protocol, client, allocator, runtime or command packages.
- Received sender identity always comes from the authenticated Link.
- Foreign realms are rejected before message delivery.
- Network discovery does not imply application authorization.
- Announce and connection queues remain bounded.

## Acceptance

- Two meshbus RNS nodes in one realm discover each other.
- Nodes in different realms do not become usable peers.
- Authenticated direct bytes can be exchanged without importing r1s.
- A discovered `PeerID` can subsequently be used for direct send.
- Reconnect after Link loss works on a later send.
- Peer snapshots can directly feed meshbus Bus fan-out.
- RNS Python interoperability tests that are transport-generic remain passing or are moved
  appropriately.
- `make check` passes.

## Notes

The dependency direction becomes:

```text
meshbus
├── realm
├── PeerDirectory
├── direct messages
└── Bus
     ↑
meshbus/rns
     ↑
r1s adapter
```

rather than the current `meshbus <- internal/transport/rns <-> r1s`.

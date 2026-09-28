# F24-05 — Extract reusable Reticulum meshbus adapter

**Status:** ✅ Complete

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
not advertise subscriptions; RNS announces remain presence/discovery only. The initial extraction
allowed custom compatibility codecs; the final cutover moved r1s itself to the generic format.

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

## Implementation notes

The public [`meshbus/rns`](https://github.com/mytecor/meshbus/tree/v0.1.0/rns) package owns the reusable RNS machinery that was
previously entangled with r1s: identity loading (file or inline encodings, or an ephemeral
in-memory identity), destination creation, announce registration and the periodic refresh loop,
Link establishment, mutual realm proof, Channel creation, authenticated direct-message delivery,
bounded pre-authentication buffering, connection/session reuse, peer route lookup, and peer
discovery integration. It imports only Reticulum-Go, the meshbus primitives, and the standard
library — no r1s protocol, allocator, client, runtime, or command packages.

The adapter exposes meshbus primitives rather than r1s envelopes:

- `Endpoint.SendMessage(ctx, peerID, payload)` resolves the adapter-private RNS destination and
  sends opaque authenticated bytes over a reused realm session. The low-level
  `SendToDestination` surface exists only for protocol adapters that already persist RNS destinations.
- Inbound bytes reach a `meshbus.Handler` as `meshbus.ReceivedMessage`, whose sender always comes
  from the authenticated Link (never from serialized payload).
- `Endpoint.DiscoveredPeers()` exposes bounded advisory announce candidates without routes.
- `SetPeerObserver` reports discovery separately from successful realm authentication. `Node` owns
  the authoritative peer directory and uses only authenticated peers for `Bus` fan-out.

### Presence wire format

The adapter has one bounded `meshbus.v1` presence descriptor — a JSON object naming the protocol
version and hex-encoded realm, with optional advisory metadata, capped at 256 bytes and 16 keys.
Realm verification runs inside the adapter using its real realm ID, so forged or foreign presence
can never enter the peer directory. `Config.PresenceMetadata` supplies application advisory data,
and `Config.Passive` suppresses local announces for clients. There is no compatibility codec or r1s
wire-format hook.

### Deterministic tests

`meshbus/rns` has standalone UDP-loopback contract tests that run without a shared instance and
without importing r1s: same-realm discovery, foreign-realm rejection on a shared pair, direct
authenticated byte exchange, send-after-link-loss reconnect, peer snapshots for Bus
fan-out, discovery/authentication observer delivery, presence round-trip, identity-source handling, config
validation, and stack construction.

The Python RNS reference interop test stays in `internal/transport/rns` because it exchanges r1s
`Envelope` protobuf across the wire rather than generic meshbus bytes; it is not transport-generic
and therefore is not moved.

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

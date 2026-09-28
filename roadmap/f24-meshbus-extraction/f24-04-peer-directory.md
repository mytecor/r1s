# F24-04 — Extract generic peer discovery

**Status:** ✅ Complete

## Outcome

The public [`meshbus`](https://github.com/mytecor/meshbus/tree/v0.2.0) package now owns the transport-independent realm peer
discovery contract. `meshbus.PeerDirectory` is an observable directory of authenticated
realm peers learned through discovery — named a directory, not a cluster, because the **realm is
a security boundary**, whereas the directory is only the observable state of the network.

A discovered [`Peer`](https://github.com/mytecor/meshbus/blob/v0.2.0/peer.go) carries:

- a public `PeerID` established by the authenticated transport identity (realm membership is
  proven separately);
- optional bounded `Metadata` (advisory application hints such as os/arch);
- an optional advisory `Hops` path metric such as hop count; and
- a local `LastSeen` timestamp used for deterministic stale expiry.

The generic package imports only the Go standard library. No Reticulum-Go and no r1s protocol,
allocator, runtime, or command types enter `meshbus`.

## Directory behaviour

- `Remember(Peer)` adds a peer keyed by authenticated `PeerID`, or updates an existing peer in
  place — a duplicate discovery never creates a second entry.
- `Get(PeerID)` returns one immutable copy of a peer.
- `Peers()` returns a copy-safe, identity-ordered snapshot for fan-out.
- `IDs()` returns the identity snapshot consumed by `meshbus.Bus` fan-out via
  `PeerSourceFunc`.
- `Remove(PeerID)` forgets a peer; `ExpireStale(age)` deterministically ages out idle peers.
- `Len()` reports the current count.

## Resource bounds

- `MaxPeers` (default 1024) caps a discovery directory; a new identity evicts the oldest candidate
  at capacity while updates to known identities still succeed. `Node` keeps authenticated peers
  separately for their transport-session lifetime without an artificial count cap.
- `MaxMetadataBytes` (default 4096) caps the total application-metadata bytes carried on each peer
  record.
- `Peers()` and `IDs()` return immutable copies, so a caller mutating a snapshot or its
  metadata cannot corrupt the directory.

## Authority and limits

Discovery is advisory only. Peer identity never comes from application metadata, and transport
routes remain private to the adapter. `ReceivedMessage.Sender` comes only from the transport
session. The directory adds no durable membership, no globally authoritative peer list, and no
gossip or subscription advertisement.

## Acceptance

- meshbus peer discovery primitives import only the Go standard library.
- Peer snapshots are immutable and copy-safe.
- Duplicate discoveries update one peer instead of creating duplicates.
- Discovery peer count and metadata size are bounded; capacity uses deterministic oldest-first
  eviction and oversized metadata returns `ErrMetadataLimit`.
- Stale peers expire deterministically from `LastSeen`.
- The Bus consumes the peer-directory identity snapshot without r1s or transport-route types.
- Deterministic tests cover update, expiry, bounds, and copy safety.
- `make check` passes.

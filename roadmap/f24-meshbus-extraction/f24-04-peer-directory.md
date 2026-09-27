# F24-04 — Extract generic peer discovery

**Status:** ⏳ Planned

## Outcome

Move the generic concept of realm peer discovery out of the r1s RNS transport. This task
introduces the transport-independent discovery contract only; Reticulum-Go code is not moved yet.
After this task, meshbus can represent discovered peers independently of Reticulum and
independently of r1s allocator descriptors.

The entity is named `PeerDirectory`/`PeerStore`, not `Cluster`, because the **realm is a security
boundary**, whereas the directory is only the observable state of the network.

## Context

The current discovery implementation lives in:

- `internal/transport/rns/discovery.go`
- `internal/transport/rns/endpoint.go`
- `internal/transport/rns/connections.go`

The current `Service` and `Descriptor` types are r1s-specific and expose allocator capacity,
placement hints and tunnel metadata. meshbus needs only enough information to identify and route
to peers in the same authenticated realm.

## Scope

Introduce transport-independent peer discovery primitives under `meshbus`. A discovered peer
should contain at minimum:

- authenticated/public `PeerID`;
- transport route/address represented opaquely;
- optional bounded application metadata;
- optional advisory path metric such as hop count.

Add a peer registry/directory that can:

- remember discovered peers;
- update an existing peer;
- resolve a `PeerID` to its current transport route;
- return a bounded snapshot of known peers for pub/sub fan-out;
- remove or expire stale entries.

The directory must have explicit resource bounds.

Discovery is advisory only. Presence must not grant application authorization.

Do not put allocator capacity, runtime, tunnel information, execution roles or r1s protocol types
into meshbus.

## Constraints

- No Reticulum-Go imports in the generic meshbus package.
- No r1s imports.
- Peer identity must never come from untrusted application metadata.
- A route learned through discovery is not an authenticated sender identity.
- Do not add durable membership or a globally authoritative peer list.
- Do not add gossip or subscription advertisement.

## Acceptance

- meshbus peer discovery primitives import only the Go standard library.
- Peer snapshots are immutable/copy-safe.
- Duplicate discoveries update one peer instead of creating duplicates.
- Peer count and metadata size are bounded.
- Stale peers can be expired deterministically.
- Bus can consume the peer directory snapshot without r1s types.
- Deterministic tests cover update, expiry, bounds and copy safety.
- `make check` passes.

## Notes

This is the foundation layer. F24-05 is deliberately kept separate so the clean
transport-independent discovery contract is fixed before the current r1s-shaped
`internal/transport/rns` architecture is carried out of the tree.

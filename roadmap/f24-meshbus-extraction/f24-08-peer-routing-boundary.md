# F24-08 — Make peer identity the public routing boundary

**Status:** ✅ Complete

## Outcome

Core meshbus APIs address authenticated `PeerID` values. Transport routes remain private adapter
mechanics and cannot become application authority or leak through pub/sub results.

## Scope

- Change the direct sender and pub/sub peer-source contracts from route strings to `PeerID`.
- Make `Node.Send` the peer-addressed direct-message surface and remove its route-addressed bypass.
- Keep RNS destination resolution inside `meshbus/rns`.
- Report fan-out failures by peer identity.
- Bound peer identity size and reject invalid resource-limit configuration.
- Stop exposing mutable adapter-owned peer directories.

## Acceptance

- A generic application can send and publish using only authenticated peer identities.
- Core meshbus contains no transport-route type or route-addressed send method.
- The RNS adapter resolves a discovered `PeerID` without application help.
- Tests cover unknown peers, identity bounds, fan-out failure attribution, and copy safety.
- `make check` passes.

## Implementation notes

`Sender`, `PeerSource`, `Bus`, `Node`, and delivery results now use `PeerID`. `Peer` no longer
contains a route, and `PeerDirectory.IDs` supplies fan-out snapshots. `meshbus/rns.Endpoint`
privately resolves identities to destinations; only the r1s protocol adapter uses the explicit
low-level `SendToDestination` bridge for its already-persisted RNS destination values.

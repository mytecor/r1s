# F24-06 — Move r1s onto the public meshbus transport

**Status:** ✅ Complete

## Outcome

Make r1s consume meshbus and `meshbus/rns` instead of owning the generic Reticulum transport
machinery. After this task, the r1s RNS layer contains only r1s-specific adaptation, removing the
remaining layer mixing.

## Scope

Refactor the current `internal/transport/rns` package. Move generic transport/session/discovery
behavior to `meshbus/rns` (F24-05). Keep r1s-specific behavior above it:

- r1s allocator discovery metadata;
- Capacity;
- OS / Arch / Runtime placement summary;
- tunnel advertisement;
- Protobuf envelope encoding/decoding;
- r1s protocol validation;
- allocator discovery semantics.

The final cutover intentionally drops the r1s compatibility domains and descriptor wire format.
r1s uses the standard meshbus realm profile and `meshbus.v1` presence with compact application
metadata. Incoming r1s envelopes must continue to have `Envelope.sender` replaced by the
authenticated meshbus sender before validation.

### Desired result

The r1s transport should look conceptually like:

```text
r1s Envelope
    ↓
r1s envelope adapter
    ↓
meshbus direct message
    ↓
meshbus/rns
```

and discovery:

```text
RNS announce
    ↓
meshbus/rns presence
    ↓
r1s descriptor adapter
    ↓
allocator catalog
```

## Acceptance

- Generic RNS code is no longer owned by r1s.
- `meshbus/rns` imports no r1s packages.
- r1s request/offer/assign behavior is unchanged.
- Allocator discovery retains Capacity and placement information.
- Forged `Envelope.sender` remains ineffective.
- Foreign cluster/realm traffic remains rejected.
- Existing reconnect, shared-instance and Python compatibility tests pass.
- `make check` passes.

## Implementation notes

The r1s [`internal/transport/rns`](../../internal/transport/rns) package is now a thin application
adapter over the public [`meshbus/rns`](../../meshbus/rns) endpoint. It retains only the r1s
placement/capacity/tunnel metadata projection, Protobuf envelope
encoding/validation, and the allocator discovery channel used by the client and authority broker.

The duplicated RNS stack, identity loader, announces, path lookup, Links, Channels, realm
challenge-response, pre-authentication buffers, connection registry, reconnect behavior, and
direct-message delivery were removed from the internal package. Identity helpers used by `r1sd`
delegate to `meshbus/rns` rather than maintaining a second implementation.

r1s uses the standard meshbus realm profile and the single meshbus presence format. Allocators put
compact capacity, placement, and tunnel fields into bounded `meshbus.v1` metadata; passive clients
announce nothing. The presence parser rejects foreign realms before discovery, while the envelope handler
always replaces the serialized sender with the authenticated `meshbus.ReceivedMessage` sender
before validation.

Deterministic tests cover r1s descriptor discovery and authenticated envelope exchange through the
public adapter, forged-sender replacement, foreign-realm rejection, generic reconnect behavior,
and direct delivery through a required shared instance. The existing Python-reference compatibility
harness remains at the r1s adapter boundary because it exchanges r1s Protobuf envelopes.

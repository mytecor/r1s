# F24-06 — Move r1s onto the public meshbus transport

**Status:** ✅ Complete

## Outcome

Make r1s consume meshbus and `meshbus/rns` instead of owning the generic Reticulum transport
machinery. After this task, the r1s RNS layer contains only r1s-specific adaptation, removing the
remaining layer mixing.

## Scope

Refactor the current `internal/transport/rns` package. Move generic transport/session/discovery
behavior to `meshbus/rns` (F24-05). Keep r1s-specific behavior above it:

- existing r1s allocator descriptor;
- Capacity;
- OS / Arch / Runtime placement summary;
- tunnel advertisement;
- Protobuf envelope encoding/decoding;
- r1s protocol validation;
- allocator discovery semantics.

Preserve existing cluster compatibility domains:

- `r1s-cluster-id-v1`;
- `r1s-auth-v1`.

Preserve existing credential and cluster behavior. The existing r1s descriptor wire format should
remain compatible unless an explicit migration is required. Incoming r1s envelopes must continue
to have `Envelope.sender` replaced by the authenticated meshbus sender before validation.

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
adapter over the public [`meshbus/rns`](../../meshbus/rns) endpoint. It retains only the established
`r1s.v1` allocator descriptor, placement/capacity/tunnel projection, Protobuf envelope
encoding/validation, and the allocator discovery channel used by the client and authority broker.

The duplicated RNS stack, identity loader, announces, path lookup, Links, Channels, realm
challenge-response, pre-authentication buffers, connection registry, reconnect behavior, and
direct-message delivery were removed from the internal package. Identity helpers used by `r1sd`
delegate to `meshbus/rns` rather than maintaining a second implementation.

r1s configures the public adapter with the existing `r1s-cluster-id-v1` and `r1s-auth-v1`
compatibility domains and supplies a `PresenceCodec` for the existing descriptor bytes. The codec
rejects foreign cluster IDs before discovery, while the envelope handler always replaces the
serialized sender with the authenticated `meshbus.ReceivedMessage` sender before validation.

Deterministic tests cover r1s descriptor discovery and authenticated envelope exchange through the
public adapter, forged-sender replacement, foreign-realm rejection, generic reconnect behavior,
and direct delivery through a required shared instance. The existing Python-reference compatibility
harness remains at the r1s adapter boundary because it exchanges r1s Protobuf envelopes.

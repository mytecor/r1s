# F24-06 — Move r1s onto the public meshbus transport

**Status:** ⏳ Planned

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

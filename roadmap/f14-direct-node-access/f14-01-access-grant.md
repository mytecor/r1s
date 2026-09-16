# F14-01 — Add execution-scoped access grant and tunnel endpoint

**Status:** ⏳ Planned

## Outcome

An authenticated allocator can authorize exactly one bounded direct-access tunnel for a running
execution without making routability, the Yggdrasil address, or the cluster key sufficient for
access, and without coupling the tunnel to an artifact model.

## Scope

- Add a transport-neutral, additive control message that opens a direct-access tunnel to a running
  execution and advertises the allocator-local endpoint URI.
- Bind each grant to execution ID, requesting owner identity, tunnel direction, maximum byte
  ceiling, and expiry through the authenticated control exchange.
- Define replay, renewal, revocation, and clock-skew behavior; keep the cluster-key proof at the
  RNS boundary rather than requiring the tunnel service to accept the cluster key as a bearer
  credential.
- Keep grant secrets out of descriptors, diagnostics, metrics, and durable records except where
  explicitly required by the chosen revocation design.

## Acceptance

- Validation rejects a grant for the wrong execution, direction, owner, byte count, issuer, or an
  expired grant.
- A grant received from an unverified payload or an unverified peer never overrides allocator-local
  authority.
- Reuse follows one documented rule and remains safe across allocator restart.
- The tunnel contract contains no Yggdrasil-specific field or address type.
- `make check` passes and existing Protobuf field numbers stay unchanged.

## Notes

This task defines the authorization and endpoint contract only; the allocator-local tunnel service
and Yggdrasil edge are F14-02. OCI image distribution is outside this task.

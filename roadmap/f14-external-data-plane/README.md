# F14. External data-plane contract

Corresponds to [milestone F14](../../ROADMAP.md#f14-external-data-plane-contract).

**Status:** ⏳ Planned

## Outcome

Executions exchange content-addressed application inputs and outputs over an external IP data plane.
RNS remains responsible for discovery, authenticated identity, control messages, and issuance of
transfer authority; it never carries the bulk artifact bytes.

## Dependencies

- [F8. Protocol feedback and state revisions](../f8-protocol-feedback/README.md)
- [F12. Shared-secret cluster membership](../f12-cluster-membership/README.md)

## Scope

- Describe artifacts by digest and size independently of their current locations.
- Advertise bounded, transport-neutral `DataEndpoint` URIs through authenticated control data.
- Issue short-lived capabilities over RNS after the relevant execution decision.
- Bind capabilities to execution, direction, digest, maximum bytes, and expiry.
- Verify length and digest before an artifact becomes available to a workload or client.
- Define input placement and output descriptors without creating a shared filesystem.
- Preserve explicit-only log retrieval if the same plane later carries requested log ranges.

## Completion criteria

- Protocol validation rejects malformed descriptors, endpoints, scopes, and expiry values.
- Endpoint metadata received from an unverified payload never overrides authenticated allocator
  authority.
- Upload and download conformance tests prove authorization, byte bounds, replay behavior, expiry,
  and digest verification without sending artifact bytes through RNS.
- The contract contains no Yggdrasil-specific field or address type.
- OCI image fetching remains outside the artifact protocol.

## Tasks

- [F14-01 — Define artifact and endpoint descriptors](./f14-01-artifact-contract.md)
- [F14-02 — Define data-access capabilities](./f14-02-capabilities.md)

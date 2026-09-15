# F15. Yggdrasil data plane

Corresponds to [milestone F15](../../ROADMAP.md#f15-yggdrasil-data-plane).

**Status:** ⏳ Planned

## Outcome

Yggdrasil-connected clients and allocators transfer application artifacts through an allocator-side
HTTP service implementing the generic F14 contract. Yggdrasil provides IPv6 reachability, not r1s
identity, discovery, placement, or authorization.

## Dependencies

- [F14. External data-plane contract](../f14-external-data-plane/README.md)

## Scope

- Serve capability-authorized artifact upload and download over HTTP on configured IP endpoints.
- Advertise those endpoints through the generic RNS control-plane descriptor.
- Enforce transfer bounds, expiry, integrity verification, temporary-file isolation, and cleanup.
- Document firewall, TLS, Yggdrasil routing, and failure/retry expectations.
- Document how an operator may expose a standard OCI registry over Yggdrasil without making it part
  of the r1s artifact service.

## Completion criteria

- A live test transfers input and output artifacts between two Yggdrasil-connected peers.
- Invalid, expired, oversized, and digest-mismatched transfers are rejected and cleaned up.
- Loss of the HTTP connection does not cancel an execution or weaken RNS command authority.
- No application artifact bytes traverse RNS, and the core builds without Yggdrasil dependencies.
- Containerd can continue using Internet, LAN, Yggdrasil, mirror, or cached OCI sources unchanged.

## Tasks

- [F15-01 — Implement the HTTP artifact service](./f15-01-http-artifact-service.md)
- [F15-02 — Verify Yggdrasil deployment and isolation](./f15-02-yggdrasil-acceptance.md)

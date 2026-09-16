# F14. Direct node access (r1s-tunneld)

Corresponds to [milestone F14](../../ROADMAP.md#f14-direct-node-access-r1s-tunneld).

**Status:** ⏳ Planned

## Outcome

An execution owner opens an authenticated tunnel from their client to a running execution on an
allocator through Yggdrasil and carries arbitrary traffic inside it (for example SSH or HTTP). The
tunnel is node-local and capability-gated, it is not bound to an artifact model, and it never
travels over RNS.

## Dependencies

- [F2. RNS transport](../f2-rns-transport/README.md)
- [F12. Shared-secret cluster membership](../f12-cluster-membership/README.md)
- [F13. Local client API](../f13-local-client-api/README.md)

## Scope

- Define the control exchange that mints a short-lived, execution-scoped access grant over the
  authenticated control plane.
- Advertise a transport-neutral endpoint through the authenticated control data so the client can
  learn where to connect without embedding an address in the immutable workload.
- Serve an allocator-local tunnel endpoint over Yggdrasil that terminates at the running execution.
- Carry arbitrary traffic inside the tunnel; r1s-tunneld does not parse or restrict the payload.
- Restrict access to the authenticated execution owner and keep it independent of artifact
  transfer: the tunnel is lifecycle and debugging access, not a data plane.
- Keep the core mockable: a generic tunnel interface in front of a Yggdrasil/HTTP implementation.

## Completion criteria

- Protocol validation rejects a grant bound to the wrong execution, direction, owner, byte ceiling,
  or expiry, or received from an unverified payload.
- An execution owner opens a tunnel to a running execution and a successful payload round-trip
  proves end-to-end connectivity without RNS carrying the data.
- A non-owner or an expired grant cannot open a tunnel; revocation takes effect promptly.
- Stale or false advertisements never bypass allocator-local validation at connect time.
- Deterministic tests cover wrong owner, wrong execution, expiry, reuse, revocation, and restart;
  `make check` passes.
- The contract and core contain no Yggdrasil-specific address type; Yggdrasil stays a network
  choice behind the generic endpoint contract.

## Tasks

- [F14-01 — Add execution-scoped access grant and tunnel endpoint](./f14-01-access-grant.md)
- [F14-02 — Implement the Yggdrasil tunnel service](./f14-02-tunnel-service.md)

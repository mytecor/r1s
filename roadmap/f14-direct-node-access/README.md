# F14. Direct node access (r1s-tunneld)

Corresponds to [milestone F14](../../ROADMAP.md#f14-direct-node-access-r1s-tunneld).

**Status:** ⏳ Planned

## Outcome

An execution owner opens an authenticated tunnel from their client to a running execution on an
allocator over an embedded Yggdrasil node and carries arbitrary traffic inside it (for example SSH
or HTTP). Access is gated by an execution-scoped grant bound to the owner's authenticated identity
and the client's Yggdrasil node key, not by a bearer secret. The tunnel is node-local and
capability-gated, independent of any artifact model, and never travels over RNS.

## Dependencies

- [F2. RNS transport](../f2-rns-transport/README.md)
- [F12. Shared-secret cluster membership](../f12-cluster-membership/README.md)
- [F13. Local client API](../f13-local-client-api/README.md)
- [F17. Execution lease](../f17-execution-lease/README.md)

## Architecture decisions

- **No separate daemon.** The allocator-side tunnel edge is a package inside `r1sd`; the client side
  is the `r1s tunnel` command. The roadmap title keeps the historical `r1s-tunneld` name; no such
  service binary is introduced.
- **Peer-key binding, not a bearer token.** The grant pins the client's Yggdrasil node public key
  over the authenticated control plane; the tunnel edge accepts only an authenticated Yggdrasil peer
  whose key matches a live grant. Nothing secret is embedded in the endpoint advertisement,
  diagnostics, metrics, or durable records.
- **Overlay mesh, not point-to-point peering.** Both processes embed yggdrasil-go; default bootstrap
  joins the public overlay mesh (address derived from a persisted node key), so no host-level daemon,
  no manual peering, and no allocator firewall port is required. A private peer set is a configurable
  edge option for isolated networks.
- **One live session per execution**, bound to the execution lifecycle: terminal state closes the
  session and invalidates its grants. The tunnel adds no second lease; the F17 lease stays the only
  authority on execution lifetime.

## Scope

- Define the control exchange that mints a short-lived, single-use, execution-scoped access grant
  over the authenticated control plane (F14-01): the grant binds execution ID, owner, the client's
  Yggdrasil node key, and expiry.
- Advertise a transport-neutral endpoint through the authenticated control data — the allocator's
  Yggdrasil address and public key — so the client can learn where to connect without embedding an
  address in the immutable workload.
- Serve the allocator-local tunnel edge over an embedded Yggdrasil node, terminating at an
  allocator-owned local target named in execution metadata (F14-01), with all authorization in the
  allocator core and the edge acting as pure plumbing.
- Carry arbitrary traffic inside the tunnel; r1s-tunneld neither parses nor restricts the payload.
- Restrict access to the authenticated execution owner and keep the tunnel independent of artifact
  transfer: lifecycle and debugging access, not a data plane.
- Keep the core mockable: a generic tunnel interface (`internal/tunnel`) in front of an embedded
  yggdrasil-go edge (`internal/tunnel/yggdrasil`).

## Completion criteria

- Protocol validation rejects a grant bound to the wrong execution, owner, or expiry, a reused
  grant, a peer key mismatch, or a grant received from an unverified payload.
- An execution owner opens a tunnel to a running execution and a successful payload round-trip
  proves end-to-end connectivity without RNS carrying the data.
- A non-owner or an expired/revoked/reused grant cannot open a tunnel; there is at most one live
  session per execution; revocation takes effect promptly.
- Stale or false advertisements never bypass allocator-local validation at connect time.
- Terminal execution state closes the execution's live sessions; losing the tunnel never cancels the
  execution.
- Deterministic tests cover wrong owner, wrong execution, expiry, reuse, revocation, peer mismatch,
  and restart; `make check` passes.
- The contract and core contain no Yggdrasil-specific address type; Yggdrasil stays a network choice
  behind the generic endpoint contract, and the core, protocol, and transport adapter build without
  it.

## Tasks

- [F14-01 — Execution-scoped access grant and tunnel endpoint](./f14-01-access-grant.md)
- [F14-02 — Tunnel edge and the `r1s tunnel` command](./f14-02-tunnel-service.md)

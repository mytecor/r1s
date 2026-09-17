# F14-01 — Execution-scoped access grant and tunnel endpoint

**Status:** ⏳ Planned

## Outcome

An authenticated allocator authorizes exactly one short-lived, single-use direct-access tunnel to a
running execution. Access binds the execution owner (authenticated over the RNS control plane) to
the client's edge-node public key (HKDF-derived), so the tunnel edge trusts an authenticated peer
identity at connect time and never a bearer secret. The grant couples the tunnel to no artifact
model.

## Scope

- Add one additive, transport-neutral control pair over the authenticated control plane:

  - `ExecutionTunnelGrant` — client requests a tunnel, carrying `execution_id` and the client's
    edge node public key (`ygg_peer_pubkey`).
  - `ExecutionTunnelGrantAck` — allocator mints the grant and replies with `execution_id`,
    `grant_id`, `expires_at`, and the allocator-local endpoint advertisement, which is rewritten as
    `allocator_endpoint` (opaque transport-specific address bytes) and `allocator_endpoint_pubkey`
    (opaque public-key bytes). The `allocator_ygg_address`/`allocator_ygg_pubkey` names are retired
    before the schema freezes; the contract stays transport-neutral.

  Envelope oneof numbers are appended; existing field numbers stay unchanged.

- The edge-node key bound into the grant is HKDF-derived by the client from its persistent
  identity seed with a distinct `info` string (`r1s-tunnel-node-key-v1`); the allocator derives its
  own edge key from the `r1sd` identity seed with a different `info` string. Keys are never stored
  as separate files and never rotate independently of the identity they derive from. The derivation
  is internal to the edge adapter; the contract carries only opaque key bytes.

- Bind the grant to execution ID, execution owner (the authenticated sender — see Distributed
  authority in [ARCHITECTURE.md](../../ARCHITECTURE.md)), the pinned `ygg_peer_pubkey`, and expiry.

- Keep grants in an allocator-local in-memory registry: single-use (one accept consumes the grant),
  short TTL, expiry evaluated by the issuer locally so host clock skew cannot arise, revocation is
  record removal, reuse of a spent grant is rejected, and grants are neither renewed nor persisted —
  after an allocator restart the client requests a new grant.
- The grant and the active session are one registry record per execution: a repeat mint replaces an
  outstanding unconfirmed grant, a re-mint after a session close is immediate, and terminal-state
  cleanup deletes the single record.
- TTL expiry is evaluated lazily at mint and at accept — there is no TTL sweeper goroutine.
- Session concurrency is capped only per execution: allocator-wide session concurrency is not a v1
  knob; the per-execution cap of one is fixed by the contract.

- Keep exactly one active session per execution (see F14-02); the related F14-01 registry allows a
  repeat mint (replacing the outstanding grant), and a re-mint right after a session close. One
  live session is enforced at accept time, not at mint time: minting is cheap and cancellation at
  accept follows the same path.

- Never treat the cluster key as a bearer credential. The cluster-key proof stays at the RNS
  boundary; the tunnel edge accepts only a peer whose endpoint public key is pinned in a live grant.

- Keep every message field public: an endpoint advertisement (addresses, public keys) is not secret,
  and nothing secret ever appears in descriptors, diagnostics, metrics, or durable records.

## Acceptance

- Mint is rejected for the wrong execution, a sender that is not the execution owner, or an
  execution that is terminal; a grant whose TTL expired or that was already consumed is rejected at
  accept.
- A grant received from an unverified payload or an unverified peer never overrides allocator-local
  authority; a peer node key at connect that does not match the pinned key is rejected.
- Reuse of a spent grant is rejected; allocator restart safely invalidates outstanding grants and
  the client recovers by requesting a new one.
- The contract contains no Yggdrasil-specific field or address *type*: the endpoint advertisement is
  opaque bytes/strings, and the peer binding is an opaque public-key value.
- `make check` passes and existing Protobuf field numbers stay unchanged.

## Server-side target: resolved at grant time

The tunnel must terminate at a concrete local endpoint on the allocator. There is no free address
for "the running execution": a workload on the host network namespace does not automatically expose
one. One slot must therefore be defined before F14-02 is testable:

- v1 resolves the target **at grant time** from allocator-local configuration, not from
  per-execution metadata: a per-resource-class target map plus a mandatory default target. The
  resolved `(host, port)` is bound into the minted grant and handed to the edge at accept.
- The target is never a client-supplied destination and never appears in the immutable workload.
- When no target can be resolved (no class-target match and no default), minting fails with a
  clear `CommandError` instead of connecting by guesswork.
- Later improvement (out of scope) is target auto-discovery from the running workload; until then
  the allocator-local target configuration is the single source of truth, and this limitation is
  recorded in [BACKLOG.md](../BACKLOG.md).

## Notes

This task defines the authorization, endpoint advertisement, and target resolution only. The
allocator-local tunnel edge, the embedded Yggdrasil node, the `r1s tunnel` command, and the
accept-time routing preamble are F14-02. OCI image distribution is outside this task.

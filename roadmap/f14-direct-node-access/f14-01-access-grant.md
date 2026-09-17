# F14-01 — Execution-scoped access grant and tunnel endpoint

**Status:** ⏳ Planned

## Outcome

An authenticated allocator authorizes exactly one short-lived, single-use direct-access tunnel to a
running execution. Access binds the execution owner (authenticated over the RNS control plane) to
the client's Yggdrasil node public key, so the tunnel edge trusts an authenticated peer identity at
connect time and never a bearer secret. The grant couples the tunnel to no artifact model.

## Scope

- Add one additive, transport-neutral control pair over the authenticated control plane:

  - `ExecutionTunnelGrant` — client requests a tunnel, carrying `execution_id` and the client's
    Yggdrasil node public key (`ygg_peer_pubkey`).
  - `ExecutionTunnelGrantAck` — allocator mints the grant and replies with `execution_id`,
    `grant_id`, `expires_at`, and the allocator-local endpoint advertisement (`allocator_ygg_address`
    and `allocator_ygg_pubkey` as opaque bytes).

  Envelope oneof numbers are appended; existing field numbers stay unchanged.

- Bind the grant to execution ID, execution owner (the authenticated sender — see Distributed
  authority in [ARCHITECTURE.md](../../ARCHITECTURE.md)), the pinned `ygg_peer_pubkey`, and expiry.
  There is no byte ceiling and no tunnel direction: the grant answers who may open one tunnel to
  which execution until when. Digest- and ceiling-style payload checks are leftovers of the removed
  data plane (see [BACKLOG.md](../BACKLOG.md), resolved decision 12); bandwidth-like limits are
  allocator admission ([F10](../f10-local-admission/README.md)) territory, never the grant's.

- Keep grants in an allocator-local in-memory registry: single-use (one accept consumes the grant),
  short TTL, expiry evaluated by the issuer locally so host clock skew cannot arise, revocation is
  record removal, reuse of a spent grant is rejected, and grants are neither renewed nor persisted —
  after an allocator restart the client requests a new grant.

- Keep exactly one active session per execution (see F14-02): minting a new grant while a session
  for that execution is open is rejected with a stable, non-retryable `CommandError`.

- Never treat the cluster key as a bearer credential. The cluster-key proof stays at the RNS
  boundary; the tunnel edge accepts only a Yggdrasil peer whose node key is pinned in a live grant.

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

## Server-side target: the connection slot that must be named

The tunnel must terminate at a concrete local endpoint on the allocator. There is no free address
for "the running execution": a workload on the host network namespace does not automatically expose
one. One slot must therefore be defined before F14-02 is testable:

- v1 takes an **allocator-owned target** — a `(host, port)` recorded in local execution metadata,
  populated by allocator-local configuration/policy (for example per resource class or per workload
  label) at allocation time. It is never a client-supplied destination embedded in the immutable
  workload, and it is never a client-provided tunnel argument.
- When no target is configured for an execution, granting the tunnel fails with a clear
  `CommandError` instead of connecting anywhere by guesswork.
- Later improvement (out of scope) is target auto-discovery from the running workload; until then the
  allocator-owned target is the single source of truth, and this limitation is recorded in
  [BACKLOG.md](../BACKLOG.md).

## Notes

This task defines the authorization, endpoint advertisement, and target-slot contract only. The
allocator-local tunnel edge, the embedded Yggdrasil node, and the `r1s tunnel` command are F14-02.
OCI image distribution is outside this task.

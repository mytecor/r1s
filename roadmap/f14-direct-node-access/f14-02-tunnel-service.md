# F14-02 — Tunnel edge and the `r1s tunnel` command

**Status:** ⏳ Planned

## Outcome

An execution owner connects through an allocator-local tunnel edge over an embedded Yggdrasil node
and carries arbitrary traffic (for example SSH or HTTP) to the running execution, gated by the
F14-01 grant plus the authenticated Yggdrasil peer binding, and never routed through RNS. The
allocator-side edge is an internal component of `r1sd`; the client side is a service-backed bridge:
`r1s tunnel` pipes bytes through a running `r1s serve` over the local socket. No separate r1s
daemon is introduced.

## User model

`r1s tunnel 01HZ...ABC` opens a raw interactive pipe between the user's stdin/stdout and the target
inside the execution. It is routed like `request`/`inspect`/`cancel`/`logs`: a running local
`r1s serve` at the default socket is discovered and used to mint the grant. Unlike one-shot
commands, `r1s tunnel` is **service-backed only**: it requires a live local service and is a *live
session*, not a one-shot RPC.

```
# interactive pipe to the running execution, no protocol flags, no --socket needed
r1s tunnel 01HZ...ABC
#   user's stdin/stdout <--> (LocalTunnel bidi stream) <--> r1s serve
#   <--> (Yggdrasil mesh) <--> allocator edge <--> running execution
```

Why service-backed only: the tunnel is a live session whose execution must stay alive for its
duration. Only a `serve` process holds the F17 keep-alive intent that renews the execution lease
[F17](../f17-execution-lease/README.md) while the session runs; a direct-mode invocation would let
the lease lapse (default 10 minutes) and the execution would be evicted mid-session. A direct run
is rejected with a clear `CommandError` telling the user to start `r1s serve`; a future
client-edge bridge for direct mode is BACKLOG work.

The tunnel exposes no protocol-specific flags: no `--ssh`, `--lport`, `--bind`, or anything that
names a protocol or a destination port inside the workload. The tunnel is a raw transport and must
not interpret the payload; the same session carries SSH, HTTP, or anything else the owner chooses.
The execution runs in the host network namespace, so only the allocator-local target resolved at
grant time (F14-01) names where the edge connects. Local port binding, if the user wants it, is a
client-side presentation decision and stays outside the protocol and the allocator contract.

## Process topology: the edge lives inside `r1sd`; the bridge lives inside `r1s serve`

There is no `r1s-tunneld` service process. Splitting the edge into a separate binary would force a
second daemon to hold or re-derive allocator authority (grants, execution lifecycle, identity), and
would contradict the repo's two-binary layout (`r1sd`, `r1s`).

- The allocator-side edge runs inside `r1sd` as a package, enabled by configuration
  (`tunnel.enabled`), and shares the allocator core's in-memory grant-and-session registry
  in-process. All authorization decisions — owner check, execution state, session slot, expiry —
  stay in the allocator core; the edge only reports the authenticated peer key and the routing
  preamble.
- The client-side edge runs inside a persistent `r1s serve` process and shares its identity store
  (HKDF-derived node key) — it is **not** started per command. `r1s tunnel` is a thin byte bridge:
  it opens a bidirectional gRPC stream (`LocalTunnel`) on the existing local Unix socket and pipes
  stdin/stdout to it. The serve process mints the grant, dials the allocator edge, and relays bytes
  between the stream and the tunnel; nothing about the mesh touches the CLI process.
- `LocalTunnel` is added additively to `local.proto` (the local API is versioned and
  local-only; it never travels over RNS). It stays a raw pipe: no protocol-level framing is
  injected into the payload beyond the one-time routing preamble (see below), and stdout carries
  only tunnel bytes — the serve process never writes logs to the pipe.

Package layout mirrors the existing transport pattern (interface + in-memory fake + one network
adapter):

```text
internal/tunnel/            generic tunnel contract, no network library
    tunnel.go               Listener/Dialer/Conn; peer key is an opaque []byte
    registry.go             allocator-side per-execution grant-and-session registry (lazy expiry)
    memory.go               deterministic in-memory fake for tests
internal/tunnel/yggdrasil/  the Yggdrasil edge adapter (the only package importing yggdrasil-go)
    node.go                 embedded node lifecycle: HKDF-derived node key, bootstrap policy
    listen.go               allocator-side overlay listener, exposes authenticated peer key
    dial.go                 client-side overlay dial with peer-key pinning
```

The core, the protocol, the local API, and the transport adapter never import
`internal/tunnel/yggdrasil`.

## Network: embedded yggdrasil-go on the overlay mesh

Each process runs an embedded yggdrasil-go node; no host-level Yggdrasil daemon is required.

- Default bootstrap joins the Yggdrasil overlay through the standard public peers, so no manual
  client↔allocator peering and no allocator firewall port for tunnels is needed: addresses derive
  from node keys, and the mesh routes between the two nodes even across NAT. For isolated networks
  the bootstrap is configurable to a private peer set; that knob is edge configuration, not a
  protocol feature.
- Node keys are HKDF-derived from persistent identity seeds, not persisted separately (see
  [F14-01](./f14-01-access-grant.md)): the client's serve derives its key from the client identity,
  the allocator from the `r1sd` identity. A stable node key means a stable overlay address, so
  grants and sessions survive reconnects within their TTL, and re-requesting after a restart reuses
  the same key. No key files to create, back up, or rotate.
- The allocator binds a tunnel listener on its overlay address; the client dials that address (the
  F14-01 advertisement). Both ends pin the peer node public key — the client pins the allocator's
  key from the ack, the allocator pins the client's key from the grant — so a man-in-the-middle or
  a relay has nothing usable even if it sees the advertisement.

## Authorization placement and lifecycle coupling

- The edge is pure plumbing. At accept it reports the authenticated peer key **and the routing
  preamble** to the allocator core; the core validates the F14-01 grant (owner, execution
  non-terminal, session slot, expiry, peer-key match) before the stream is spliced to the execution
  target. Stale or false advertisements can therefore never bypass allocator-local validation.
- **Routing preamble.** The client sends a short routing preamble as the first bytes of the tunnel
  stream — execution ID and grant ID — before any payload. The preamble disambiguates the registry
  lookup, because one peer key may hold grants for several executions; the core resolves the record
  under the core lock and re-validates execution liveness at splice time. The grant is consumed
  only on a successful splice; a failed validation (mismatched preamble, wrong key, expiry, busy
  slot) leaves the grant unconsumed and the rejection happens before a single payload byte is
  spliced.
- **One registry record per execution.** The grant and the active session are the same record; a
  repeat mint replaces the outstanding grant, a re-mint after a session close is immediate, and
  one live session per execution is enforced at accept (a second accept while a session is open is
  rejected). There is no TTL sweeper goroutine: expiry is evaluated lazily at mint and at accept.
- Sessions are bound to the execution lifecycle. When the workload completes, is cancelled, or its
  F17 lease expires without renewal, the allocator closes that execution's active sessions and
  invalidates its grants in the same local sweep that commits the terminal state — deleting the
  single registry record. Losing the tunnel never cancels the execution and never weakens RNS
  control authority.
- The tunnel adds no second lease: execution lifetime stays governed solely by the F17 execution
  lease.

## Client session behavior

- Raw bidirectional pipe with half-close per direction so interactive protocols (SSH and similar)
  behave correctly; for interactive use the CLI sets the local terminal to raw mode for the session
  and restores it on exit. The CLI stdin/stdout pipe the `LocalTunnel` stream; the serve process
  relays to the edge and back. Diagnostics and logs always go to stderr — stdout is the data
  channel and must stay byte-clean.
- No application-level keepalives or framing are injected into the payload stream: liveness is left
  to the Yggdrasil link layer, and the session ends when either side closes or the execution ends.
- The teardown reason is surfaced on exit (execution ended, grant rejected/expired/revoked, mesh
  unreachable, unauthorized), with a non-zero exit code on abnormal termination, mirroring how other
  commands surface `CommandError` without attaching workload output.

## Acceptance

- A live test over the Yggdrasil mesh opens a tunnel from a client to a running execution and a
  payload round-trip succeeds; the same run proves no tunnel bytes traverse RNS.
- A non-owner, an expired/reused/revoked grant, and a peer key that does not match the pinned key
  cannot open a tunnel; rejected sessions are cleaned up and leave no state behind. A failed
  validation before splice leaves the grant unconsumed and reusable until expiry.
- A routing preamble mismatch (wrong execution or grant ID for the authenticated peer) rejects the
  session before any payload is relayed.
- `r1s tunnel` without a live `r1s serve` fails with a clear `CommandError`; the tunnel flows only
  over the local `LocalTunnel` stream, never over stdout.
- A terminal execution closes its active sessions; tunnel loss never cancels the execution.
- The core, protocol, local API, and transport adapter build without Yggdrasil dependencies;
  deterministic tests cover wrong owner, wrong execution, expiry, reuse, revocation, peer mismatch,
  preamble mismatch, and restart through the in-memory fake and a fake runtime lifecycle.
- `make check` passes.

## Notes

Resolved simplification decisions carried into the docs:

- Eager allocator-side node start when tunneling is enabled (`tunnel.enabled` → node starts with
  `r1sd`), no lazy-start mode; the bootstrap policy is edge configuration.
- No allocator-wide session concurrency cap in v1; the per-execution cap of one is fixed by the
  contract.
- The server-side target is resolved at grant time from allocator-local configuration (per
  resource-class map plus a mandatory default), bound into the grant, and enforced at accept (see
  [F14-01](./f14-01-access-grant.md)); a missing default fails the mint with a clear `CommandError`.
  Per-execution target metadata is not populated.

Still open during implementation and covered by [BACKLOG.md](../BACKLOG.md):

- The exact `tunnel.enabled` configuration surface on the client (`r1s serve`) side and whether the
  client serve starts its edge eagerly at startup or lazily on the first tunnel session.
- Whether the target configuration surface needs a per-identity override beyond the per-resource-class
  map and default.
- TCP-fallback or NAT-traversal plans if a private peer set is not reachable in a future deployment.

# F14-02 — Tunnel edge and the `r1s tunnel` command

**Status:** ⏳ Planned

## Outcome

An execution owner connects through an allocator-local tunnel edge over an embedded Yggdrasil node
and carries arbitrary traffic (for example SSH or HTTP) to the running execution, gated by the
F14-01 grant plus the authenticated Yggdrasil peer binding, and never routed through RNS. The
allocator-side edge is an internal component of `r1sd`, and the client side is the `r1s tunnel`
command; no separate r1s daemon is introduced.

## User model

`r1s tunnel 01HZ...ABC` opens a raw interactive pipe between the user's stdin/stdout and the target
inside the execution. It is routed like `request`/`inspect`/`cancel`/`logs`: a running local
`r1s serve` at the default socket is discovered and used to mint the grant, otherwise the command
runs in direct mode. It is a *live session*, not a one-shot RPC.

```
# interactive pipe to the running execution, no protocol flags, no --socket needed
r1s tunnel 01HZ...ABC
#   user's stdin/stdout <--> (Yggdrasil mesh) <--> allocator edge <--> running execution
```

The tunnel exposes no protocol-specific flags: no `--ssh`, `--lport`, `--bind`, or anything that
names a protocol or a destination port inside the workload. The tunnel is a raw transport and must
not interpret the payload; the same session carries SSH, HTTP, or anything else the owner chooses.
The execution runs in the host network namespace, so only the allocator's local target metadata
(F14-01) names where the edge connects. Local port binding, if the user wants it, is a client-side
presentation decision and stays outside the protocol and the allocator contract.

## Process topology: the edge lives inside `r1sd`

There is no `r1s-tunneld` service process. Splitting the edge into a separate binary would force a
second daemon to hold or re-derive allocator authority (grants, execution lifecycle, identity), and
would contradict the repo's two-binary layout (`r1sd`, `r1s`).

- The allocator-side edge runs inside `r1sd` as a package, enabled by configuration, and shares the
  allocator core's in-memory grant registry and session registry in-process. All authorization
  decisions — owner check, execution state, session slot, expiry — stay in the allocator core; the
  edge only reports the authenticated peer key.
- The client-side edge is started by `r1s tunnel` for the lifetime of the command and torn down with
  it.
- A future standalone `r1s-tunneld` would be at most a thin `main` over the same package; it is not
  built now.

Package layout mirrors the existing transport pattern (interface + in-memory fake + one network
adapter):

```text
internal/tunnel/            generic tunnel contract, no network library
    tunnel.go               Listener/Dialer/Conn; peer key is an opaque []byte
    registry.go             allocator-side grant registry + active-session set (in-memory, TTL sweep)
    memory.go               deterministic in-memory fake for tests
internal/tunnel/yggdrasil/  the Yggdrasil edge adapter (the only package importing yggdrasil-go)
    node.go                 embedded node lifecycle: persisted node key, bootstrap policy
    listen.go               allocator-side overlay listener, exposes authenticated peer key
    dial.go                 client-side overlay dial with peer-key pinning
```

The core, the protocol, and the transport adapter never import `internal/tunnel/yggdrasil`.

## Network: embedded yggdrasil-go on the overlay mesh

Each process runs an embedded yggdrasil-go node; no host-level Yggdrasil daemon is required.

- Default bootstrap joins the Yggdrasil overlay through the standard public peers, so no manual
  client↔allocator peering and no allocator firewall port for tunnels is needed: addresses derive
  from node keys, and the mesh routes between the two nodes even across NAT. For isolated networks
  the bootstrap is configurable to a private peer set; that knob is edge configuration, not a
  protocol feature.
- The allocator binds a tunnel listener on its overlay address; the client dials that address (the
  F14-01 advertisement). Both ends pin the peer node public key — the client pins the allocator's
  key from the ack, the allocator pins the client's key from the grant — so a man-in-the-middle or a
  relay has nothing usable even if it sees the advertisement.
- Node keys are persisted per identity: the client's beside its identity store, the allocator's
  beside `r1sd` state. A stable node key means a stable overlay address, so grants and sessions
  survive reconnects within their TTL, and re-requesting after a restart reuses the same key.

## Authorization placement and lifecycle coupling

- The edge is pure plumbing. At accept it reports the authenticated peer key to the allocator core;
  the core validates the F14-01 grant (owner, execution non-terminal, session slot, expiry, peer-key
  match) before the stream is spliced to the execution target. Stale or false advertisements can
  therefore never bypass allocator-local validation.
- One active session per execution: a second accept or a second mint while one is open is rejected.
- Sessions are bound to the execution lifecycle. When the workload completes, is cancelled, or its
  F17 lease expires without renewal, the allocator closes that execution's active sessions and
  invalidates its grants in the same local sweep that commits the terminal state. Losing the tunnel
  never cancels the execution and never weakens RNS control authority.
- The tunnel adds no second lease: execution lifetime stays governed solely by the F17 execution
  lease.

## Client session behavior

- Raw bidirectional pipe with half-close per direction so interactive protocols (SSH and similar)
  behave correctly; for interactive use the CLI sets the local terminal to raw mode for the session
  and restores it on exit.
- No application-level keepalives or framing are injected into the payload stream: liveness is left
  to the Yggdrasil link layer, and the session ends when either side closes or the execution ends.
- The teardown reason is surfaced on exit (execution ended, grant rejected/expired/revoked, mesh
  unreachable, unauthorized), with a non-zero exit code on abnormal termination, mirroring how other
  commands surface `CommandError` without attaching workload output.

## Acceptance

- A live test over the Yggdrasil mesh opens a tunnel from a client to a running execution and a
  payload round-trip succeeds; the same run proves no tunnel bytes traverse RNS.
- A non-owner, an expired/reused/revoked grant, and a peer key that does not match the pinned key
  cannot open a tunnel; rejected sessions are cleaned up and leave no state behind.
- A terminal execution closes its active sessions; tunnel loss never cancels the execution.
- The core, protocol, and transport adapter build without Yggdrasil dependencies; deterministic
  tests cover wrong owner, wrong execution, expiry, reuse, revocation, peer mismatch, and restart
  through the in-memory fake and a fake runtime lifecycle.
- `make check` passes.

## Notes

Open points to resolve during implementation:

- Eager versus lazy start of the allocator's embedded Yggdrasil node (at `r1sd` startup versus on
  the first grant mint), and the configuration surface for the bootstrap policy.
- Default allocator-wide session concurrency cap as a local policy knob (the per-execution cap of
  one is fixed by the contract).
- The server-side target metadata source (F14-01's slot): which config surface feeds the local
  `(host, port)` target, and whether per-resource-class defaults are enough for v1.

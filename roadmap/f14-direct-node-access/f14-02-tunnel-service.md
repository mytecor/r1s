# F14-02 — Implement the Yggdrasil tunnel service

**Status:** ⏳ Planned

## Outcome

An execution owner connects through an allocator-local tunnel service over Yggdrasil and carries
arbitrary traffic (for example SSH or HTTP) to the running execution, gated by the F14-01 grant and
never routed through RNS.

## User model

`r1s tunnel` is one command, scoped to the execution owner and routed like
`request`/`inspect`/`cancel`/`logs`: it transparently uses a running local `r1s serve` service
(when the default socket is live) or falls back to direct mode, so no `--socket` is required.
It opens a raw interactive pipe between the user's stdin/stdout and the running execution, and is
a *live session*, not a one-shot RPC like the other workflow commands.

The tunnel exposes no protocol-specific flags. It does not add `--ssh`, `--lport`, `--bind`, or any
other surface that names a protocol or a destination port inside the workload:

- The execution runs in the containerd host network namespace (the adapter's spec does not create a
  per-container network namespace), so there is no isolated container network stack and no
  "port inside" to forward to. Local port binding on the client side is a presentation decision,
  not a protocol property, and stays out of the protocol and the allocator contract.
- The tunnel is a raw transport and must not care which protocol rides in it. A flag like
  `--ssh` would make r1s-tunneld interpret the payload, which the contract forbids; the same tunnel
  carries SSH, HTTP, or anything else the owner chooses inside the execution.

The allocator learns where to connect from its own local state (host address + execution identity),
never from a client-supplied destination embedded in the immutable workload. The client supplies
only the execution ID; provision of a useful allocator-host address is allocator-local metadata,
not a client-provided tunnel argument.

```
# interactive pipe to the running execution, no protocol flags, no --socket needed
# (the default socket is discovered when 'r1s serve' is running)
r1s tunnel 01HZ...ABC
#   user's stdin/stdout <--> (Yggdrasil) <--> running execution
```

## Scope

- Implement the allocator-local tunnel endpoint that terminates at the running execution.
- Advertise the endpoint through the authenticated control-plane descriptor from F14-01.
- Enforce grant expiry, byte ceiling, and owner binding at connect and during the session; revoke
  promptly.
- Carry arbitrary bytes in both directions without parsing or restricting the payload.
- Keep the core mockable behind a generic tunnel interface and document firewall, TLS, Yggdrasil
  routing, and failure/retry expectations.
- Keep the tunnel independent of artifact transfer; it is lifecycle/debug access, not a data plane.

## Acceptance

- A live test on Yggdrasil opens a tunnel from a client to a running execution and a payload
  round-trip succeeds.
- A non-owner, an expired grant, and an oversized or digest-irrelevant payload are rejected and the
  session is cleaned up.
- Loss of the tunnel connection does not cancel the execution or weaken the RNS control command
  authority.
- No tunnel bytes traverse RNS, and the core builds without Yggdrasil dependencies.
- `make check` passes.

## Notes

The tunnel carries any protocol the owner chooses; r1s-tunneld neither interprets nor constrains it.

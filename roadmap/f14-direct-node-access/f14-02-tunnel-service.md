# F14-02 — Implement the Yggdrasil tunnel service

**Status:** ⏳ Planned

## Outcome

An execution owner connects through an allocator-local tunnel service over Yggdrasil and carries
arbitrary traffic (for example SSH or HTTP) to the running execution, gated by the F14-01 grant and
never routed through RNS.

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

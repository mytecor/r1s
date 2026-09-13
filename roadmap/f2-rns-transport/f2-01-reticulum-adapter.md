# F2-01 — Integrate Reticulum-Go

**Status:** Planned

## Outcome

Implement the transport interface with Reticulum-Go announces, destinations, links, and channels,
and add the allocator service entry point under `cmd/r1sd/`.

## Acceptance

- Two local `r1sd` processes exchange a validated envelope.
- A forged payload sender cannot override the authenticated link identity.
- Reconnect and path discovery use bounded, event-driven waits.

# F2-01 — Integrate Reticulum-Go

**Status:** In progress

## Outcome

Implement the transport interface with Reticulum-Go announces, destinations, links, and channels,
and add the allocator service entry point under `cmd/r1sd/`.

## Acceptance

- Two local `r1sd` processes exchange a validated envelope.
- A forged payload sender cannot override the authenticated link identity.
- Reconnect and path discovery use bounded, event-driven waits.

## Implemented

- Reticulum-Go is embedded behind [`internal/transport/rns`](../../internal/transport/rns/) and
  referenced as a normal pinned Go module.
- Allocator capacity is encoded in bounded `r1s.v1` announce app data.
- Link identification supplies sender authority; serialized `Envelope.sender` bytes are replaced
  before validation or allocator dispatch.
- Path and link establishment use callbacks plus context-bounded waits.
- [`cmd/r1sd`](../../cmd/r1sd/) loads an explicit Reticulum configuration and persistent service
  identity, wires allocator responses, and shuts down on process cancellation.
- A two-endpoint UDP test covers discovery, authenticated envelope exchange, path expiry, and Link
  re-establishment.

## Remaining

- Add the Python-reference interop harness.
- Complete reliable retransmission acceptance after the fixed Channel implementation is available
  from the canonical Go module path.

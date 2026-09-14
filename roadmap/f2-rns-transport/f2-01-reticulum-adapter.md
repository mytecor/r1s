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
- Python-reference discovery interoperability: a gated live test
  ([`interop_python_test.go`](../../internal/transport/rns/interop_python_test.go)) drives a reference
  peer under [`testdata/`](../../internal/transport/rns/testdata/) and proves the Go endpoint discovers
  the r1s service descriptor announced by an upstream Python RNS node.

## Remaining

- Complete reliable Channel envelope delivery and retransmission acceptance against the Python
  reference node. The Go endpoint can discover — and initiate a link toward — an upstream Python RNS
  node, but envelope delivery over a Reticulum-Go Channel is not yet demonstrated end to end and
  remains an open decision in [BACKLOG.md](../BACKLOG.md) (item 1).

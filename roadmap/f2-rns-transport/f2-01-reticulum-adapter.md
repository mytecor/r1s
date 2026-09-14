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
- Python-reference Channel delivery: the same live harness proves reliable r1s envelope delivery over a
  Reticulum-Go Channel to the upstream Python RNS node. The endpoint discovers the node, initiates a
  link, and sends validated envelopes while a wrapped UDP interface (`NewUDPInterface` + a `Send`
  override) injects channel-context packet loss. The Python reference reassembles, echoes the exact
  bytes over the same Channel, and the Go endpoint re-validates them with the authenticated identity;
  the injected drop is confirmed and recovered by Channel retransmission.

## Remaining

- The production containerd/OCI runtime adapter (F3) and durable allocator persistence remain open
  work tracked in [BACKLOG.md](../BACKLOG.md).

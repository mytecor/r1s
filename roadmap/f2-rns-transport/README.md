# F2 — RNS transport

**Status:** Complete

## Outcome

Two r1s processes discover allocator capacity and exchange authenticated Protobuf envelopes through
Reticulum-Go without tunnelling gRPC or HTTP/2 over RNS.

## Completion criteria

- Allocators announce small service descriptors.
- Control messages travel over authenticated Links with reliable ordering.
- The adapter derives envelope sender authority from the link identity.
- Interoperability is tested against the Python reference implementation.

## Tasks

- [F2-01 — Integrate Reticulum-Go](./f2-01-reticulum-adapter.md)

## Implementation

- [`internal/transport/rns`](../../internal/transport/rns/) embeds the canonical Reticulum-Go module,
  announces bounded allocator descriptors, establishes Links, and overwrites envelope sender data
  with authenticated identities.
- [`cmd/r1sd`](../../cmd/r1sd/) wires the adapter to the allocator core while F3 still owns the OCI
  runtime integration.
- A loopback UDP test exercises announce and path discovery, Link establishment and re-establishment,
  identification, Channel delivery, validation, and forged-sender replacement between two
  independent endpoints.
- A gated live interoperability harness ([`interop_python_test.go`](../../internal/transport/rns/interop_python_test.go))
  and reference peer ([`testdata/python_reference_peer.py`](../../internal/transport/rns/testdata/python_reference_peer.py))
  prove the Go endpoint discovers an r1s descriptor announced by an upstream Python RNS node and
  reliably delivers r1s envelopes over a Reticulum-Go Channel to that node, including recovery from
  injected channel-packet loss.

## Follow-on work

- The production containerd/OCI runtime, durable allocator persistence, and the result contract are
  owned by later features and [BACKLOG.md](../BACKLOG.md); they are not F2 completion criteria.

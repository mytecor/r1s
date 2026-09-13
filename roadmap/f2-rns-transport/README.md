# F2 — RNS transport

**Status:** In progress

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

## Remaining

- Run and automate interoperability against the Python reference implementation.
- Verify reliable Channel retransmission and reconnect behavior against a canonically published
  Reticulum-Go release containing the post-0.9 Channel fixes.

# F2 — RNS transport

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


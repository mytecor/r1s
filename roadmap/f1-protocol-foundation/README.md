# F1 — Protocol foundation

## Outcome

A transport-independent Go core can validate control messages, reserve allocator capacity, assign
and cancel executions, and exercise message delivery without a live RNS network.

## Completion criteria

- Protobuf schema is versioned and generated code is reproducible.
- Invalid envelopes are rejected before state mutation.
- Request, offer, assignment, and cancellation transitions are tested.
- Capacity includes outstanding offers as well as running executions.
- Duplicate delivery and offer expiry behavior are completely tested.

## Tasks

- [F1-01 — Define the control protocol](./f1-01-control-protocol.md)
- [F1-02 — Implement allocator transitions](./f1-02-allocator-core.md)
- [F1-03 — Add deterministic transport](./f1-03-memory-transport.md)
- [F1-04 — Complete protocol foundations](./f1-04-foundation-hardening.md)


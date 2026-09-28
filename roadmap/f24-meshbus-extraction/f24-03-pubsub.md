# F24-03 — Add bounded pub/sub fan-out

**Status:** ✅ Complete

## Outcome

The public [`meshbus`](https://github.com/mytecor/meshbus/tree/v0.2.0) package provides a brokerless in-memory event bus over its
authenticated direct-message sender. `Publish` creates one event and sends it once to each unique
destination in the current authenticated peer snapshot. Receiving dispatches only to exact local
topic subscriptions.

This layer is intentionally not a distributed log. It keeps no durable events, replay cursor,
consumer group, offset, acknowledgement journal, forwarding graph, or exactly-once state.

## Event contract

The compact `MBE` v1 frame carries:

- a random 128-bit event ID;
- a validated topic of at most 128 bytes;
- publication time and bounded millisecond TTL;
- an optional bounded printable content type;
- a bounded opaque payload, which may be empty.

The frame does not contain a sender. `ReceivedEvent.Sender` always comes from the authenticated
`ReceivedMessage` that carried the frame. Publication time is metadata: expiry is bounded by both
`published_at + TTL` and `received_at + TTL`, so a future publisher clock cannot extend retention
beyond one TTL after receipt.

## Resource bounds

- Deduplication uses a finite in-memory event-ID cache and evicts the entry with the earliest
  expiry when full.
- Each exact-topic subscription has one worker and a finite non-blocking queue.
- A full interested queue reports explicit backpressure without blocking unrelated subscriptions.
- Subscription count and fan-out peer count have no artificial global caps.
- Fan-out uses a fixed worker count, attempts every authenticated destination in the snapshot, and joins
  partial delivery errors.
- Duplicate peer destinations are removed before fan-out.

Defaults are one-minute event TTL, one-hour maximum TTL, 64 MiB payloads, 4096 dedup entries, 32
queued events per subscription, and eight concurrent sends. RNS sends payloads up to the negotiated
Channel MDU as Channel messages and larger payloads as Resources on the authenticated Link.

## r1s boundary

r1s does not instantiate `meshbus.Bus`. Execution request/offer/assign, leases, logs, and tunnel
authorization remain direct r1s protocol messages. The existing r1s Channel message type and bytes
are unchanged. F24-04 extracted a transport-independent peer discovery contract
(`PeerDirectory`), F24-05 extracted a reusable `meshbus/rns` adapter, and F24-06 moved r1s onto it,
so other applications can supply an authenticated peer snapshot without importing r1s descriptors.

## Acceptance

- Publish fans out once to every unique peer with bounded concurrency and reports partial failure.
- The receiver exposes only the direct transport's authenticated sender.
- Expired and duplicate events are not dispatched.
- Deduplication storage remains within its configured capacity.
- Exact topic filtering, malformed frames, and queue backpressure have
  deterministic tests.
- Ordinary direct messages fall through to a composed direct-message handler.
- The generic package imports only the Go standard library.
- `make check` passes.

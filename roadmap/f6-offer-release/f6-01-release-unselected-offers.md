# F6-01 — Release unselected offers

**Status:** 🚧 Implemented; live Linux partition-recovery rerun passed on `mytecor-homelab` on 2026-09-15

## Outcome

Keep `ExecutionOffer` as a hard, time-bounded capacity lease, but let the client explicitly release
every known offer it did not select. This removes the normal-case capacity delay caused by waiting
for offer expiry without weakening the single-assignment behavior required during disconnection.

## Chosen design

The request and assignment exchange remains a lease-based two-phase operation:

1. An allocator with available capacity creates an `ExecutionOffer` and reserves one local slot.
2. The client collects offers, selects one, and atomically persists the selected assignment together
   with stable release intent for every known losing offer.
3. The client sends `ExecutionAssign` only to the selected allocator and sends an authenticated,
   additive offer-release command to each losing allocator.
4. An allocator releases capacity only when the offer is still outstanding and belongs to the
   authenticated sender. Release is idempotent under duplicate delivery.
5. A release for the selected or already assigned offer must not stop or alter the execution.
6. Offer expiry remains authoritative cleanup when the client crashes, a release is lost, or a late
   offer cannot be returned before the client disconnects.

Release commands use stable, durable message IDs so retry after a client restart sends the same
command. Allocator replay records preserve the same response or error across duplicate delivery and
restart. The protocol change must be additive: existing Protobuf field numbers remain unchanged,
and new payloads receive new field numbers.

The client must also handle an offer received after selection. If its request already has a selected
execution and the offer is not the selected one, it records and attempts a release instead of
silently discarding the offer. Expiry still bounds reservations that arrive after the client process
has exited and therefore cannot be observed.

## Why this design

A hard offer is a useful promise rather than a stale capacity observation: until `expires_at`, the
client can assign it without racing another client for the same slot. This matters on RNS paths where
request, offer, and assignment latency may be variable and where an absent response does not prove
that a remote mutation failed.

The current TTL-only cleanup preserves that guarantee but strands all known losing reservations
until expiry. Explicit release recovers those slots in the normal connected case, while the existing
expiry mechanism retains bounded cleanup under partitions. The design therefore improves capacity
utilization without introducing consensus, a global scheduler, or connection-dependent execution
lifetime.

## Alternatives considered

### Soft advisory offers

An allocator could report that capacity is currently free without reserving it, then atomically
accept or reject `ExecutionAssign`. This improves utilization while clients compare offers, but the
offer can become stale before assignment when multiple clients compete for one slot. It also needs a
new rejection-and-fallback workflow.

Soft offers are not selected because a timeout after assignment is ambiguous: the allocator may
have accepted and started the workload even though the acknowledgement was lost. Assigning the same
request to a fallback allocator can then create two executions, and independent allocators have no
shared state with which to prevent that outcome.

### Reserve only during assignment

Skipping offers and claiming capacity only at assignment has the same stale-capacity and ambiguous
failover problems as soft offers. It also removes the client's ability to compare guaranteed choices
before committing.

### Keep TTL-only release

The existing behavior is simple and partition-safe, but every allocator that loses a client-side
selection retains a slot until offer expiry. Fan-out requests can therefore create avoidable bursts
of artificial capacity exhaustion.

### Shorten the offer TTL

A shorter TTL reduces stranded capacity but also reduces the time available to collect offers and
deliver an assignment over a slow or intermittently connected RNS path. It tunes the tradeoff rather
than removing it.

### Assign several allocators and keep the first acceptance

Parallel assignment lowers latency but permits duplicate workload starts because allocators cannot
observe each other's decisions. Making that safe would require shared coordination, fencing, or an
application-level idempotency contract, all outside the current authority model.

## Scope

- Add request and acknowledgement payloads for offer release to `r1s.v1` without changing existing
  field numbers.
- Define correlation, validation, authorization, and terminal response semantics.
- Add an allocator transition from outstanding to released that immediately returns the reserved
  class slot and is safe under duplicates, expiry, and restart.
- Persist released offer state and replay results without allowing released offers to be assigned.
- Persist stable client release messages alongside the selected assignment.
- Send releases to all known losing allocator routes and handle late offers after selection.
- Document diagnostics for unreachable losing allocators; expiry remains the final fallback.

## Acceptance

- With two one-slot allocators offering for one request, selection assigns exactly one allocator and
  the other reports its slot available immediately after release rather than after the offer TTL.
- Repeating the same release before and after either process restarts does not double-release
  capacity and returns a stable result.
- A different authenticated client cannot release another client's offer.
- A release delayed until after expiry is a safe idempotent no-op.
- Releasing an assigned offer cannot cancel, stop, or mutate its execution.
- A late losing offer received after durable selection is released without changing the selected
  allocator or assignment envelope.
- Injected loss of every release message leaves capacity bounded by the existing offer expiry.
- Assignment and release reordering cannot release the selected reservation or produce more than one
  runtime start.
- Protocol validation, allocator capacity accounting, durable client replay, RNS delivery, and the
  complete partition-recovery scenario pass under `go test -race ./...`.

## Notes

This task optimizes control-plane capacity accounting only. It does not authorize speculative image
pulls, add global scheduling, or change execution lifetime semantics. Whether selected workload
classes should prepare images before assignment remains a separate open decision in
[BACKLOG.md](../BACKLOG.md).

## Implementation and verification

The additive payloads use envelope field numbers 16 and 17. Released offers remain durable and
cannot be assigned. Release acknowledgement is correlated to the original command and checked
against the authenticated allocator identity. Selection and losing release intent share one client
store commit; failed commits roll back both. Late-offer and acknowledgement commits also roll back
on failure.

The CLI runs a bounded release worker alongside network commands. It retries stable commands at
most once per second with at most four concurrent sends and 500 ms per attempt. Shutdown allows
up to two seconds for acknowledgements. Pending releases are reported on stderr and retried by a
subsequent network command while unexpired. An acknowledgement that a losing offer was assigned
is treated as a conflict rather than permission to change the selected execution.

Deterministic tests cover immediate two-allocator capacity release before assignment, replay and
restart, authenticated authority, post-assignment and post-expiry no-ops, concurrent releases, failed
store commits, durable late offers, lost acknowledgements, and worker cancellation. The RNS UDP
loopback test carries release and acknowledgement envelopes with verified sender identity. The live
Linux containerd partition-recovery harness was rerun on `mytecor-homelab` on 2026-09-15
(`TestPartitionRecovery`, containerd 2.3.4 / runc 1.4.3 / Go 1.26.7, digest-pinned Alpine
fixture) and passed in 21s, exercising the F6 release worker, F8 revisions, and F9 log retention
against real containers.

# F6 — Efficient offer release

Corresponds to the [F6 milestone](../../ROADMAP.md#f6-efficient-offer-release).

**Status:** 🚧 Implemented; live Linux partition-recovery rerun pending

## Outcome

After durably selecting one hard offer, the client explicitly releases every known unselected offer
so losing allocators promptly recover reserved capacity. The existing offer expiry remains a safe
fallback for lost messages, client crashes, and network partitions.

## Dependencies

- [F1 — Protocol foundation](../f1-protocol-foundation/README.md)
- [F2 — RNS transport](../f2-rns-transport/README.md)
- [F4 — Client workflow](../f4-client-workflow/README.md)
- [F5 — Partition recovery](../f5-partition-recovery/README.md)

## Scope

- Extend the versioned protocol additively with authenticated offer release messages.
- Release only outstanding offers owned by the authenticated client.
- Persist client release intent and allocator release state for replay-safe restart behavior.
- Release known losing offers after selection and offers that arrive after a selection was made.
- Retain offer expiry as the cleanup path when explicit release cannot be delivered.
- Preserve the rule that only assignment starts a workload.

## Completion criteria

- Losing offers stop consuming allocator capacity promptly after client selection.
- Duplicate, delayed, unauthorized, and post-expiry releases are deterministic and safe.
- A selected offer cannot be released accidentally by loser cleanup or message reordering.
- Client or allocator restart does not lose a committed assignment or create a second execution.
- Existing partition-recovery guarantees continue to pass under race-enabled tests.

## Tasks

- [F6-01 — Release unselected offers](./f6-01-release-unselected-offers.md)

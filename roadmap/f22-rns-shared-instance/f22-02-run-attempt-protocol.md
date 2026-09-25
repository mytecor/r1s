# F22-02 — Stable run identity and at-least-once attempts

**Status:** ✅ Complete (2026-09-25)

## Outcome

One logical run has a stable `run_id` and a monotonic attempt number. Each attempt receives a new
`execution_id`; rescheduling never pretends that an execution migrated or retained its identity.
The protocol and documentation explicitly define execution across ambiguous lease loss as
at-least-once, not exactly-once.

## Scope

- Add `run_id` and `attempt` to `ExecutionRequest` with new Protobuf field numbers and validation:
  `run_id` is stable for the run process and `attempt` starts at one and increases for each fresh
  announcement.
- Persist both values in allocator execution state and expose them as container labels and
  environment variables so workloads can implement fencing or idempotency.
- Keep `execution_id` as the immutable identifier of one allocator attempt.
- Define rescheduling triggers: explicit authenticated `EXPIRED`/`NOT_FOUND`, or a conservative
  lease-loss threshold. A timeout or one missing renewal acknowledgement is never proof that an
  attempt stopped.
- Document that two attempts may overlap when renewal succeeded but its acknowledgement was lost;
  no coordinator or exactly-once claim is introduced.
- Preserve authenticated sender identity as the authority for every attempt; payload `run_id` never
  grants ownership.
- Keep `Inspect`, `Cancel`, lease renewal, and `OfferRelease` protocol operations for internal run
  recovery and cleanup.

## Acceptance

- Protocol validation rejects an empty/malformed `run_id`, attempt zero, and a replay that mutates
  the run identity or attempt while preserving the same command identity.
- Allocator restart recovery preserves `run_id`/`attempt` and restores the corresponding runtime
  labels/environment.
- A lost first renewal acknowledgement does not announce attempt 2.
- Confirmed expiry/not-found advances the attempt, creates a new execution ID, and never pins the
  request to the old allocator.
- Tests cover the ambiguous-renewal overlap and assert at-least-once behavior without weakening
  replay protection or lease eviction.
- Generated Protobuf artifacts are current and `make check` passes.

## Notes

- The stable workload-facing labels are `io.r1s.run-id` and `io.r1s.attempt`; the environment
  variables are `R1S_RUN_ID` and `R1S_ATTEMPT`. Runtime-supplied values override colliding workload
  environment keys.
- `run_id` is correlation and fencing context, not a durable server-side run object.

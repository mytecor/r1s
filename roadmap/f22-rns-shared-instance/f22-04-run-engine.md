# F22-04 — Ephemeral run engine and deterministic placement

**Status:** ✅ Complete

## Outcome

`r1s run` owns the complete in-memory control lifecycle of one logical run: discovery, offer
selection, assignment, lease renewal, completion, and rescheduling. No durable client database or
controller is required. F22-05 and F22-06 attach log tailing and published ports to this run loop.

## Scope

- Generate a fresh client identity in memory for each run and use its transport-verified identity
  for control, lease, log, and tunnel authorization. Persistent identity remains allocator-only.
- Implement the loop: announce, collect compatible offers, deterministically select, release every
  loser, assign, observe/inspect after reconnect, renew the lease, and either finish or reschedule.
- Remove allocator pinning. Initial placement and every later attempt use the same ordinary
  distributed selection policy.
- Define a total, deterministic offer ordering after mandatory constraint compatibility. Stable
  allocator identity is the final tie-breaker; map iteration and goroutine arrival order must not
  influence selection.
- On SIGINT/SIGTERM, stop renewal and attempt an authenticated best-effort cancel. Lease expiry is
  the correctness fallback when the allocator cannot be reached.
- Exit with a documented status derived from workload completion or run failure.
- Keep the core independent of Reticulum-Go through the current transport interface.

## Acceptance

- Repeated tests with identical offers in randomized delivery/map order select the same allocator.
- All known losing offers are released, with expiry remaining the partition-safe fallback.
- Temporary disconnect shorter than the lease duration does not terminate or reschedule work;
  reconnect uses authenticated inspection to recover current state.
- A process restart creates a new identity and cannot reclaim an old execution; the old lease
  expires independently.
- Signals do not tie allocator execution lifetime to connection teardown and do not wait for the
  full lease when best-effort cancellation succeeds.
- `go test -race ./...` and `make check` pass.

## Implemented

- `r1s run <cluster> [--offer-wait duration] '<ExecutionRequest JSON>'` resolves the positional
  cluster credential, creates a fresh in-memory RNS identity and client engine, and never opens a
  client state database.
- Selection requires compatible, unexpired offers and orders them by path hops, exact cached-image
  evidence, authenticated allocator identity, and (only for duplicate allocator offers) offer ID.
  Randomized-order coverage proves map insertion and delivery order do not affect the winner.
- Every selection records releases for all known losers. The existing release worker retries them
  while their reservations remain live; offer expiry remains the unreachable-allocator fallback.
- The foreground loop renews the selected execution, performs authenticated inspection after quiet
  periods/reconnects, ignores inconclusive timeouts, and advances the same `run_id` only after an
  authenticated `NOT_FOUND`/`EXPIRED` or the stable lease-expiry terminal state.
- SIGINT/SIGTERM stops the loop and sends a bounded best-effort authenticated cancellation using a
  fresh context, rather than waiting for lease expiry or treating transport teardown as execution
  authority.
- Exit status is the workload status when it is in the range 1–255, zero for successful completion,
  one for a terminal failure without a usable workload status, 130 for SIGINT, and 143 for SIGTERM.

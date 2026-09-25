# F22-04 — Ephemeral run engine and deterministic placement

**Status:** ⏳ Planned

## Outcome

`r1s run` owns the complete in-memory lifecycle of one logical run: discovery, offer selection,
assignment, lease renewal, completion, rescheduling, log tailing, and published ports. No durable
client database or controller is required.

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

## Notes

- The exact scoring inputs beyond compatibility and the stable identity tie-breaker are tracked in
  [BACKLOG.md](../BACKLOG.md); they must be settled before implementation begins.

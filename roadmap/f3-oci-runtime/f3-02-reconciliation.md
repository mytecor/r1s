# F3-02 — Persist and reconcile allocator state

**Status:** Planned

## Outcome

Allocator offers, executions, and recovery metadata survive restart, and `r1sd` reconciles that
state with r1s-labelled containers and tasks already present in containerd.

## Scope

- Resolve the durable-store decision in [BACKLOG.md](../BACKLOG.md).
- Persist accepted requests, offers, assignments, terminal state, and policy timing data.
- Enumerate only containers whose r1s labels and stored fingerprints match durable allocator state.
- Reattach completion monitoring and local deadline enforcement without restarting running tasks.
- Resolve stopped, missing, foreign, and partially-created containerd objects deterministically.

## Acceptance

- Restarting `r1sd` while a workload runs neither stops nor duplicates that workload.
- A completion that occurs while `r1sd` is offline is recovered with its exit code.
- Missing or conflicting containerd metadata cannot impersonate a stored execution.
- Reconciliation and replay tests pass under `go test -race ./...`.

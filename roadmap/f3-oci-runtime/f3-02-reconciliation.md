# F3-02 — Persist and reconcile allocator state

**Status:** In progress

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

## Implemented

- A `StateStore` boundary keeps allocator persistence independent of the database implementation;
  the shipped adapter uses a transactional, owner-only bbolt file.
- Versioned snapshots persist offers, requests, assignments, terminal state, replay responses and
  errors, authenticated owner identities, and durable execution start times.
- Startup rebuilds capacity accounting, rejects state belonging to another allocator identity, and
  restores classifiable replay errors.
- Runtime recovery is distinct from `Start`: containerd reattaches only to the deterministic
  container whose execution and specification labels match durable state. Missing tasks and
  never-started partial tasks are not recreated.
- Running and already-stopped tasks regain completion monitoring. Original deadlines and maximum
  runtime continue from the durable start time rather than restarting with the daemon.
- Persisted cancellations finish idempotently; missing or conflicting runtime objects become failed
  executions and release their capacity.
- Terminal state is stored before task/container cleanup, preserving stopped metadata for another
  recovery attempt if persistence fails.
- Deterministic tests cover restart, replay, offline completion, missing/conflicting metadata, and
  identity mismatch. `TestContainerdFixtureRecovery` provides gated live coverage.

## Verification

- `go test -race ./...`

## Remaining

- Gated live verification still needs a Linux host with containerd and a digest-pinned fixture
  image; use the command documented in the project [README](../../README.md).

# F15-01 — Add durable `r1s deploy` reconciliation

**Status:** ❌ closed 2026-09-16 without implementation — the durable single-execution part landed
as the [F17-01](../f17-execution-lease/f17-01-execution-lease.md) keep-alive intent, which was
itself removed by [F22-07](../f22-rns-shared-instance/f22-07-client-cleanup.md); the manifest
scope below is deferred (see [BACKLOG.md](../BACKLOG.md)). Historical record only.

## Outcome

The CLI accepts a versioned desired-workload manifest, converges each named deployment to one r1s
execution, and safely replaces or removes executions without losing owner authority or replay
safety.

## Scope

- Define and validate the initial manifest version, canonical specification encoding, revision hash,
  and full-manifest removal semantics.
- Add `r1s deploy apply <manifest>` with the following transitions:
  - absent deployment: request and record one execution;
  - unchanged revision with a non-terminal execution: no-op;
  - changed revision: request and persist a candidate, wait for `RUNNING`, promote it, then cancel
    the previous execution;
  - failed or timed-out candidate: retain the previous execution and expose the failure in status;
  - deployment absent from the desired manifest: cancel its recorded execution before removing the
    durable deployment record.
- Add `r1s deploy status` with desired revision, active and candidate execution IDs, allocator,
  execution phase, and last reconciliation error, without including container log content.
- Persist every externally visible transition before performing the next side effect so restart or
  repeated apply resumes the same reconciliation instead of issuing a second request or cancelling
  the wrong execution.
- Reuse the selected direct or service-backed client frontend and the authenticated owner identity;
  do not add deployment messages to the RNS protocol or deployment state to allocators.

## Acceptance

- A table-driven reconciler test covers create, unchanged no-op, successful replacement, failed
  replacement, removal, and recovery from every persisted intermediate phase.
- Repeating apply before and after a process restart produces at most one execution for a desired
  revision and never cancels an execution that was not recorded as the previous active execution.
- A lost or ambiguous assignment response is recovered through the durable request state rather
  than treated as permission to submit a replacement request.
- Direct-mode and service-backed CLI acceptance tests exercise create, status, update, and removal.
- Existing authority, disconnection lifetime, explicit-log-request, retention, and replay tests stay
  green; `make check` passes.

## Notes

`deploy` is not an alias for `request`: `request` creates one immutable execution, while `deploy`
owns a durable name, desired revision, and replacement state for that execution. `RUNNING` is the
initial promotion threshold and means only that the runtime started the workload; application
readiness and traffic management are outside this task.

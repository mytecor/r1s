# F15. Deployment reconciliation

Corresponds to [milestone F15](../../ROADMAP.md#f15-deployment-reconciliation).

**Status:** ⏳ Planned

## Outcome

`r1s deploy` applies a versioned desired-workload manifest and durably reconciles each named
deployment to one execution. Updating a specification starts its replacement before cancelling the
previous execution, while an unchanged apply performs no execution operation.

## Dependencies

- [F4. Client workflow](../f4-client-workflow/README.md)
- [F5. Partition recovery](../f5-partition-recovery/README.md)
- [F13. Local client API](../f13-local-client-api/README.md)
- [F17. Execution lease](../f17-execution-lease/README.md) — deploy reconcilers renew the leases
  of the executions they manage.

## Scope

- Add `r1s deploy apply <manifest>` and `r1s deploy status`.
- Define a versioned local manifest with stable deployment names and ordinary r1s workload,
  execution-policy, and resource-class fields.
- Derive a deterministic revision from the canonical execution specification.
- Persist desired revision, reconciliation phase, request ID, execution ID, and allocator route in
  state bound to the owning client identity.
- Reconcile missing, unchanged, changed, and removed deployments through the existing request,
  inspect, cancel, and watch operations.
- Use create-before-destroy for changes and retain the previous execution when the replacement does
  not reach `RUNNING`.
- Keep deployment state out of allocator persistence and the RNS control protocol.

The first version manages one execution per deployment. Replicas, application health and readiness
probes, continuous self-healing, rollout strategies, traffic switching, connection draining, and
load balancing require separate tasks.

## Completion criteria

- Applying a new deployment creates exactly one execution and records its durable identity.
- Reapplying an unchanged manifest is a no-op, including after the CLI or local client service
  restarts.
- Applying a changed specification reaches `RUNNING` before cancelling the previous execution.
- Failure, timeout, or ambiguous delivery while creating the replacement never cancels the previous
  execution or authorizes a duplicate replacement.
- Removing a deployment from a full desired-state manifest explicitly cancels its recorded
  execution and removes the deployment only after durable terminal confirmation.
- Deployment state is isolated by client identity and cannot grant authority over executions owned
  by another identity.
- Deterministic direct-mode and service-backed acceptance tests pass under `go test -race`; `make
  check` passes.

## Tasks

- [F15-01 — Add durable `r1s deploy` reconciliation](./f15-01-deploy-reconciliation.md)

# F18-01 — Add local inspection, metrics, and structured logs

**Status:** ⏳ Planned

## Outcome

Expose bounded operator-facing observations through the client and allocator without turning those
observations into a new authority source.

## Scope

- Add allocator, execution, event, and statistics views to the local client surface.
- Add a configurable allocator-local Prometheus endpoint.
- Emit structured service lifecycle and control events with secret redaction.
- Document metric stability, label budgets, and stale-state semantics.

## Acceptance

- Tests cover metric values, bounded labels, redaction, and stale-state presentation.
- No command or metric endpoint implicitly retrieves container stdout/stderr.
- Telemetry endpoint failure cannot fail or cancel a workload.
- `make check` passes.

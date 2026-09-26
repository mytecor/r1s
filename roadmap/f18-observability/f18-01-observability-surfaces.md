# F18-01 — Add local inspection, metrics, and structured logs

**Status:** ⏳ Planned — realigned to the F22 model: no `inspect`/`result`/`logs`/`list` CLI
commands remain, so "local client surface" means `r1s run` output, allocator-local metrics and
structured logs, and the explicit authenticated run-tail log stream. See
[F22-07](../f22-rns-shared-instance/f22-07-client-cleanup.md).

## Outcome

Expose bounded operator-facing observations through the client and allocator without turning those
observations into a new authority source.

## Scope

- Add allocator, execution, event, and statistics views to the `r1s run` / `r1sd` surface.
- Add a configurable allocator-local Prometheus endpoint.
- Emit structured service lifecycle and control events with secret redaction.
- Document metric stability, label budgets, and stale-state semantics.

## Acceptance

- Tests cover metric values, bounded labels, redaction, and stale-state presentation.
- No command or metric endpoint implicitly retrieves container stdout/stderr.
- Telemetry endpoint failure cannot fail or cancel a workload.
- `make check` passes.

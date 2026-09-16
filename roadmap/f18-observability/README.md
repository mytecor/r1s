# F18. Observability

Corresponds to [milestone F18](../../ROADMAP.md#f18-observability).

**Status:** ⏳ Planned

## Outcome

Operators can inspect allocator and execution health through local commands, structured service logs,
and standard metrics export points without adding a mandatory telemetry backend or global state.

## Dependencies

- [F13. Local client API](../f13-local-client-api/README.md)

## Scope

- Add client views for discovered allocators, executions, recent control events, and local statistics.
- Add allocator-local Prometheus metrics for requests, offers, running executions, duration, latency,
  and RNS control bytes.
- Standardize structured service logs with stable event names and redaction rules.
- Keep workload stdout/stderr in the existing explicit authenticated retrieval path.
- Provide export points only; do not bundle Grafana, OpenTelemetry collectors, or a global database.

## Completion criteria

- Commands distinguish locally observed state from allocator-authoritative state and stale discovery.
- Metrics have documented units, labels, and bounded cardinality.
- Capabilities, join tokens, private identities, environment secrets, and workload output are redacted.
- Scraping or losing a telemetry consumer never changes execution lifetime.

## Tasks

- [F18-01 — Add local inspection, metrics, and structured logs](./f18-01-observability-surfaces.md)

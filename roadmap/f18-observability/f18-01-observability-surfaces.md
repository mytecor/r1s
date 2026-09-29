# F18-01 — Add local inspection, metrics, and structured logs

**Status:** ✅ Complete — landed: `r1sd --metrics-address` exposes Prometheus metrics,
`--log-json` emits structured lifecycle events with secret redaction, and `client.Stats()`
exposes in-memory controller views. See
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

## Metrics catalog

| Metric | Type | Unit | Labels | Description |
| --- | --- | --- | --- | --- |
| `r1s_allocator_requests_total` | Counter | requests | `resource_class` | Total execution requests evaluated by the allocator. |
| `r1s_allocator_offers_total` | Counter | offers | `resource_class` | Total execution offers minted by the allocator. |
| `r1s_allocator_executions_total` | Counter | executions | `resource_class`, `phase` | Total executions reaching terminal states (`completed`, `failed`, `cancelled`). |
| `r1s_allocator_rejections_total` | Counter | rejections | `reason` | Total command rejections categorized by fixed reason code. |
| `r1s_allocator_lease_evictions_total` | Counter | evictions | (none) | Total executions evicted due to unrenewed client leases. |
| `r1s_allocator_rns_envelopes_inbound_total` | Counter | envelopes | (none) | Total inbound RNS control envelopes received. |
| `r1s_allocator_rns_envelopes_outbound_total` | Counter | envelopes | (none) | Total outbound RNS control envelopes sent. |
| `r1s_allocator_rns_control_bytes_inbound_total` | Counter | bytes | (none) | Total inbound RNS control bytes received. |
| `r1s_allocator_rns_control_bytes_outbound_total` | Counter | bytes | (none) | Total outbound RNS control bytes sent. |
| `r1s_allocator_execution_duration_seconds` | Histogram | seconds | `resource_class`, `phase` | Execution duration in seconds from assignment to terminal state. |
| `r1s_allocator_dispatch_latency_seconds` | Histogram | seconds | `command` | Latency of allocator command dispatch handling in seconds. |
| `r1s_allocator_active_executions` | Gauge | executions | `resource_class` | Current number of non-terminal running executions. |
| `r1s_allocator_capacity_slots` | Gauge | slots | `resource_class` | Total configured capacity slots by class. |
| `r1s_allocator_available_slots` | Gauge | slots | `resource_class` | Current available unreserved capacity slots by class. |

### Label budgets and stability

Metric label sets are strictly bounded to prevent cardinality explosion:
- `resource_class`: bounded to statically configured operator classes (defaults to `default`),
  plus the fixed `__unconfigured__` label for rejected or retained legacy classes.
- `phase`: bounded to terminal protocol execution phases (`completed`, `failed`, `cancelled`).
- `command`: bounded to protocol command types (`request`, `assign`, `cancel`, `inspect`, `release`, `renew`, `tunnel_open`, `logs`, `unknown`).
- `reason`: bounded to fixed protocol rejection categories (`CAPACITY`, `INCOMPATIBLE`, `ADMISSION`, `UNAUTHORIZED`, `NOT_FOUND`, `EXPIRED`, `CONFLICT`, `TUNNEL`, `INVALID_REQUEST`, `UNAVAILABLE`, `OUTCOME_UNKNOWN`, `INTERNAL`).
- Dynamic runtime IDs (`execution_id`, `request_id`, `message_id`), hashes, identities, tokens, and workload stdout/stderr are **never** permitted as label values.

### Stale-state semantics

- Allocator metrics reflect local durable state and runtime status at scrape time. Scrapes do not affect execution lifetime or renew leases.
- Client statistics via `client.Stats()` represent the in-memory perspective of the active run controller; unselected or expired announcements do not constitute an authoritative cluster view.

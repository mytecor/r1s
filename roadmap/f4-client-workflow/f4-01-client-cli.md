# F4-01 — Build client CLI

**Status:** Complete

## Outcome

Provide the `cmd/r1s/` command-line workflow for request, list, inspect, cancel, and result retrieval.

## Acceptance

- The full workflow operates against two local allocator processes.
- CLI restart does not lose durable request state or accidentally reassign a request.

## Implemented

- The client core stores its state transactionally and binds it to the authenticated client identity.
- Requests and assignments have durable message IDs and timestamps; retrying selection after restart
  returns the exact same assignment.
- Offers are accepted only from registered authenticated allocator identities and selected by hop
  count, identity, then offer ID.
- `request`, `list`, `inspect`, `cancel`, and `result` are exposed by [`cmd/r1s`](../../cmd/r1s/).
- `request` accepts one bounded Protobuf JSON positional argument; unknown fields and a
  caller-supplied `requestId` are rejected.
- Long options use the canonical `--flag` spelling in help, diagnostics, and documentation.
- `request` returns after the assignment is queued to the selected allocator; it does not wait for
  runtime start or completion. State observation remains an explicit `inspect` or `result` action.
- A fresh additive `ExecutionInspect` message retrieves the allocator's latest durable state after
  either participant restarts.
- Deterministic tests run the workflow against two allocator cores and recover a terminal exit code
  that completed while the client was not observing callbacks.

## Live verification

- On 2026-09-14, the acceptance path passed against two live `r1sd` processes and containerd 2.3.4
  on `mytecor-homelab`. The selected workload returned exit code 31, while only one durable
  assignment was created.
- The current `result` output is terminal metadata (phase, detail, and exit code). Bounded logs were
  added by [F9](../f9-local-logs/README.md), and richer application outputs are planned as artifacts
  in [F14](../f14-external-data-plane/README.md).

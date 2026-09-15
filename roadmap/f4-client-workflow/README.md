# F4 — Client workflow

**Status:** Complete

## Outcome

The `r1s` client can publish demand to `r1sd` allocators, collect offers, select one allocator,
inspect state, cancel work, and retrieve a retained result.

## Completion criteria

- CLI operations map to asynchronous protocol messages.
- Offer selection is deterministic and policy-driven.
- Client state survives local process restart.

## Tasks

- [F4-01 — Build client CLI](./f4-01-client-cli.md)

## Implementation

- [`internal/client`](../../internal/client/) persists requests, offers, allocator routes, the selected
  assignment, cancellation intent, and the latest execution state in an identity-bound snapshot.
- Offer selection is deterministic by hop count, authenticated allocator identity, and offer ID.
- [`cmd/r1s`](../../cmd/r1s/) provides `request`, `list`, `inspect`, `cancel`, and `result` commands
  over a passive RNS endpoint.
- `request` accepts one bounded, strictly decoded Protobuf JSON `ExecutionRequest` argument;
  allocator routing and offer collection timing remain explicit `--...` CLI flags.
- After sending the durable assignment, `request` returns immediately. Execution state and results
  are queried explicitly with `inspect` and `result`.
- `ExecutionInspect` was added additively to the `r1s.v1` schema so state can be recovered without
  reassigning or restarting a workload.
- A two-allocator in-memory workflow proves selection, execution, offline terminal completion, and
  later result inspection.

## Live verification

- On 2026-09-14, the CLI connected to two live `r1sd` processes over separate loopback RNS UDP
  interfaces on `mytecor-homelab`, collected both offers, selected one allocator, and returned
  `status=assignment-sent` without waiting for runtime state.
- A later client process recovered the durable execution and retrieved `completed` with the expected
  exit code. A separate long-running workload was cancelled and reported `cancelled`.
- Bounded stdout/stderr retrieval was added by [F9](../f9-local-logs/README.md); application artifact
  transfer is planned in [F14](../f14-external-data-plane/README.md).

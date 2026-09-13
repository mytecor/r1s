# Open decisions and deferred work

This file records unresolved choices so they do not remain implicit in implementation code.

## Open decisions

1. **Reticulum-Go pin and integration mode** — select a reviewed release or commit and decide whether
   r1s embeds `pkg/node` or talks to a local daemon. The intended first choice is in-process Go, but
   dependency provenance and upgrade policy must be fixed before adoption.
2. **Allocator persistence** — choose the durable store for offers, executions, replay IDs, and
   recovery metadata. SQLite is the leading local-only option.
3. **Result contract** — define inline result limits, RNS Resource transfer, checksums, retention,
   and optional external artifact references.
4. **Offer strategy** — the initial implementation reserves capacity and starts after assignment.
   Revisit whether selected workload classes benefit from speculative image pulling.
5. **Identity storage** — define secure creation, persistence, rotation, backup, and per-service
   identity boundaries.

## Deferred

- Local gRPC management API over a Unix socket.
- Resource classes beyond fixed concurrent execution slots.
- VM and microVM runtime adapters.
- Multi-owner fairness and allocator-local admission policy.
- Application-level event buses, agent hierarchy, and task decomposition.


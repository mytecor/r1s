# Open decisions and deferred work

This file records unresolved choices so they do not remain implicit in implementation code.

## Open decisions

1. **Reticulum-Go Channel upgrade** — F2 embeds the canonical Go module
   `git.quad4.io/Networks/Reticulum-Go` at `v0.9.5`. The newer GitHub mirror release contains Channel
   and node lifecycle fixes but is not published with consumable module metadata. Upgrade through the
   canonical module path before declaring reliable retransmission and reconnect acceptance complete;
   do not copy its internal dependencies into this repository.
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

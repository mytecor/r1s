# Open decisions and deferred work

This file records unresolved choices so they do not remain implicit in implementation code.

## Open decisions

1. **Reticulum-Go Channel envelope delivery to the Python reference** — RSS discovery interoperability
   against an upstream Python RNS node is implemented and proven by a gated live test
   ([`internal/transport/rns/interop_python_test.go`](../internal/transport/rns/interop_python_test.go)).
   The endpoint can also initiate a link toward such a node. What remains open is reliable envelope
   delivery over a Reticulum-Go Channel to the Python reference implementation, which should be
   closed from the canonical module path before declaring reliable retransmission and reconnect
   acceptance complete; do not copy Reticulum-Go internal dependencies into this repository.
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

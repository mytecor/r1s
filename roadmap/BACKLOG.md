# Open decisions and deferred work

This file records unresolved choices so they do not remain implicit in implementation code.

## Open decisions

1. **Allocator persistence** — choose the durable store for offers, executions, replay IDs, and
   recovery metadata. SQLite is the leading local-only option.
2. **Result contract** — define inline result limits, RNS Resource transfer, checksums, retention,
   and optional external artifact references.
3. **Offer strategy** — the initial implementation reserves capacity and starts after assignment.
   Revisit whether selected workload classes benefit from speculative image pulling.
4. **Identity storage** — define secure creation, persistence, rotation, backup, and per-service
   identity boundaries.

## Resolved

1. **Reticulum-Go Channel envelope delivery to the Python reference** — closed by
   [`internal/transport/rns/interop_python_test.go`](../internal/transport/rns/interop_python_test.go)
   (`TestPythonReferenceChannelEnvelope`): the Go endpoint discovers the upstream Python RNS node,
   initiates a link, and sends validated r1s envelopes over a Reticulum-Go Channel while a wrapped
   UDP interface injects channel-packet loss. The Python reference reassembles the envelopes, echoes
   the exact bytes back over the same Channel, and the Go endpoint re-validates them with the
   authenticated sender authority from the link identity. The wrapper is built on the canonical
   `pkg/interfaces` interception primitive (`NewUDPInterface` + `Send` override), so no Reticulum-Go
   internal dependencies are copied into this repository.

## Deferred

- Local gRPC management API over a Unix socket.
- Resource classes beyond fixed concurrent execution slots.
- VM and microVM runtime adapters.
- Multi-owner fairness and allocator-local admission policy.
- Application-level event buses, agent hierarchy, and task decomposition.

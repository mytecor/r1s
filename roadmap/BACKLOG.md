# Open decisions and deferred work

This file records unresolved choices so they do not remain implicit in implementation code.

## Open decisions

1. **Requested log and artifact transfer mechanism** — local-only stdout/stderr with explicit authenticated
   retrieval is decided ([F9](./f9-local-logs/README.md)). Still to define: whether requested transfers use
   RNS Resources or an external artifact plane, byte limits, stream offsets, truncation, and checksum contract.
2. **Speculative image preparation** — decide whether selected workload classes benefit from
   pulling or preparing an image before assignment. An offer itself remains a capacity lease and
   does not authorize workload start.
3. **Identity storage** — define secure creation, persistence, rotation, backup, and per-service
   identity boundaries.

4. **History retention and replay horizon defaults** — tombstone lifetime, replay horizon, clock
   behavior, and expired-result defaults remain to be pinned before
   [F11-01](./f11-state-retention/f11-01-retention-contract.md) can be marked complete.
5. **Admission defaults and device profiles** — define trusted-client defaults, quotas, and GPU device
   ownership in [F10](./f10-local-admission/README.md).
6. **Live regression environment** — choose an isolated Linux runner and pinned fixture/runtime
   versions for [F7](./f7-verification/README.md).
7. **Cluster membership rotation and revocation** — define an authenticated `cluster rotate`
   workflow, safe distribution of the replacement join token, transition windows for partitioned
   members, and whether individual member revocation warrants moving beyond the shared-key baseline
   established by [F12](./f12-cluster-membership/README.md).

## Resolved

1. **Allocator persistence** — bbolt provides a local, transactional, pure-Go single-file store for
   versioned allocator snapshots. It reuses a storage technology already present through the
   containerd dependency graph, avoids a CGO requirement, and keeps the allocator core independent
   through its [`StateStore`](../internal/allocator/state.go) interface.
2. **Reticulum-Go Channel envelope delivery to the Python reference** — closed by
   [`internal/transport/rns/interop_python_test.go`](../internal/transport/rns/interop_python_test.go)
   (`TestPythonReferenceChannelEnvelope`): the Go endpoint discovers the upstream Python RNS node,
   initiates a link, and sends validated r1s envelopes over a Reticulum-Go Channel while a wrapped
   UDP interface injects channel-packet loss. The Python reference reassembles the envelopes, echoes
   the exact bytes back over the same Channel, and the Go endpoint re-validates them with the
   authenticated sender authority from the link identity. The wrapper is built on the canonical
   `pkg/interfaces` interception primitive (`NewUDPInterface` + `Send` override), so no Reticulum-Go
   internal dependencies are copied into this repository.
3. **Offer commitment model** — keep offers as hard, time-bounded capacity leases and add an
   authenticated explicit release for every known unselected offer in
   [F6](./f6-offer-release/README.md). Hard offers preserve the guarantee that a selected allocator
   still has capacity when assignment arrives; explicit release avoids holding losing reservations
   until expiry. Expiry remains the fallback when a release is lost or the client disconnects.
   Soft advisory offers were rejected because concurrent clients can consume the advertised slot
   before assignment, and failover after an ambiguous assignment timeout can start the same request
   on multiple independent allocators.
4. **Command errors are explicit, bounded, and correlated** — authenticated allocator rejections
   return a correlatable `CommandError` with a stable code and retry flag in
   [F8](./f8-protocol-feedback/README.md). Rejections never carry container stdout/stderr;
   duplicate rejection is stable across restart via the replay cache. An assignment timeout remains
   ambiguous and never authorizes a second allocation.
5. **Execution state is revision-ordered** — `ExecutionState.revision` monotonically orders durable
   transitions and survives allocator and client restart in
   [F8-02](./f8-protocol-feedback/f8-02-state-revisions.md). Legacy timestamp-only states
   keep backward-compatible ordering; contradictory equal revisions are rejected.
6. **Logs stay local; retrieval is explicit, authenticated, and bounded** — stdout/stderr are kept
   in local allocator storage and are transferred only after an explicit `ExecutionLogsRequest` with
   stream, offset, and byte cap from the authenticated owner in
   [F9](./f9-local-logs/README.md). Completion, failure, inspection, result, and reconnection
   never transfer logs. Transfer mechanism and checksum contract remain an open decision.
7. **Admission and resource profiles are allocator-owned** — resource classes map to runtime-neutral
   limits enforced by the containerd adapter and are preserved across restart in
   [F10-01](./f10-local-admission/f10-01-resource-profiles.md); per-identity quotas and
   an admission policy JSON gate reservation and execution in
   [F10-02](./f10-local-admission/f10-02-identity-quotas.md). GPU device allocation and
   trusted-client defaults remain open.
8. **History retention uses tombstones behind the replay horizon** — terminal executions gain a
   bounded `retain_until`, expired results are explicit, and cleanup sweeps offers, executions, and
   tombstones transactionally in [F11-01](./f11-state-retention/f11-01-retention-contract.md),
   so an old assignment replayed after cleanup cannot start another workload.
9. **Cluster membership uses one shared secret without a control plane** — `cluster init` creates a
   random 256-bit key, announces expose only its domain-separated public ID, and authenticated RNS
   peers mutually prove key possession before any control envelope is delivered in
   [F12](./f12-cluster-membership/README.md). Per-identity admission remains allocator-local.

## Deferred

- Local gRPC management API over a Unix socket.
- VM and microVM runtime adapters.
- Broader multi-client fairness policy (admission and identity quotas are in [F10](./f10-local-admission/README.md)).
- Application-level event buses, agent hierarchy, and task decomposition.

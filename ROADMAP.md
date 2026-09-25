# r1s roadmap

The high-level plan and implementation detail live at different depths, so the project can be read
at the level it is needed. Each feature is a separate vertical with its own directory
`roadmap/<feature-id>-<feature-slug>/` containing the feature file (`README.md`) and its task files.
Templates for new features and tasks live in [TEMPLATE_FEATURE.md](./roadmap/TEMPLATE_FEATURE.md)
and [TEMPLATE_TASK.md](./roadmap/TEMPLATE_TASK.md).

Feature numbers are stable identifiers, not an instruction to complete every row top to bottom.
Open decisions and deferred work live in [BACKLOG.md](./roadmap/BACKLOG.md).

## [F1. Protocol foundation](./roadmap/f1-protocol-foundation/README.md)

> A transport-independent Go core that validates control messages, reserves allocator capacity, and
> exercises delivery and transitions without a live RNS network.

- **Status:** ✅ complete
- **Done when:** versioned messages, allocator transitions, and adapter contracts pass deterministic
  tests.
- **Depends on:** —

## [F2. RNS transport](./roadmap/f2-rns-transport/README.md)

> Two r1s processes discover allocator capacity and exchange authenticated Protobuf envelopes through
> Reticulum-Go without tunnelling gRPC or HTTP/2 over RNS.

- **Status:** ✅ complete
- **Done when:** two Go processes discover and exchange authenticated envelopes through
  Reticulum-Go.
- **Depends on:** [F1](#f1-protocol-foundation)

## [F3. OCI runtime](./roadmap/f3-oci-runtime/README.md)

> Selected executions run through containerd with durable metadata and restart reconciliation.

- **Status:** ✅ complete
- **Done when:** an assigned workload starts and stops through containerd with reconciliation after
  restart.
- **Depends on:** [F1](#f1-protocol-foundation)

## [F4. Client workflow](./roadmap/f4-client-workflow/README.md)

> A client publishes demand, collects offers, selects one allocator, inspects state, cancels work, and
> retrieves a retained result.

- **Status:** ✅ complete
- **Done when:** a CLI can request, select, inspect, cancel, and retrieve a result.
- **Depends on:** [F2](#f2-rns-transport), [F3](#f3-oci-runtime)

## [F5. Partition recovery](./roadmap/f5-partition-recovery/README.md)

> The complete system tolerates duplicate messages, process restarts, and temporary loss of client
> connectivity without duplicate execution or premature termination.

- **Status:** ✅ complete
- **Done when:** end-to-end tests prove replay safety, network-partition survival, and result
  recovery.
- **Depends on:** [F2](#f2-rns-transport), [F3](#f3-oci-runtime), [F4](#f4-client-workflow)

## [F6. Efficient offer release](./roadmap/f6-offer-release/README.md)

> The client explicitly releases every known unselected hard offer while offer expiry remains the
> partition-safe fallback.

- **Status:** 🚧 implemented; live Linux partition-recovery rerun passed on `mytecor-homelab` (2026-09-15)
- **Done when:** losing allocators release reserved capacity promptly without weakening assignment
  durability, authenticated authority, or duplicate-delivery safety.
- **Depends on:** [F1](#f1-protocol-foundation), [F2](#f2-rns-transport),
  [F4](#f4-client-workflow), [F5](#f5-partition-recovery)

## [F7. Continuous verification](./roadmap/f7-verification/README.md)

> Every pull request runs the same generated-code, race, and documentation checks as local development, and a
> repeatable Linux run verifies Python RNS interoperability and real containerd partition recovery.

- **Status:** ✅ complete; F7-01 CI parity green on GitHub Actions and F7-02 live regression documented,
  with the full live suite verified on `mytecor-homelab` (2026-09-15)
- **Done when:** the linked tasks pass their acceptance checks.
- **Depends on:** [F1](#f1-protocol-foundation), [F2](#f2-rns-transport), [F3](#f3-oci-runtime), [F5](#f5-partition-recovery).

## [F8. Protocol feedback and state revisions](./roadmap/f8-protocol-feedback/README.md)

> Clients distinguish authenticated allocator rejection from missing responses, and execution state ordering
> survives clock rollback and process restart.

- **Status:** ✅ complete; unit, protocol, and live Linux rejection acceptance verified on `mytecor-homelab` (2026-09-15)
- **Done when:** the linked tasks pass their acceptance checks.
- **Depends on:** [F1](#f1-protocol-foundation), [F2](#f2-rns-transport), [F4](#f4-client-workflow), [F5](#f5-partition-recovery).

## [F9. Local logs and explicit retrieval](./roadmap/f9-local-logs/README.md)

> The allocator retains bounded stdout/stderr locally, including after a task fails or the daemon restarts, and
> an execution owner can separately request a bounded range of locally retained logs.

- **Status:** ✅ complete; live Linux log retention and owner retrieval verified on `mytecor-homelab` (2026-09-15)
- **Done when:** the linked tasks pass their acceptance checks.
- **Depends on:** [F3](#f3-oci-runtime), [F4](#f4-client-workflow), [F8](#f8-protocol-feedback-and-state-revisions), and the [F11](#f11-bounded-durable-history) retention contract.

## [F10. Local admission and resource limits](./roadmap/f10-local-admission/README.md)

> Allocator-defined resource classes enforce CPU, memory, and process limits, and an allocator controls which
> clients can reserve capacity and how much they can reserve.

- **Status:** ✅ complete; admission acceptance tests and live Linux enforcement verified on `mytecor-homelab` (2026-09-15)
- **Done when:** the linked tasks pass their acceptance checks.
- **Depends on:** [F3](#f3-oci-runtime), [F8](#f8-protocol-feedback-and-state-revisions).

## [F11. Bounded durable history](./roadmap/f11-state-retention/README.md)

> Finished executions and obsolete offers are collected without allowing old commands to restart work, and
> persistence remains predictable as execution history grows.

- **Status:** ✅ complete; retention acceptance, storage benchmarks, and live crash injection verified; F11-02 measured on 2026-09-15
- **Done when:** the linked tasks pass their acceptance checks.
- **Depends on:** [F5](#f5-partition-recovery), [F6](#f6-efficient-offer-release), [F8](#f8-protocol-feedback-and-state-revisions).

## [F12. Shared-secret cluster membership](./roadmap/f12-cluster-membership/README.md)

> Participants bootstrap with one secret join token while announces expose only a derived public cluster ID, and
> RNS links mutually prove cluster membership before any control envelope reaches a client or allocator.

- **Status:** ✅ complete; Go loopback and Python-reference interoperability verified on 2026-09-15
- **Done when:** foreign-cluster discovery and control traffic are rejected without introducing a
  central authority.
- **Depends on:** [F2](#f2-rns-transport).

## [F13. Local client API](./roadmap/f13-local-client-api/README.md)

> A persistent, identity-scoped local service exposes the client workflow to applications over a
> Unix socket and streams execution-state changes without creating a cluster-wide API server.

- **Status:** ✅ complete; socket-contract and service-backed CLI acceptance tests pass
- **Done when:** applications can request, inspect, cancel, list, retrieve results or logs, and watch
  executions through the local API while the existing CLI remains usable.
- **Depends on:** [F4](#f4-client-workflow), [F5](#f5-partition-recovery),
  [F9](#f9-local-logs-and-explicit-retrieval).

## [F14. Direct node access (r1s-tunneld)](./roadmap/f14-direct-node-access/README.md)

> An execution owner opens an authenticated tunnel from their client to a running execution over
> Yggdrasil and carries arbitrary traffic inside it, independent of any artifact model.

- **Status:** ✅ done — F14-01 (execution-scoped access grants) and F14-02 (tunnel edge and the
  `r1s tunnel` command) are implemented: the transport-neutral tunnel stream contract, in-memory
  fake, node-key derivation, `LocalTunnel` bidi RPC, the serve-side relay, the client grant mint,
  the service-backed `r1s tunnel` command, the Yggdrasil stream-adaptation layer (framing, packet
  mux, stream adapter over the embedded `Core`), the `r1sd` edge splice loop, and the live mesh
  test (`make check` passes; the live mesh test peers two embedded nodes over a local link and
  round-trips a multi-packet payload).
- **Done when:** a client tunnels to a running execution through Yggdrasil, only the authenticated
  execution owner can open or keep the tunnel, and the core and protocol stay free of
  Yggdrasil-specific types while carrying no tunnel bytes over RNS. `r1s tunnel` is service-backed
  only over a bidi `LocalTunnel` stream; edge node keys are HKDF-derived from persistent identities
  (no separate key files).
- **Depends on:** [F2](#f2-rns-transport), [F12](#f12-shared-secret-cluster-membership),
  [F13](#f13-local-client-api).

## [F15. Deployment reconciliation](./roadmap/f15-deployment-reconciliation/README.md)

> Durable client-owned desired state landed through the F17 keep-alive intent instead of a
> manifest-driven deploy reconciler.

- **Status:** ✅ closed 2026-09-16 via [F17](#f17-execution-lease): the durable single-execution
  part is the lease-holding intent recorded by `request --keep-alive`, renewed by the shared client
  engine, and converted into a re-request of the recorded workload on lease loss. The manifest
  layer — `r1s deploy apply/status`, named multi-deployment state, revision hashes,
  create-before-destroy replacement, removal semantics — is deferred; see
  [BACKLOG.md](./roadmap/BACKLOG.md).
- **Depends on:** [F4](#f4-client-workflow), [F5](#f5-partition-recovery),
  [F13](#f13-local-client-api), [F17](#f17-execution-lease).

## [F16. Node capabilities and placement](./roadmap/f16-node-placement/README.md)

> Allocators advertise bounded OS, architecture, runtime, device, and operator labels so clients can
> request compatible nodes and still make the final placement decision from returned offers.

- **Status:** ✅ complete; allocator capabilities and client constraints verified under
  `go test -race` (protocol, allocator, client, socket-contract, RNS descriptor)
- **Done when:** incompatible allocators do not offer, compatible offers expose useful placement
  metadata, and selection remains client-owned without a global scheduler.
- **Depends on:** [F4](#f4-client-workflow), [F10](#f10-local-admission-and-resource-limits),
  [F12](#f12-shared-secret-cluster-membership).

## [F17. Execution lease](./roadmap/f17-execution-lease/README.md)

> A client keeps its execution alive with an authenticated, durably persisted, renewable lease;
> a permanently gone client's execution is evicted locally after lease expiry.

- **Status:** ✅ complete; deterministic allocator, client, protocol, socket-contract, and
  service-backed CLI tests pass under `go test -race`
- **Done when:** an unrenewed lease leads to local eviction with terminal state distinguishable
  from cancellation, renewal is owner-only and replay-safe, allocator restart honors persisted
  leases, and no transport connection state ever determines execution lifetime.
- **Depends on:** [F2](#f2-rns-transport), [F3](#f3-oci-runtime),
  [F13](#f13-local-client-api).

## [F18. Observability](./roadmap/f18-observability/README.md)

> Local structured logs, metrics, and client inspection commands expose allocator and execution
> health without introducing global desired state or bundling an observability backend.

- **Status:** 🚧 in progress — F22-01 complete
- **Done when:** operators can inspect discovered allocators and executions and scrape documented
  allocator-local metrics without receiving workload stdout/stderr implicitly.
- **Depends on:** [F13](#f13-local-client-api).

## [F19. Universal tunnel rework](./roadmap/f19-tunnel-rework/README.md)

> The F14 direct-access tunnel becomes a general-purpose, addressable, multiplexed stream transport
> behind a single `r1s tunnel` command (ngrok-style): the same command that binds local port mappings
> can run several concurrent protocols (SSH + HTTP + API) to one execution over one authenticated mesh
> connection. The direction stays client→allocator — the client connects to the container, never the
> other way around; there is no reverse/listen/publish surface.

- **Status:** ✅ landed (2026-09)
- **Done when:** `r1s tunnel <execution-id> --port <host>:<container>` is the only user-facing tunnel
  command and exposes the execution as many addressable logical streams (one per inbound connection,
  always client→allocator) over one authenticated pair of node keys; destinations are the
  client-supplied container ports, and the
  core/protocol stay transport-neutral and free of Yggdrasil-specific types.
- **Depends on:** [F14](#f14-direct-node-access-r1s-tunneld), [F13](#f13-local-client-api), [F17](#f17-execution-lease).

## [F20. Client-managed tunnel targets](./roadmap/f20-client-tunnel-targets/README.md)

> The tunnel's destinations stop being hard-coded on the allocator. The `r1s` client owns the
> destination ports and passes them in the tunnel grant request; the allocator only
> proxies/splices to grant-carried ports and keeps its authorization role.

- **Status:** ✅ complete — `r1s tunnel <id> --port <host>:<container>` binds a local listener on
  `127.0.0.1:<host>` and relays inbound connections to the container port `<container>` over the
  tunnel; `r1sd` carries no target configuration (`--tunnel-target` and
  `--tunnel-default-target` removed); a stream-open to a port not in the client-supplied list is
  rejected `ReasonUnauthorized` before any payload byte.
- **Depends on:** [F19](#f19-universal-tunnel-rework), [F14](#f14-direct-node-access-r1s-tunneld).

## [F21. Tunnel data plane over system Yggdrasil + private RNS](./roadmap/f21-tunnel-rns-dataplane/README.md)

> F21 evaluated replacing the embedded Ygg tunnel with a private Python-compatible RNS
> `Link`/`Channel`/`Buffer` data plane over system Ygg. The recorded benchmark rejected that path:
> after compression and readiness-poll workarounds, RNS remains 26.7–39.9× slower in the relevant
> rows because of its 423-byte stream payload, bounded Channel window, per-packet signed proofs,
> IFAC work, and small Backbone writes. RNS remains the control plane; Ygg remains the application
> data plane.

- **Status:** 🔄 no-go recorded; F21-06 rollback planned
- **Done when:** the experimental private-RNS tunnel package and its unused Open/advertisement
  surface are removed, while the F19/F20 embedded-Ygg adapter, grants, multiplexed streams,
  user-facing tunnel UX, owner authorization, and container namespace isolation remain intact;
  `go build ./...`, `go vet ./...`, `go test -race ./...` and `make check` pass.
- **Depends on:** [F19](#f19-universal-tunnel-rework),
  [F20](#f20-client-managed-tunnel-targets), [F17](#f17-execution-lease),
  [F13](#f13-local-client-api), [F12](#f12-shared-secret-cluster-membership).

## [F22. Shared-instance RNS and run-oriented client](./roadmap/f22-rns-shared-instance/README.md)

> F22 replaces the accumulated client-side control plane with one lease-owning
> `r1s run <cluster> <workload>` process. It connects to a required shared RNS daemon, uses an
> ephemeral identity, correlates rescheduled execution attempts with a stable run ID, tails logs,
> and owns published ports. Client persistence and the local command service disappear; allocator
> persistence remains. The switch is intentionally incompatible: old client state and CLI surfaces
> are not migrated or supported.

- **Status:** 🚧 in progress — F22-01 through F22-06 complete
- **Done when:** `r1s run` owns discovery, deterministic placement, leases, rescheduling, log
  tailing, and tunnels across stable `run_id`/monotonic attempts; detached runs retain output;
  cluster choice is explicit; production transport requires a shared RNS instance; legacy client
  commands, state, service, and socket are gone; allocator recovery remains intact; and all standard
  verification passes.
- **Depends on:** [F2](#f2-rns-transport), [F9](#f9-local-logs-and-explicit-retrieval),
  [F12](#f12-shared-secret-cluster-membership), [F16](#f16-node-capabilities-and-placement),
  [F17](#f17-execution-lease), [F19](#f19-universal-tunnel-rework),
  [F20](#f20-client-managed-tunnel-targets), and F21-06.

## Current implementation order

- [F17](#f17-execution-lease) is complete: execution lifetime is bounded by a durably persisted,
  explicitly renewed client-held lease instead of a request-time deadline, and `r1s serve` renews
  the durable lease-holding intents recorded by `r1s request --keep-alive`.
- [F15](#f15-deployment-reconciliation) was closed on 2026-09-16 without building the manifest
  layer: the durable desired state for one execution already exists as the keep-alive intent from
  [F17](#f17-execution-lease) — renewed by the client engine, re-requested with its allocator
  pinning after lease loss. A manifest-driven `r1s deploy` (named multi-deployment state, revision
  diffing, create-before-destroy replacement, removal) is deferred in
  [BACKLOG.md](./roadmap/BACKLOG.md).
- [F14](#f14-direct-node-access-r1s-tunneld) landed (2026-09): `r1s tunnel` with the
  allocator-side edge inside `r1sd` gives the authenticated execution owner a direct Yggdrasil
  tunnel to a running execution over an embedded yggdrasil-go node, independent of artifact
  transfer, without Yggdrasil-specific protocol types or a new global state source.
- [F19](#f19-universal-tunnel-rework) landed (2026-09): the F14 single interactive byte pipe is
  reworked into a multiplexed, addressable stream transport — several streams, always
  client→allocator, over one authenticated mesh connection. `r1s tunnel <id> --port <host>:<port>`
  binds a local listener and relays to the container port; the interactive pipe is removed and a
  tunnel requires at least one `--port`. The allocator-side slot resolution was reworked as
  [F20](#f20-client-managed-tunnel-targets) so the client, not `r1sd`, owns the destination ports.
  See [F19-01](./roadmap/f19-tunnel-rework/f19-01-universal-tunnel.md).
- [F20](#f20-client-managed-tunnel-targets) landed: the destination source of truth moved from
  allocator config to the `r1s` client — `r1s tunnel <id> --port <host>:<container>` binds a local
  listener and sends the container port in the grant, `r1sd` drops `--tunnel-target` /
  `--tunnel-default-target` and becomes a proxy/splice point to grant-carried ports.
- [F21](#f21-tunnel-data-plane-over-system-yggdrasil--private-rns) reached its benchmark decision:
  the private RNS tunnel is a no-go, the embedded Ygg adapter remains selected, and F21-06 rolls
  back the experimental RNS package and unused Open/advertisement slice. The benchmark and upstream
  Reticulum-Go findings remain recorded; they no longer gate tunnel delivery.
- [F16](#f16-node-capabilities-and-placement) landed (2026): allocators advertise bounded
  OS/arch/runtime/device/resource-profile/label capabilities in offers and a compact RNS announce
  summary; clients express exact-match `--constraints`, and only compatible allocators receive the
  request. Placement never overrides allocator-local admission: incompatible requests are rejected
  before capacity is reserved, and assignment re-validation emits an explicit `INCOMPATIBLE`
  rather than an invalid start.
- [F18](#f18-observability) adds standard local export points and inspection surfaces after the
  client and API surfaces exist.
- [F22](#f22-shared-instance-rns-and-run-oriented-client) is in progress: F22-01 makes the shared
  RNS daemon mandatory, removes `--rns-config`, and fails closed without taking ownership of the
  shared listener. F22-02 adds stable run identity, monotonic attempts, runtime fencing metadata,
  and explicit at-least-once rescheduling semantics. F22-03 adds the multi-cluster credential
  directory, non-secret listing, unique-prefix selection, and the allocator's required cluster
  operand. F22-04 adds the ephemeral `run` process, deterministic unpinned placement, lease
  ownership, authenticated inspection recovery, conclusive-loss rescheduling, best-effort signal
  cancellation, workload-derived exit status, foreground byte-offset log tailing, and detached runs
  that record PID and output under `~/.local/state/r1s/runs/<run-id>/` with a `-d` ownership
  handshake. F22-05 makes foreground and detached run output continuous (bounded retained logs with
  byte-offset tailing and resumed appends after reschedule). F22-06 moves the tunnel surface into
  repeatable `r1s run [-p host:container]`, replacing the mint/grant flow with an additive
  owner-authenticated open handshake: the allocator binds the authenticated run owner's edge key and
  container ports to the execution for its lifetime (no grant ID/TTL/single-use), and a run
  process's loopback listeners stay bound across reschedules while each new stream is authenticated
  and spliced to the currently active execution. The remaining task removes the legacy client DB
  and local API.

The remaining live Linux legs F8–F11 were completed on `mytecor-homelab` on 2026-09-15
(containerd 2.3.4 / runc 1.4.3 / Go 1.26.7 / digest-pinned Alpine fixture) and the whole live
acceptance suite passes together under `go test -race`:

- [F8](#f8-protocol-feedback-and-state-revisions) — `TestLiveRejectionUnderLossNoDuplicateExecution`
  proves a rejection in flight never becomes an execution.
- [F9](#f9-local-logs-and-explicit-retrieval) — the transport-spy zero-log-bytes leg is inherently a
  wire-level deterministic check and lives in the unit acceptance
  (`TestLogsOnlyByExplicitOwnerRequest`); the live restart leg was already verified.
- [F10](#f10-local-admission-and-resource-limits) — `TestLiveResourceLimitsEnforced` (OCI-spec
  memory/CPU/pids limits on a real container, kept across restart) and
  `TestLiveIdentityQuotaEnforced` (per-identity quota, repeated cleanup, restart consistency).
- [F11](#f11-bounded-durable-history) — `TestLiveSweepCrashPreservesCapacityAndAuthority` (SIGKILL
  during the `Sweep` window; collected assignment stays `EXPIRED`, capacity freed).

F8, F9, F10, and F11 are therefore complete.

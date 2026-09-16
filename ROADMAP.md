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

## [F16. Node capabilities and placement](./roadmap/f16-node-placement/README.md)

> Allocators advertise bounded OS, architecture, runtime, device, and operator labels so clients can
> request compatible nodes and still make the final placement decision from returned offers.

- **Status:** ⏳ planned
- **Done when:** incompatible allocators do not offer, compatible offers expose useful placement
  metadata, and selection remains client-owned without a global scheduler.
- **Depends on:** [F4](#f4-client-workflow), [F10](#f10-local-admission-and-resource-limits),
  [F12](#f12-shared-secret-cluster-membership).

## [F18. Observability](./roadmap/f18-observability/README.md)

> Local structured logs, metrics, and client inspection commands expose allocator and execution
> health without introducing global desired state or bundling an observability backend.

- **Status:** ⏳ planned
- **Done when:** operators can inspect discovered allocators and executions and scrape documented
  allocator-local metrics without receiving workload stdout/stderr implicitly.
- **Depends on:** [F13](#f13-local-client-api).

## Current implementation order

- [F13](#f13-local-client-api) is complete: `r1s serve` plus `--socket` give applications a persistent
  local frontend and a `Watch` stream while preserving client-owned placement and identity.
- [F16](#f16-node-capabilities-and-placement) is the next vertical: allocators advertise bounded
  capability labels, clients express exact-match constraints, and only compatible allocators offer.
  It follows the local API independently and does not introduce a scheduler or global state.
- [F18](#f18-observability) adds standard local export points and inspection surfaces after the
  client and API surfaces exist.

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

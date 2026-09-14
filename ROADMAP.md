# r1s roadmap

The high-level plan and implementation detail live at different depths, so the project can be read
at the level it is needed. Each feature is a separate vertical with its own directory
`roadmap/<feature-id>-<feature-slug>/` containing the feature file (`README.md`) and its task files.
Templates for new features and tasks live in [TEMPLATE_FEATURE.md](./roadmap/TEMPLATE_FEATURE.md)
and [TEMPLATE_TASK.md](./roadmap/TEMPLATE_TASK.md).

Feature numbers are stable identifiers, not an instruction to complete every row top to bottom.
Open decisions and deferred work live in [BACKLOG.md](./roadmap/BACKLOG.md).

## [F1. Protocol foundation](./roadmap/f1-protocol-foundation/README.md)

A transport-independent Go core that validates control messages, reserves allocator capacity, and
exercises delivery and transitions without a live RNS network.

- **Status:** ✅ complete
- **Done when:** versioned messages, allocator transitions, and adapter contracts pass deterministic
  tests.
- **Depends on:** —

## [F2. RNS transport](./roadmap/f2-rns-transport/README.md)

Two r1s processes discover allocator capacity and exchange authenticated Protobuf envelopes through
Reticulum-Go without tunnelling gRPC or HTTP/2 over RNS.

- **Status:** ✅ complete
- **Done when:** two Go processes discover and exchange authenticated envelopes through
  Reticulum-Go.
- **Depends on:** [F1](#f1-protocol-foundation)

## [F3. OCI runtime](./roadmap/f3-oci-runtime/README.md)

Selected executions run through containerd with durable metadata and restart reconciliation.

- **Status:** ✅ complete
- **Done when:** an assigned workload starts and stops through containerd with reconciliation after
  restart.
- **Depends on:** [F1](#f1-protocol-foundation)

## [F4. Client workflow](./roadmap/f4-client-workflow/README.md)

A client publishes demand, collects offers, selects one allocator, inspects state, cancels work, and
retrieves a retained result.

- **Status:** ✅ complete
- **Done when:** a CLI can request, select, inspect, cancel, and retrieve a result.
- **Depends on:** [F2](#f2-rns-transport), [F3](#f3-oci-runtime)

## [F5. Partition recovery](./roadmap/f5-partition-recovery/README.md)

The complete system tolerates duplicate messages, process restarts, and temporary loss of client
connectivity without duplicate execution or premature termination.

- **Status:** ✅ complete
- **Done when:** end-to-end tests prove replay safety, network-partition survival, and result
  recovery.
- **Depends on:** [F2](#f2-rns-transport), [F3](#f3-oci-runtime), [F4](#f4-client-workflow)

## [F6. Efficient offer release](./roadmap/f6-offer-release/README.md)

The client explicitly releases every known unselected hard offer while offer expiry remains the
partition-safe fallback.

- **Status:** 🚧 implemented; live Linux acceptance rerun pending
- **Done when:** losing allocators release reserved capacity promptly without weakening assignment
  durability, authenticated authority, or duplicate-delivery safety.
- **Depends on:** [F1](#f1-protocol-foundation), [F2](#f2-rns-transport),
  [F4](#f4-client-workflow), [F5](#f5-partition-recovery)

## [F7. Continuous verification](./roadmap/f7-verification/README.md)

- Every pull request runs the same generated-code, race, and documentation checks as local development.
- A repeatable Linux job verifies Python RNS interoperability and real containerd partition recovery.

- **Status:** 🚧 in progress; CI parity implemented, first GitHub Actions run pending
- **Done when:** the linked tasks pass their acceptance checks.
- **Depends on:** F1, F2, F3, F5.

## [F8. Protocol feedback and state revisions](./roadmap/f8-protocol-feedback/README.md)

- Clients distinguish authenticated allocator rejection from missing responses.
- Execution state ordering survives clock rollback and process restart.

- **Status:** 🚧 implemented; direct acceptance tests pending
- **Done when:** the linked tasks pass their acceptance checks.
- **Depends on:** F1, F2, F4, F5.

## [F9. Local logs and explicit retrieval](./roadmap/f9-local-logs/README.md)

- The allocator retains bounded stdout/stderr locally, including after a task fails or the daemon restarts.
- An execution owner can separately request a bounded range of locally retained logs.

- **Status:** 🚧 implemented; direct acceptance tests and live Linux restart pending
- **Done when:** the linked tasks pass their acceptance checks.
- **Depends on:** F3, F4, F8, and the F11 retention contract.

## [F10. Local admission and resource limits](./roadmap/f10-local-admission/README.md)

- Allocator-defined resource classes enforce CPU, memory, and process limits.
- An allocator controls which clients can reserve capacity and how much they can reserve.

- **Status:** 🚧 implemented; acceptance tests and live Linux enforcement pending
- **Done when:** the linked tasks pass their acceptance checks.
- **Depends on:** F3, F8.

## [F11. Bounded durable history](./roadmap/f11-state-retention/README.md)

- Finished executions and obsolete offers are collected without allowing old commands to restart work.
- Persistence remains predictable as execution history grows.

- **Status:** 🚧 in progress; retention implemented, scaling benchmarks pending
- **Done when:** the linked tasks pass their acceptance checks.
- **Depends on:** F5, F6, F8.

## Current implementation order

Re-run live acceptance for [F6](./roadmap/f6-offer-release/README.md) and dispatch the CI verification
in [F7](./roadmap/f7-verification/README.md), then add the acceptance tests that F8–F11 still need:
explicit command-error and revision checks in [F8](./roadmap/f8-protocol-feedback/README.md), the
zero-bytes transport spy and cross-restart retrieval in [F9](./roadmap/f9-local-logs/README.md),
identity-quota and live resource enforcement in [F10](./roadmap/f10-local-admission/README.md), and the
storage benchmarks in [F11](./roadmap/f11-state-retention/README.md). Until those finish, F8–F11 stay
in progress rather than done.

# F10-01 — Allocator resource profiles

**Status:** ✅ Implemented; admission acceptance tests and live Linux enforcement pass

## Outcome

Allocator-defined resource classes enforce CPU, memory, and process limits.

## Scope

- Map local class profiles through runtime-neutral resource limits into the containerd adapter.
- Validate profiles before advertising capacity and preserve execution profiles across restart.
- The client cannot override local limits; define whether GPU classes need explicit device allocation.

## Acceptance

- A live Linux workload is subject to its configured memory, CPU, and process limits.
- Invalid profiles fail startup; existing executions retain their admitted limits after recovery.
- Run `make check` and the feature-specific checks described above.

## Verification status

Covered by `internal/allocator/acceptance_test.go` (`TestInvalidResourceProfileFailsStartup`,
`TestResourceProfileSurvivesRestart`, plus `internal/runtime/resources.go` validation used by the
adapter):

- Invalid profiles fail startup before capacity is advertised.
- An admitted resource profile is persisted with the offer and execution and restored across restart.

Live verification (2026-09-15, `mytecor-homelab`, containerd 2.3.4 / runc 1.4.3 / Go 1.26.7) is covered by
`internal/acceptance/live_limits_test.go` (`TestLiveResourceLimitsEnforced`): a workload admitted
through a bounded 64 MiB / 500 milliCPU / 16-pid profile carries exactly those limits in the running
container's OCI spec (read back with the containerd observer), and after `r1sd` restarts from the
same state and log directory, the recovered running execution still carries the same admitted limits.
The OCI spec is the kernel-enforced cgroup contract runc applies, so this proves a live workload is
subject to its configured memory, CPU, and process limits.


## Notes

Track unresolved cross-feature decisions in [BACKLOG.md](../BACKLOG.md).

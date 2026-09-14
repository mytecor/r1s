# F7-02 — Regular live regression

**Status:** ⏳ Planned

## Outcome

A repeatable Linux job verifies Python RNS interoperability and real containerd partition recovery.

## Scope

- Provide an isolated Linux runner, digest-pinned fixture, and pinned Python RNS environment.
- Run the existing gated lifecycle, recovery, Python Channel, and partition harnesses.
- Retain diagnostic metadata; do not introduce automatic workload-log transfer over RNS.

## Acceptance

- All live gates run without skips on the configured runner.
- Failures retain enough diagnostics to reproduce the scenario.
- Run `make check` and the feature-specific checks described above.

## Notes

Track unresolved cross-feature decisions in [BACKLOG.md](../BACKLOG.md).

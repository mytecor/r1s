# F7-01 — CI verification parity

**Status:** ✅ Complete; GitHub Actions execution verified (2026-09-15)

## Outcome

Every pull request runs the same generated-code, race, and documentation checks as local development.

## Scope

- Install pinned protoc and lychee versions in CI.
- Run make check alongside formatting, go vet, and release-platform compilation.

## Acceptance

- make check passes locally.
- The workflow invokes the complete local check target and preserves cross-platform builds.
- Run `make check` and the feature-specific checks described above.

## Notes

Track unresolved cross-feature decisions in [BACKLOG.md](../BACKLOG.md).

## Implementation

[ci.yml](../../.github/workflows/ci.yml) installs checksum-pinned protoc 36.0 and lychee 0.24.2,
then runs `make check`. Formatting, `go vet`, and all release-platform builds remain enabled.

## Verification

- `make check` passes locally (protoc 36.0, lychee 0.24.2, go1.27.1) — 145 links OK, 0 errors.
- `ci` workflow green on every push to `main` since the pipeline landed (2026-09-15), and
  re-confirmed via a fresh `workflow_dispatch` run (34937777544): `full verification` and
  `cross-compile` jobs both pass.

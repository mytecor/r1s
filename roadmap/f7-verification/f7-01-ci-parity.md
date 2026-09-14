# F7-01 — CI verification parity

**Status:** ✅ Implemented; first GitHub Actions execution pending

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
The workflow has not been dispatched from this local change.

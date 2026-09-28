# F24-10 — Establish an independent Go module boundary

**Status:** ✅ Complete

## Outcome

`github.com/mytecor/meshbus` is an independently buildable and published Go module. r1s consumes
its tagged release as an ordinary external dependency.

## Scope

- Add a meshbus `go.mod`, `go.sum`, README, license, and verification targets.
- Change imports to the standalone `github.com/mytecor/meshbus` module path.
- Make the r1s module require meshbus, use a local `replace` only while both live in this repository,
  then remove it after publication.
- Run formatting, vet, tests, and race tests for both modules in local and CI verification.

## Acceptance

- `go test ./...` succeeds from inside `meshbus/` without access to r1s packages.
- r1s builds only through the public standalone meshbus import paths.
- Each repository verifies its own module in CI.
- Moving `meshbus/` to its own repository requires no source-code rewrite.

## Implementation notes

[`github.com/mytecor/meshbus`](https://github.com/mytecor/meshbus) has its own dependency lock,
README, license, Makefile, CI, and `v0.1.0` tag. r1s imports the tagged module without a local
replacement.

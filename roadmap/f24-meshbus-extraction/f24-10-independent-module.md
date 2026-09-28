# F24-10 — Establish an independent Go module boundary

**Status:** ✅ Complete

## Outcome

`meshbus/` is an independently buildable Go module using its future standalone import path. r1s
consumes it as an external dependency through a temporary local replacement.

## Scope

- Add a meshbus `go.mod`, `go.sum`, README, license, and verification targets.
- Change imports to the standalone `github.com/mytecor/meshbus` module path.
- Make the r1s module require meshbus and use a local `replace` only while both live in this repository.
- Run formatting, vet, tests, and race tests for both modules in local and CI verification.

## Acceptance

- `go test ./...` succeeds from inside `meshbus/` without access to r1s packages.
- r1s builds only through the public standalone meshbus import paths.
- The root verification command checks both modules.
- Moving `meshbus/` to its own repository requires no source-code rewrite.

## Implementation notes

`meshbus/` is the independent `github.com/mytecor/meshbus` module with its own dependency lock,
README, license, Makefile, and verification. r1s imports only that module path and temporarily uses
`replace github.com/mytecor/meshbus => ./meshbus` until repository publication.

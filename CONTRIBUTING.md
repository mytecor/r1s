# Contributing to r1s

## Development workflow

1. Read [ARCHITECTURE.md](./ARCHITECTURE.md) and the relevant feature task.
2. Keep changes within the existing protocol, transport, allocator, and runtime boundaries.
3. Add contract-focused tests when implementation begins.
4. Run `make check` before submitting changes.
5. Update the affected documentation and roadmap task status in the same change.

## Protocol changes

The initial schema is planned in
[F1-01](./roadmap/f1-protocol-foundation/f1-01-control-protocol.md). Once it exists, never
renumber or reuse an existing Protobuf field. Reserve removed numbers and names. Breaking changes
require a new package version such as `r1s.v2`.

## Go code

- Keep transport and runtime implementations behind explicit internal interfaces.
- Propagate `context.Context` through network and runtime operations.
- Make handlers safe under duplicate delivery and concurrent callbacks.
- Prefer explicit errors that callers can classify with `errors.Is`.
- Run `gofmt`; generated files must not be edited manually once code generation is introduced.

## Commands

- Put every shipped executable in `cmd/<binary>/` using the standard Go project layout.
- Keep `main` packages limited to configuration, dependency wiring, process lifecycle, and output.
- Put reusable behavior in the appropriate non-command package and test it there.
- The planned system binaries are `cmd/r1sd/` for the allocator service and `cmd/r1s/` for the client CLI.

## Tests

Tests should verify protocol validation, authority boundaries, idempotency, capacity accounting,
state transitions, cancellation, deadlines, and adapter contracts. Avoid assertions that merely
duplicate configuration constants without checking behavior.

## Live verification

Deterministic checks (`make check`, `go vet`, cross-platform builds) run automatically in GitHub Actions
([F7-01](./roadmap/f7-verification/f7-01-ci-parity.md)). Live verification against a real containerd host
runs manually on a Linux development host and is recorded in the roadmap — it is deliberately not a GitHub
Actions job. Before considering container-runtime or live-acceptance changes complete, run the documented
procedure in [F7-02](./roadmap/f7-verification/f7-02-live-regression.md): the containerd adapter gates,
the Python-reference interoperability gates, the full `./internal/acceptance/` live suite with
`RUN_PARTITION_RECOVERY=1`, then `make check`. A run that skips a live gate (a skipped `RUN_*` test) is not
a pass.

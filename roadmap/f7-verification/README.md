# F7 — Continuous verification

Corresponds to the [F7 milestone](../../ROADMAP.md#f7-continuous-verification).

**Status:** 🚧 In progress — CI parity implemented; live regression planned

## Outcome

- Every pull request runs the same generated-code, race, and documentation checks as local development.
- A repeatable Linux job verifies Python RNS interoperability and real containerd partition recovery.

## Dependencies

F1, F2, F3, F5. See [ROADMAP.md](../../ROADMAP.md).

## Scope and completion criteria

Complete the behavior and acceptance checks in each task below.

## Tasks

- [F7-01 — CI verification parity](./f7-01-ci-parity.md)
- [F7-02 — Regular live regression](./f7-02-live-regression.md)

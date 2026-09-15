# F7 — Continuous verification

Corresponds to the [F7 milestone](../../ROADMAP.md#f7-continuous-verification).

**Status:** ✅ Complete — CI parity (F7-01) green on GitHub Actions (2026-09-15); live Linux regression (F7-02) documented and verified on `mytecor-homelab` (2026-09-15)

## Outcome

- Every pull request runs the same generated-code, race, and documentation checks as local development.
- A repeatable Linux run (documented, manual — not a hosted CI job) verifies Python RNS interoperability and real containerd partition recovery.

## Dependencies

F1, F2, F3, F5. See [ROADMAP.md](../../ROADMAP.md).

## Scope and completion criteria

Complete the behavior and acceptance checks in each task below.

## Tasks

- [F7-01 — CI verification parity](./f7-01-ci-parity.md)
- [F7-02 — Regular live regression](./f7-02-live-regression.md)

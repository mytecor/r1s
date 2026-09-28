# F24-12 — Prove the external-consumer boundary

**Status:** ✅ Complete

## Outcome

Tests and examples consume meshbus exactly as an unrelated repository would, and the remaining
repository extraction is a release operation rather than an architectural refactor.

## Scope

- Add black-box tests using `meshbus_test`, `realm_test`, and `rns_test` where practical.
- Add a minimal direct-message/pub-sub example using only exported APIs.
- Add a dependency guard forbidding imports from meshbus back into r1s.
- Record the standalone repository cutover and first-version procedure.

## Acceptance

- At least one complete Node workflow is covered from an external package.
- Automated verification rejects any meshbus import of r1s code.
- r1s remains an integration consumer and owns all allocator, execution, lease, log, and tunnel policy.
- `make check` passes.

## Implementation notes

An external-package test runs a complete two-Node discovery, direct-authentication, subscription,
and publication workflow using only exported APIs. Realm vectors and the public RNS presence codec
also have external-package tests. The root dependency guard checks that the meshbus module cannot
resolve any r1s package. Physical repository publication remains the release operation recorded in
[`BACKLOG.md`](../BACKLOG.md).

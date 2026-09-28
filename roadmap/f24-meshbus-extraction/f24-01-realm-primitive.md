# F24-01 — Extract the secure realm primitive

**Status:** ✅ Complete

## Outcome

The public [`meshbus/realm`](https://github.com/mytecor/meshbus/tree/v0.1.0/realm) package owns shared-secret realm key generation,
public ID derivation, and ordered mutual membership proofs. It depends only on the Go standard
library and assigns no r1s roles or permissions.

The RNS adapter delegates proof creation and verification to the realm rather than implementing
HMAC membership itself. The initial extraction retained r1s-specific domains; the final meshbus
cutover subsequently removed that compatibility profile. r1s now uses the standard meshbus realm
ID and proof domains, so old credential filenames and RNS handshake bytes are intentionally
incompatible.

## Acceptance

- Realm tests prove key validation, domain-separated IDs, nonce binding, ordered peer-role binding,
  invalid-input rejection, and independent key generation.
- Cluster credential tests cover the standard meshbus realm profile.
- Existing RNS loopback and authentication tests continue to pass.
- `make check` passes.

## Notes

This task deliberately does not rename the r1s `ClusterKey` configuration surface or generalize the
r1s Protobuf envelope. Those changes belong to the direct-message vertical, where the dependency
can be inverted end-to-end instead of adding unused abstractions.

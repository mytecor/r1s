# F24-01 — Extract the secure realm primitive

**Status:** ✅ Complete

## Outcome

The public [`meshbus/realm`](../../meshbus/realm) package owns shared-secret realm key generation,
public ID derivation, and ordered mutual membership proofs. It depends only on the Go standard
library and assigns no r1s roles or permissions.

The r1s cluster package supplies the established `r1s-cluster-id-v1` and `r1s-auth-v1` domains to
that primitive. Existing credential filenames, discovery filtering, join tokens, and RNS handshake
bytes therefore remain compatible. The RNS adapter delegates proof creation and verification to the
realm rather than implementing HMAC membership itself.

## Acceptance

- Realm tests prove key validation, domain-separated IDs, nonce binding, ordered peer-role binding,
  invalid-input rejection, and independent key generation.
- Existing cluster credential tests continue to pass without changing token or ID behavior.
- Existing RNS loopback and authentication tests continue to pass.
- `make check` passes.

## Notes

This task deliberately does not rename the r1s `ClusterKey` configuration surface or generalize the
r1s Protobuf envelope. Those changes belong to the direct-message vertical, where the dependency
can be inverted end-to-end instead of adding unused abstractions.

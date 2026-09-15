# F12. Shared-secret cluster membership

Corresponds to [F12 in the roadmap](../../ROADMAP.md#f12-shared-secret-cluster-membership).

**Status:** ✅ Complete

## Outcome

Only participants holding the same cluster join token discover each other and exchange control
messages, without introducing a central authority or control plane.

## Dependencies

- [F2. RNS transport](../f2-rns-transport/README.md)

## Scope

- Generate and persist a random 256-bit `ClusterKey` through `cluster init` and `cluster join`.
- Derive a public, domain-separated `ClusterID` for allocator announces.
- Ignore allocator descriptors for other clusters.
- Mutually prove knowledge of the `ClusterKey` after RNS identity authentication and before
  delivering any control envelope.
- Keep allocator-local identity admission and quotas as an optional policy layer inside the
  authenticated cluster boundary.

## Completion criteria

- A same-cluster loopback pair completes mutual authentication and exchanges an envelope whose
  sender comes from the RNS link identity.
- A different-cluster participant is neither discovered nor authorized through a direct link.
- The join token is never carried in announces or control envelopes.
- Python-reference discovery and Channel delivery interoperate with the cluster handshake.
- `make check` passes.

## Tasks

- [F12-01 — Cluster bootstrap and transport authentication](./f12-01-cluster-authentication.md)

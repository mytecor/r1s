# F14-01 — Define artifact and endpoint descriptors

**Status:** ⏳ Planned

## Outcome

Add an additive protocol contract that identifies application artifacts independently of the URIs
from which their bytes can currently be transferred.

## Scope

- Add digest, byte size, input target, and declared output metadata.
- Add generic endpoint URIs without `yggdrasil_*` fields.
- Define canonical digest syntax, byte limits, duplicate handling, and verification timing.
- Keep endpoint advertisements small enough for the RNS control plane.

## Acceptance

- Validation tests cover supported digests, sizes, paths, URI bounds, and unknown fields.
- The same artifact descriptor can be paired with different endpoint locations without changing
  immutable workload identity.
- A digest or size mismatch is rejected before data is exposed to a workload or materialized locally.
- Existing Protobuf field numbers remain unchanged and `make check` passes.

## Notes

An artifact is application data, not an OCI image. Containerd continues to resolve digest-pinned
images through a standard registry.

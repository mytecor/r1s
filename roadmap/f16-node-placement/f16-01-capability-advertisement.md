# F16-01 — Advertise bounded node capabilities

**Status:** ✅ Complete

## Outcome

Allocator discovery and offers expose enough bounded metadata for a client to identify compatible
nodes without creating an authoritative cluster inventory.

## Scope

- Define normalized OS, architecture, runtime, device, resource-profile, and label metadata.
- Bound descriptor size, label count, key/value lengths, and update frequency.
- Treat cache and free-capacity values as hints owned by the issuing allocator.
- Represent the advertisement as an additive `NodeCapabilities` protobuf message so the same
  normalized shape is used by discovery, offers, and the local API, while the RNS announce keeps
  carrying a compact JSON summary bounded by the Reticulum app-data limit (255 bytes).

## Acceptance

- Validation tests reject oversized, duplicate, malformed, and unsupported capability data.
- RNS announce descriptors remain within their documented size budget (the 255-byte Reticulum
  app-data bin8 limit).
- Capability changes do not mutate an already assigned workload.
- `make check` passes.

## Completion

- Implemented 2026: [`NodeCapabilities`](../../api/proto/r1s/v1/control.proto) plus bounded
  validation in [`internal/protocol/placement.go`](../../internal/protocol/placement.go)
  (`MaxCapability*` bounds, lowercase-ASCII identity rules).
- `r1sd --node` parses and validates the advertisement; os/arch default from `runtime.GOOS`/
  `GOARCH`; the allocator stores it on `Config.Node` and embeds a cloned copy in every offer.
- The RNS descriptor carries only the compact os/arch/runtime summary and stays within
  `maxDescriptorBytes` (256 ≤ app-data budget); the full advertisement rides offers.
- Tests: protocol validation/matching, descriptor summary round-trip and budget,
  allocator nil-node and isolated-clone behavior; `make check` (generate-check, `-race`, lychee)
  passes.

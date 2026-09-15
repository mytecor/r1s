# F16-01 — Advertise bounded node capabilities

**Status:** ⏳ Planned

## Outcome

Allocator discovery and offers expose enough bounded metadata for a client to identify compatible
nodes without creating an authoritative cluster inventory.

## Scope

- Define normalized OS, architecture, runtime, device, resource-profile, and label metadata.
- Bound descriptor size, label count, key/value lengths, and update frequency.
- Treat cache and free-capacity values as hints owned by the issuing allocator.

## Acceptance

- Validation tests reject oversized, duplicate, malformed, and unsupported capability data.
- RNS announce descriptors remain within their documented size budget.
- Capability changes do not mutate an already assigned workload.
- `make check` passes.

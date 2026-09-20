# F16. Node capabilities and placement

Corresponds to [milestone F16](../../ROADMAP.md#f16-node-capabilities-and-placement).

**Status:** ✅ Complete

## Outcome

Clients express compatibility constraints for heterogeneous nodes, allocators advertise bounded
capabilities, and only compatible allocators offer. The client still selects the winning offer; no
global scheduler or authoritative inventory is introduced.

## Dependencies

- [F4. Client workflow](../f4-client-workflow/README.md)
- [F10. Local admission and resource limits](../f10-local-admission/README.md)
- [F12. Shared-secret cluster membership](../f12-cluster-membership/README.md)

## Scope

- Advertise OS, architecture, runtime, supported resource profiles, devices, and operator labels.
- Add exact-match placement constraints with explicit validation and bounded cardinality.
- Include useful offer metadata such as free capacity, selected resource profile, and image-cache hint.
- Keep local admission authoritative even when advertised labels match.
- Defer general-purpose scoring, affinity graphs, and global inventory.

## Completion criteria

- Incompatible allocators do not reserve capacity or return offers.
- Stale or false advertisements never bypass allocator-local validation at assignment.
- Deterministic tests cover mixed architecture, labels, devices, duplicate delivery, and restart.
- Client-owned offer selection and all existing authority guarantees remain intact.

## Tasks

- [F16-01 — Advertise bounded node capabilities](./f16-01-capability-advertisement.md)
- [F16-02 — Match placement constraints](./f16-02-constraint-matching.md)

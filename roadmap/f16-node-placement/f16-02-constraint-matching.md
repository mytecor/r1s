# F16-02 — Match placement constraints

**Status:** ✅ Complete

## Outcome

Allocator-local matching prevents incompatible offers while leaving the final choice among valid
offers to the client.

## Scope

- Add bounded exact-match constraints for architecture, OS, runtime, labels, and declared devices.
- Revalidate constraints during assignment against current local state.
- Expose simple offer hints without adding a generic scoring language.
- Client-side: only compatible allocators receive the request; offer selection ranks compatible
  allocators deterministically.

## Acceptance

- Mixed-node tests prove only matching allocators reserve and offer.
- An advertisement-to-assignment capability change yields an explicit rejection (`INCOMPATIBLE`),
  not an invalid start.
- Matching cannot grant device access not allowed by the allocator's admission/runtime policy.
- `make check` passes.

## Completion

- Implemented 2026: [`PlacementConstraints`](../../api/proto/r1s/v1/control.proto) on
  `ExecutionRequest` (field 5) and `LocalRequest` (field 7), carrying bounded exact-match
  constraints across the socket and wire contracts.
- Matching lives in [`internal/protocol/placement.go`](../../internal/protocol/placement.go):
  `PlacementMatches` (authoritative) and `PlacementCompatible` (advisory, nil = unknown = try).
- Request-time rejection happens before any capacity is reserved or offer minted; assignment-time
  revalidation emits an explicit `INCOMPATIBLE` (never an invalid start). A resource class absent
  from the node's declared profiles is rejected as admission.
- Client-side only compatible allocators receive the request; offer selection ranks deterministically
  with the workload's cached-image as a tie-break.
- Tests: mixed-node allocator suite including incompatible-request-no-reserve,
  assignment-revalidation, and nil-node behavior; `make check` passes.

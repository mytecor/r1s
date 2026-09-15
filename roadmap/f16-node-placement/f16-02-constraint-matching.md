# F16-02 — Match placement constraints

**Status:** ⏳ Planned

## Outcome

Allocator-local matching prevents incompatible offers while leaving the final choice among valid
offers to the client.

## Scope

- Add bounded exact-match constraints for architecture, OS, runtime, labels, and declared devices.
- Revalidate constraints during assignment against current local state.
- Expose simple offer hints without adding a generic scoring language.

## Acceptance

- Mixed-node tests prove only matching allocators reserve and offer.
- An advertisement-to-assignment capability change yields an explicit rejection, not an invalid start.
- Matching cannot grant device access not allowed by the allocator's admission/runtime policy.
- `make check` passes.

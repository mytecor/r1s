# F17-01 — Add deterministic workspace input and output

**Status:** ⏳ Planned

## Outcome

Implement the client-side pack, transfer, execution mapping, output capture, and safe materialization
needed for a single-directory workspace workflow.

## Scope

- Define deterministic archive rules and ignore handling.
- Upload the input through F14/F15 and map it to a declared workload directory.
- Capture only declared outputs, publish their descriptors, and materialize them atomically.
- Add path traversal, symlink, permissions, size, and overwrite protections.

## Acceptance

- An end-to-end test edits a workspace remotely and retrieves the verified result locally.
- Determinism and extraction-safety tests cover supported file types and hostile archive entries.
- No workspace bytes are placed in RNS messages.
- `make check` passes.

## Notes

Repository synchronization and merge semantics remain application concerns. This task only maps
local directory snapshots to the artifact contract.

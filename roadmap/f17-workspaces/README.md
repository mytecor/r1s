# F17. Workspaces

Corresponds to [milestone F17](../../ROADMAP.md#f17-workspaces).

**Status:** ⏳ Planned

## Outcome

A user can package a local directory, run a remote workload against it, and materialize declared
outputs locally through content-addressed artifacts. `Workspace` is client convenience behavior,
not a shared filesystem or a new distributed-protocol object.

## Dependencies

- [F13. Local client API](../f13-local-client-api/README.md)
- [F14. External data-plane contract](../f14-external-data-plane/README.md)
- [F15. Yggdrasil data plane](../f15-yggdrasil-data-plane/README.md)

## Scope

- Deterministically pack and hash a local directory as one or more input artifacts.
- Materialize verified input content at the declared workload path.
- Capture declared outputs as artifacts and safely write them to a selected local destination.
- Add CLI and local API convenience operations for the end-to-end flow.
- Define ignore rules, symlink policy, permissions, path safety, and overwrite behavior.

## Completion criteria

- One command or local API call runs a workload with a local workspace and retrieves its outputs.
- Packing the same supported tree produces the same digest.
- Extraction cannot escape the target directory or overwrite undeclared paths.
- Interrupted transfers and client restart resume or fail explicitly without corrupting local data.
- The core protocol continues to know artifacts, not repositories, projects, agents, or shared mounts.

## Tasks

- [F17-01 — Add deterministic workspace input and output](./f17-01-workspace-flow.md)

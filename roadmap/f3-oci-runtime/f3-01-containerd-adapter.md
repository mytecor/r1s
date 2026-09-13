# F3-01 — Integrate containerd

**Status:** Planned

## Outcome

Implement the runtime interface using the containerd Go client and an isolated r1s namespace, then
wire it into `cmd/r1sd/`.

## Acceptance

- A fixture workload starts, exits, and reports its exit code.
- Cancellation stops and cleans the task and container.
- Repeating start or stop does not duplicate work or corrupt state.

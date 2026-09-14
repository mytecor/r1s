# Agent instructions

## Required context

- [README.md](./README.md) defines project purpose and the documentation index.
- [ARCHITECTURE.md](./ARCHITECTURE.md) defines architectural decisions and component boundaries.
- [ROADMAP.md](./ROADMAP.md) points to the plan, features, and tasks.
- [CONTRIBUTING.md](./CONTRIBUTING.md) defines verification requirements.

## Invariants

- Keep the core independent of Reticulum-Go and containerd through explicit interfaces.
- Treat the sender identity verified by the transport as authority; never trust an identity copied
  from an unverified payload.
- Do not tie execution lifetime to a connection or heartbeat.
- Keep container stdout/stderr local to the allocator. Transfer logs only in response to an explicit
  authenticated log request; never attach or automatically send logs on completion, failure,
  inspection, result retrieval, or reconnection.
- Preserve Protobuf field numbers once the schema exists and evolve messages additively.
- Record unfinished decisions in [`roadmap/BACKLOG.md`](./roadmap/BACKLOG.md).

## Documentation links

Links to repository files must use relative Markdown links such as
`[ARCHITECTURE.md](./ARCHITECTURE.md)`, not bare file paths.

## Verification

Run `make check` before considering a documentation change complete. Add implementation-specific
checks together with the feature that introduces the relevant code or generated artifact.

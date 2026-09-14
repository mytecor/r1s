# F3-01 — Integrate containerd

**Status:** In progress

## Outcome

Implement the runtime interface using the containerd Go client and an isolated r1s namespace, then
wire it into `cmd/r1sd/`.

## Acceptance

- A fixture workload starts, exits, and reports its exit code.
- Cancellation stops and cleans the task and container.
- Repeating start or stop does not duplicate work or corrupt state.

## Implemented

- The adapter uses the containerd 2.3 LTS client and the isolated `r1s` namespace.
- Image references must contain a valid digest, and the pulled target digest must match it.
- Container IDs are derived deterministically from execution IDs. Labels retain the original
  execution ID and a deterministic owner/workload/policy fingerprint so an existing container is
  reused only when its authority and immutable specification match.
- OCI image defaults, command/argument overrides, environment, and working directory are mapped to
  the generated runtime specification.
- Start and stop are concurrency-safe and idempotent. Cancellation kills the task, waits for exit,
  deletes the task, and removes the container snapshot without publishing a normal completion.
- Absolute deadlines and maximum runtime are enforced by a local timer independent of transport
  connectivity or request context lifetime.
- Unit tests use an internal backend boundary; `TestContainerdFixtureLifecycle` is gated for a real
  Linux containerd daemon.

## Remaining

- Run `TestContainerdFixtureLifecycle` against a live containerd daemon with a digest-pinned fixture
  image. The local development host used for this change has no containerd daemon.
- Durable allocator state and restart reconciliation are tracked in
  [F3-02](./f3-02-reconciliation.md).

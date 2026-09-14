# F3-01 — Integrate containerd

**Status:** Complete

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
  execution ID and a deterministic client/workload/policy fingerprint so an existing container is
  reused only when its authority and immutable specification match.
- OCI image defaults, command/argument overrides, environment, and working directory are mapped to
  the generated runtime specification.
- Start and stop are concurrency-safe and idempotent. Cancellation kills the task, waits for exit,
  deletes the task, and removes the container snapshot without publishing a normal completion.
- Absolute deadlines and maximum runtime are enforced by a local timer independent of transport
  connectivity or request context lifetime.
- Unit tests use an internal backend boundary; `TestContainerdFixtureLifecycle` is gated for a real
  Linux containerd daemon.

## Live verification

- On 2026-09-14, `TestContainerdFixtureLifecycle` passed on `mytecor-homelab` using containerd 2.3.4
  and `docker.io/library/alpine@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce`.
- Durable allocator state and restart reconciliation are covered by
  [F3-02](./f3-02-reconciliation.md).

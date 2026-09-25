# F22. Shared-instance RNS and run-oriented client

Corresponds to the [F22 milestone](../../ROADMAP.md#f22-shared-instance-rns-and-run-oriented-client).

**Status:** 🚧 In progress — F22-01 through F22-05 complete

## Outcome

The public client becomes one lease-owning operation:

```text
r1s cluster init
r1s cluster join <token>
r1s cluster list

r1s run <cluster> [-d] [-p host:container] [--log-file path] <workload>
r1sd <cluster>
```

`r1s run` attaches to an already-running shared RNS instance, creates an ephemeral client identity,
announces the workload, deterministically selects a compatible offer, holds the execution lease,
tails allocator-local logs, and owns any published ports. If an execution is conclusively lost, the
same logical run announces a new attempt without allocator pinning. Foreground termination ends
ownership; detached mode keeps the same run loop alive and records its output locally.

The feature removes the client-side control plane accumulated around `request`: no persistent client
database, local gRPC service, `serve` process, Unix command socket, durable lease intent, or separate
CRUD/log/tunnel commands remain in the public interface. Allocator persistence remains intact. F22
is a clean cutover: old client configuration, state, sockets, commands, and cluster-store layout are
not migrated or supported after the switch.

## Dependencies

- [F2](../f2-rns-transport/README.md) — authenticated RNS transport boundary.
- [F9](../f9-local-logs/README.md) — allocator-local logs and explicit bounded retrieval.
- [F12](../f12-cluster-membership/README.md) — cluster credentials and authenticated membership.
- [F16](../f16-node-placement/README.md) — compatible offers and placement metadata.
- [F17](../f17-execution-lease/README.md) — allocator-enforced renewable execution lifetime.
- [F19](../f19-tunnel-rework/README.md) and
  [F20](../f20-client-tunnel-targets/README.md) — multiplexed tunnel and client-selected ports.
- [F21-06](../f21-tunnel-rns-dataplane/f21-06-remove-ygg-adapter.md) — complete the rejected private-RNS
  tunnel rollback before simplifying the retained Ygg tunnel control flow.

## Scope

- Require a pre-existing shared RNS instance for `r1s` and `r1sd`; neither binary silently starts a
  private stack or becomes the shared-instance server.
- Replace the command-oriented client with one `run` lifecycle and an ephemeral per-run identity.
- Add a stable protocol `run_id` and monotonic `attempt` while keeping each `execution_id` scoped to
  one allocator attempt. Execution is explicitly at-least-once across ambiguous lease loss.
- Make cluster selection an explicit positional part of `run`, backed by a multi-cluster local
  credential store. A single `r1sd` process joins exactly one cluster.
- Reannounce without allocator pinning after conclusive loss and select offers with a deterministic
  policy that never depends on map or goroutine arrival order.
- Tail allocator-local logs by offset into foreground stdout/stderr or one detached output file;
  log transfer remains explicit and authenticated internally.
- Bind published local ports for the lifetime of the logical run and rebind them to each new
  execution attempt without dropping the listeners.
- Retain allocator bbolt state, restart reconciliation, replay protection, tombstones, capacity
  reservations, and lease enforcement. Move result-retention policy out of the workload surface and
  into allocator-local configuration.
- Retain internal control operations needed by the run loop (`Inspect`, `Cancel`, `OfferRelease`,
  lease renewal, and bounded log retrieval) even though their standalone CLI commands disappear.
- Do not provide compatibility shims, automatic state migration, or a transition mode for the old
  CLI, local API, persistent client identity, or single-cluster credential file.
- Evolve Protobuf additively: allocate new field numbers, reserve every removed field/name, and
  never reuse the experimental tunnel numbers removed by F21-06. Number preservation is a schema
  safety invariant, not a promise that old clients remain supported.

## Completion criteria

- With a shared RNS daemon running, `r1s run <cluster> <workload>` reaches compatible allocators,
  starts one attempt, tails its logs, renews its lease, and exits with the workload result.
- A confirmed expired/not-found attempt or a documented conservative lease-loss threshold creates
  a higher-numbered attempt for the same `run_id`; a single missed acknowledgement does not trigger
  rescheduling, and tests acknowledge the possible overlap inherent in at-least-once execution.
- `-d` returns a run ID and log path after a child handshake; the child keeps ownership, records its
  PID/output, and removes the PID marker when it exits.
- Published listeners survive rescheduling; existing streams may close, but new streams reach the
  replacement execution.
- `r1s --help` exposes only cluster management and `run`; removed client commands, sockets, state
  paths, persistent client identity, allocator pinning, and RNS config flags are absent.
- Allocator restart recovery and retained protocol operations continue to pass without a client DB
  or local gRPC service.
- `go build ./...`, `go vet ./...`, `go test -race ./...`, and `make check` pass.

## Tasks

- [F22-01 — Required shared-instance RNS client](./f22-01-shared-instance-config.md) — complete
- [F22-02 — Stable run identity and at-least-once attempts](./f22-02-run-attempt-protocol.md) — complete
- [F22-03 — Multi-cluster credential store and explicit selection](./f22-03-cluster-selection.md) — complete
- [F22-04 — Ephemeral run engine and deterministic placement](./f22-04-run-engine.md) — complete
- [F22-05 — Foreground and detached log continuity](./f22-05-detached-logs.md) — complete
- [F22-06 — Run-owned published ports and tunnel rebinding](./f22-06-run-tunnels.md)
- [F22-07 — Legacy client removal and allocator-local retention](./f22-07-client-cleanup.md)

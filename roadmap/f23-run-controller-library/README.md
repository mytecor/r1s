# F23. Public Run Controller library

Corresponds to the [F23 milestone](../../ROADMAP.md#f23-public-run-controller-library).

**Status:** ✅ Complete

## Outcome

The reusable product boundary is the public [`client`](../../client) package, not the `r1s`
command and not a machine-wide client daemon. An application opens a joined cluster, starts one
ephemeral RNS participant, and calls `Run` directly. The transport-verified identity of that
participant is the authority for the run; no identity copied from workload data is trusted.

The controller owns discovery, request creation, deterministic offer selection, losing-offer
release, assignment, lease renewal, authenticated inspection, and conclusive-loss rescheduling.
Container stdout/stderr remains allocator-local and is read only through the explicit bounded
`Logs` operation. `OpenTunnel` performs the authenticated control-plane authorization without
moving application bytes onto RNS or coupling the core to the selected tunnel adapter.

The `r1s run` command is now an adapter over the same API. It retains command parsing,
foreground/detached process lifetime, terminal/file presentation, and local published-port
listeners, but no longer owns a separate request/lease/reschedule implementation.

## Authority and lifetime

- One `Client` instance owns exactly one logical run and uses a fresh in-memory RNS identity.
- A new run requires a new client and therefore fresh authority.
- The client persists no request, assignment, identity, or lease intent.
- Closing the client does not stop an execution through transport state. The allocator-enforced,
  durably persisted lease remains the only lifetime boundary.
- Context cancellation sends a best-effort authenticated cancel, but a missing acknowledgement is
  not treated as proof that the execution stopped.
- Delegation to another identity is not part of this feature; it remains an explicit future
  capability decision rather than an implicit forwarding daemon.

## Acceptance

- External Go code can import `github.com/mytecor/r1s/client`, select a locally joined cluster, and
  run a workload without importing an `internal` package.
- `r1s run` uses `client.Run` for foreground and detached workloads.
- Log transfer occurs only when `Logs` is called explicitly.
- Tunnel control is owner-authenticated and the application-data adapter remains outside the
  public run controller.
- Separate clients use separate ephemeral identities; a client rejects a second run.
- `make check` passes.

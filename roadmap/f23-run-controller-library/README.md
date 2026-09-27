# F23. Public Run Controller library and cluster authority context

Corresponds to the
[F23 milestone](../../ROADMAP.md#f23-public-run-controller-library-and-cluster-authority-context).

**Status:** ✅ Complete

## Outcome

The reusable product boundary is the public [`client`](../../client) package, not the `r1s`
command and not a machine-wide run daemon. An application can open joined credentials directly and
call `Run`. The CLI uses `r1s cluster use <cluster>` to start a foreground per-user authority
broker, or adds `-d` to detach it, after which `r1s run <workload>` needs no cluster operand. The
broker retains the cluster key and creates
one ephemeral RNS endpoint per connected controller; the calling process, not the broker, owns the
run. The transport-verified identity of that endpoint is the authority for the run; no identity
copied from workload data is trusted.

The controller owns discovery, request creation, deterministic offer selection, losing-offer
release, assignment, lease renewal, authenticated inspection, and conclusive-loss rescheduling.
Container stdout/stderr remains allocator-local and is read only through the explicit bounded
`Logs` operation. `OpenTunnel` performs the authenticated control-plane authorization without
moving application bytes onto RNS or coupling the core to the selected tunnel adapter.

The `r1s run` command is an adapter over the same API. It retains command parsing,
foreground/detached process lifetime, terminal/file presentation, and local published-port
listeners, but owns no separate request/lease/reschedule implementation.

## Cluster authority context

- `r1s cluster use <cluster>` resolves a stored full ID or unique prefix, replaces any previous
  current authority context, and serves the local broker in the foreground until interrupted.
- `r1s cluster use -d <cluster>` performs the same selection but detaches the broker into a
  background process. It shares the same re-exec, stdout readiness, timeout, cancellation, and
  process-release machinery as `r1s run -d`; only the readiness payload validation differs.
- `r1s cluster status` reports the selected public cluster ID and broker endpoint;
  `r1s cluster unset` stops it.
- `r1s run` connects to the broker and carries no cluster operand or join token.
- `R1S_SOCKET` may point the CLI at a mounted broker endpoint, so a container can use r1s without
  receiving the cluster key.
- Every broker connection creates a fresh RNS identity and endpoint. The broker proxies only
  authenticated transport discovery and envelopes; it exposes no run CRUD or desired-state API.
- Stopping or replacing the broker removes transport access but is not execution-stop evidence.
  Allocator-side lease expiry remains the durable lifetime boundary.

## Authority and lifetime

- One `Client` instance owns exactly one logical run and uses a fresh ephemeral RNS identity,
  whether its endpoint is direct or broker-backed.
- A new run requires a new client and therefore fresh authority.
- The client persists no request, assignment, identity, or lease intent.
- Closing the client does not stop an execution through transport state. The allocator-enforced,
  durably persisted lease remains the only lifetime boundary.
- Context cancellation sends a best-effort authenticated cancel, but a missing acknowledgement is
  not treated as proof that the execution stopped.
- The authority broker is not allocator-side per-execution delegation. Scoped child permissions
  remain an explicit future capability decision rather than an unrestricted workload credential.

## Acceptance

- External Go code can import `github.com/mytecor/r1s/client`, select a locally joined cluster, and
  run a workload without importing an `internal` package.
- Foreground `cluster use`, explicit `cluster use -d`, `status`, and `unset` manage a non-secret
  current context; `r1s run` requires that context and no longer accepts a cluster operand.
- The broker never sends a cluster key to a connected run and creates separate transport identities
  for separate connections.
- `r1s run` uses `client.Run` for foreground and detached workloads.
- Log transfer occurs only when `Logs` is called explicitly.
- Tunnel control is owner-authenticated and the application-data adapter remains outside the
  public run controller.
- Separate clients use separate ephemeral identities; a client rejects a second run.
- `make check` passes.

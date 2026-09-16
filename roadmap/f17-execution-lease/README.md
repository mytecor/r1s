# F17. Execution lease

Corresponds to [milestone F17](../../ROADMAP.md#f17-execution-lease).

**Status:** ✅ complete; deterministic allocator, client, protocol, socket-contract, and
service-backed CLI tests pass under `go test -race`

## Outcome

Execution lifetime is bounded by a durable, explicitly renewed client-held lease instead of a
request-time deadline. A client that keeps renewing keeps its execution; a client that is gone
forever has its execution evicted locally after lease expiry, with terminal metadata retained and
distinguishable from a client cancellation.

## Dependencies

- [F2. RNS transport](../f2-rns-transport/README.md)
- [F3. OCI runtime](../f3-oci-runtime/README.md)
- [F13. Local client API](../f13-local-client-api/README.md)

## Scope

- Add an authenticated, idempotent, replay-safe lease renewal operation to the control protocol,
  driven from `r1s request --keep-alive` (with a local-API binding through the service).
- Persist the lease expiry for every running execution in allocator durable state so it survives
  allocator restart.
- Evict an execution whose lease expires without renewal through the existing runtime stop
  boundary; record terminal state whose cause is distinguishable from client cancellation.
- Retire `deadline` and `max_runtime` as lifetime bounds: reserve the retired `ExecutionPolicy`
  fields, lift the "at least one required" validation, and move enforcement from the runtime
  completion monitor to the allocator lease sweep. `result_retention` is unchanged. (Done: the
  initial lease is granted by the allocator at assignment and extends on each renewal.)
- Reword the lifetime rule: connection state still never determines execution lifetime; a lease
  outlives any partition shorter than its duration. Update ARCHITECTURE.md, the AGENTS.md
  invariant wording, and the README flow to match.
- Renewal never attaches logs, results, or other payload; it returns only the new expiry.
- When a lease is lost — the execution was evicted before a renewal landed — the durable
  lease-holding intent converts into a re-request duty: the client engine runs a fresh request with
  the recorded workload and rebinds the intent to the replacement execution. The keep-alive
  requester prints the recovery (`status=rerequested`), and the serve renewal loop does the same
  on its next tick.

Renewal is an authenticated control-plane primitive, not a loop. The renewal loop lives only in
the shared durable client engine: `r1s serve` renews the lease-holding intents recorded in its
durable state (recorded by `r1s request --keep-alive`), and the deploy reconciler records the same
intent instead of implementing its own renewal logic. Direct-mode callers pass `--keep-alive` so
the request process itself stays up and renews until the workload terminates.

## Completion criteria

- An execution whose lease is never renewed is evicted after expiry; capacity returns and the
  terminal metadata stays retrievable for the `result_retention` horizon.
- Renewal extends the expiry; only the authenticated owner can renew; replaying a renewal is safe.
- An allocator restart neither evicts executions whose lease is still valid nor revives ones whose
  lease already expired.
- Losing the transport connection alone never ends an execution while the lease is unexpired.
- Deterministic and acceptance tests pass under `go test -race`; `make check` passes.

## Tasks

- [F17-01 — Add renewable client-held execution leases](./f17-01-execution-lease.md)

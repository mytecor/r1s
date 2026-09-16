# F17-01 — Add renewable client-held execution leases

**Status:** ⏳ Planned

## Outcome

Executions are bounded by a durably persisted, explicitly renewed lease instead of a request-time
deadline. An unrenewed lease ends the execution locally with retained terminal metadata; a live
client holds its execution alive with periodic renewals.

## Scope

- Add `ExecutionLeaseRenew { execution_id, lease_duration }` and its ack carrying the new expiry to
  the control protocol additively; reserve the retired `ExecutionPolicy` fields 1–2
  (`deadline`, `max_runtime`) without reusing their numbers, lift the "at least one of
  deadline/max_runtime" validation, and keep `result_retention` unchanged.
- Add the `LocalClient` maintain binding and the `r1s maintain <exec-id> [--lease duration]` CLI
  command as a one-shot authenticated renewal primitive in both frontends. The continuous renewal
  loop lives only in the shared durable client engine as a durable lease-holding intent replayed
  on every reconciliation tick; `r1s serve` keeps it running, and the deploy reconciler records the
  same intent instead of implementing its own renewal logic. An unreachable socket is an error,
  never a fallback, and `maintain` never spawns a background process.
- Allocator: persist lease expiry per execution in the bbolt state snapshot; run a local expiry
  sweep that stops the container through the existing runtime `Stop` boundary; commit terminal
  state before containerd metadata removal as with cancellation; return capacity on eviction.
- Renewal is authenticated against the execution owner, idempotent, and replay-safe; a renewal
  from any other identity is rejected with the same authority error as a foreign cancel.
- Terminal state after eviction records a lease-expiry reason distinct from client cancellation;
  retention, tombstones, and log access rules are unchanged.
- `r1s deploy` reconcilers (F15) record lease-holding duty in their durable state and renew the
  active execution as part of every persisted reconciliation transition.
- Update the documentation that pins the current lifetime rule: the ARCHITECTURE.md
  "Lifecycle under disconnection" section (deadline bullet becomes lease expiry; add that a
  partition shorter than the lease never ends an execution), the persistence paragraph that names
  local deadline enforcement, and the AGENTS.md invariant wording so lifetime is tied to an
  explicit client-held lease, never to transport connection state.

## Acceptance

- An execution whose lease is never renewed is evicted after expiry, capacity returns, and the
  terminal state is distinguishable from cancellation while staying retrievable within
  `result_retention`.
- Renewal by the owner extends the persisted expiry; renewal by another identity is rejected;
  a replayed renewal message cannot shorten or double-extend the lease.
- An allocator restart evicts expired leases and does not evict executions whose lease is still
  valid, without restarting running tasks.
- A lost transport connection alone never ends an execution while the lease is unexpired.
- Direct-mode and service-backed CLI tests exercise `r1s maintain` for extend and reject paths.
- Existing authority, disconnection lifetime, explicit-log-request, retention, and replay tests
  stay green under `go test -race`; `make check` passes.

## Notes

- Lease renewal is an explicit authenticated message, not a property of the transport connection:
  the invariant "lifetime is not tied to a connection or heartbeat" survives as "lifetime is
  bounded by explicit renewals, not connection state". The AGENTS.md wording is revisited when
  this feature lands.
- Open parameters are recorded in [BACKLOG.md](../BACKLOG.md): default and bounded lease
  duration, clock semantics across allocator restart, eviction grace period, and whether
  `max_runtime` is kept as an optional hard backstop against a failed local sweep.
- Protobuf evolution stays additive; field numbers of the retired `ExecutionPolicy` fields are
  reserved, never reused.
- `r1s maintain` is a one-shot primitive, never a daemon launcher: an unreachable service socket
  is an error per the established frontend rule, and the only long-running renewal holder is the
  shared durable client engine inside `r1s serve`. Direct-mode callers either invoke `maintain`
  periodically themselves (cron with a lease duration covering the interval) or run `r1s serve`.

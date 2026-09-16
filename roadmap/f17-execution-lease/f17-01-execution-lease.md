# F17-01 — Add renewable client-held execution leases

**Status:** ✅ complete; deterministic and CLI tests pass under `go test -race`

## Outcome

Executions are bounded by a durably persisted, explicitly renewed lease instead of a request-time
deadline. An unrenewed lease ends the execution locally with retained terminal metadata; a live
client holds its execution alive with periodic renewals.

## Scope

- Add `ExecutionLeaseRenew { execution_id, lease_duration }` and its ack carrying the new expiry to
  the control protocol additively; reserve the retired `ExecutionPolicy` fields 1–2
  (`deadline`, `max_runtime`) without reusing their numbers, lift the "at least one of
  deadline/max_runtime" validation, and keep `result_retention` unchanged.
- Expose lease holding as `r1s request --keep-alive [--lease duration]` in both frontends. The
  flag durably records a lease-holding intent at assignment; in direct mode the request process
  itself is the renewal loop (it exits when the workload terminates), and in service-backed mode
  the intent is forwarded to `r1s serve`, whose shared durable client engine replays it on every
  reconciliation tick. The deploy reconciler records the same intent instead of implementing its
  own renewal logic. An unreachable socket is an error, never a fallback, and keep-alive never
  spawns a background process. A lost lease — an execution evicted before a renewal landed —
  converts the intent into a re-request of the recorded workload, rebinds it to the replacement
  execution, and surfaces the recovery on stdout.
- Allocator: grant a durable initial lease at assignment (10 minutes by default) and persist it per
  execution in the bbolt state snapshot; run a local expiry sweep that stops the container through
  the existing runtime `Stop` boundary; commit terminal state before containerd metadata removal as
  with cancellation; return capacity on eviction. Renewal durations are bounded by the 7-day command
  replay horizon; a renewal is accepted while the execution is still non-terminal, so the lazy sweep
  interval is the implicit grace period.
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
- Service-backed CLI tests exercise `request --keep-alive` intent recording and flag rejection; allocator and client engine tests cover extend, foreign-reject, and replay paths.
- Existing authority, disconnection lifetime, explicit-log-request, retention, and replay tests
  stay green under `go test -race`; `make check` passes.

## Notes

- Lease renewal is an explicit authenticated message, not a property of the transport connection:
  the invariant "lifetime is not tied to a connection or heartbeat" landed as "lifetime is bounded
  by a durably persisted, explicitly renewed client-held lease, never by connection state".
- Open parameters are recorded in [BACKLOG.md](../BACKLOG.md): clock semantics across allocator
  restart (wall-clock persisted expiry versus monotonic accounting) and whether a strict eviction
  grace period after one missed renewal is worth adding over the lazy sweep.
- Protobuf evolution stays additive; field numbers of the retired `ExecutionPolicy` fields are
  reserved, never reused.
- There is no standalone renewal command: lease holding is requested where the execution is born,
  through `request --keep-alive`. An unreachable service socket is an error per the established
  frontend rule, and the long-running renewal holders are the shared durable client engine inside
  `r1s serve` and the foreground direct-mode keep-alive request.

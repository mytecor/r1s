# F13-01 — Add the persistent local client service

**Status:** ✅ complete; unit, socket-contract, and service-backed CLI acceptance tests pass

## Outcome

Run the durable client behind a versioned local gRPC API and stream execution-state changes to
local applications.

## Scope

- Define additive API messages for request, inspect, cancel, list, result, logs, and watch.
- Add `r1s serve` with a Unix socket and restrictive default permissions.
- Keep one RNS endpoint, client identity, and state store alive for the service lifetime.
- Make watch delivery revision-ordered, reconnectable, and bounded for slow consumers.

## Acceptance

- Contract tests exercise every unary operation through the socket.
- A watch test observes ordered state revisions across disconnect and service restart.
- Existing replay, authority, log-request, and partition guarantees remain covered.
- `make check` passes.

## Notes

The API is a local frontend for one client identity. It does not own allocator state, perform global
scheduling, or become a required hop for direct CLI use.

## Implementation

- `api/proto/r1s/v1/local.proto` defines the additive `LocalClient` service and its messages. The
  service namespace and versioning are shared with the control plane (`r1s.v1`); it is served only
  over a Unix socket and never over RNS.
- `internal/client` grows a durable, revision-ordered watch journal. Each accepted execution-state
  transition is assigned a monotonic `watchSequence` and retained in a bounded journal
  (`watchJournalCapacity` = 1024). The sequence and journal are persisted with the client snapshot,
  so a watcher resumes from an exact durable position across service restarts and a slow consumer
  is told to re-synchronize (via `resync`) rather than silently missing a revision.
- `internal/localserver` implements the gRPC service. `Watch` replays the retained journal after a
  durable position, then follows live observers; delivery is pull-from-journal so the slow-consumer
  case never drops an event.
- `r1s serve` keeps the RNS endpoint, identity, state store, and offer-release loop alive for the
  process lifetime and binds the socket at `0600` by default (`--socket-mode` to change). An existing
  stale socket file is replaced only when it is not a live service.
- Contract tests in `internal/localserver` and `internal/client` cover every unary operation over a
  real Unix socket, watch replay/resume, gap detection, and the "never emit on failed persist" rule.

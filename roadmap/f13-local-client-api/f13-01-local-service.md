# F13-01 — Add the persistent local client service

**Status:** ⏳ Planned

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

# F22-01 — Required shared-instance RNS client

**Status:** ✅ Complete (2026-09-25)

## Outcome

`r1s` and `r1sd` attach as clients to an already-running Reticulum shared instance. They do not
load an r1s-specific RNS config, construct a private `rnstransport.Transport`, or silently become
the shared-instance server when no daemon is available.

## Scope

- Replace `internal/transport/rns/stack.go` private-stack bootstrap with a client-only
  shared-instance attachment behind the existing transport interface.
- Use Reticulum-Go platform defaults for the shared-instance address (Unix abstract socket on
  Linux and the supported TCP default elsewhere), without exposing `share_instance`,
  `shared_instance_type`, or `instance_name` in the normal r1s CLI.
- Fail startup with an actionable `RNS shared instance is not running` error when attachment is not
  possible.
- If Reticulum-Go's public `Attach` API can still elect the caller as server, add or upstream a
  client-only `Connect`/`RequireSharedInstance` capability rather than accepting join-or-own
  behavior in r1s.
- Keep injected/standalone transports only in deterministic and live test harnesses, never as a
  production fallback.
- Remove `--rns-config` from `r1s` and `r1sd` user-facing configuration.

## Acceptance

- A test proves that production startup uses `sharedinstance.ModeClient`, never `ModeDisabled` or
  `ModeServer`, and never calls the private-stack constructor.
- Starting either binary without a shared instance fails closed and does not create a listener.
- Two r1s participants attached to the same Go or Python shared instance discover the same peer set
  and exchange authenticated r1s control envelopes.
- Existing in-memory transport tests remain independent of Reticulum-Go.
- `go build ./...`, `go vet ./...`, `go test -race ./...`, and `make check` pass.

## Notes

- This changes transport bootstrap only. The shared daemon owns interfaces and routing; r1s still
  owns its application identity, cluster authentication, control protocol, and execution leases.
- Backward compatibility with the private per-process production stack is deliberately not kept.

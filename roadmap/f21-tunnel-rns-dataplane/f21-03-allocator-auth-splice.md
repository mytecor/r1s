# F21-03 — Allocator authorization and container splice

**Status:** ⏳ Planned

## Outcome

`r1sd` authorizes a tunnel purely from the identified remote RNS identity and the `Open` message,
then resolves the requested port strictly inside the execution's network namespace — preserving the
existing security boundary with no grant flow and no peer-key pinning.

## Scope

- On an inbound tunnel Link, wait for `Link.Identify()`; treat the verified remote identity as the
  sole application-level authority (never trust an identity copied from an unverified payload).
- Validate the `Open { execution_id, port }` against allocator state:
  - execution exists,
  - execution is running,
  - `execution.owner == remote RNS identity`,
  - `port > 0`.
- On success call `Allocator.DialExecution(executionID, port)` and splice
  `Backbone Link ↔ container TCP` through `RNS Channel`/`Buffer`; reply `OK` then begin the byte
  stream. On failure reply a classified error (`unauthorized`, `not running`, `invalid port`) and
  never fall back into the allocator's own network namespace.
- Keep the existing container path
  (`DialTunnelTarget → containerd.Adapter.DialExecution → LoadContainer →
  verifyLabels/fingerprint → task running check → pidfd pin → setns → loopback → tcp4
  127.0.0.1:<port>`) unchanged.
- Remove the peer-key-pinned accept logic and the one-live-session-per-execution grant/session
  registry bookkeeping that the new identity + Open flow makes redundant; a Link is the session.

## Acceptance

- Owner can open a tunnel to their running execution; the tunnel reaches only `127.0.0.1:<port>`
  inside the execution's namespace.
- A different (`Link.Identify`-verified) identity is rejected `unauthorized` before any payload.
- Tunnel to a stopped/completed execution is rejected; `port == 0` is rejected with an explicit
  classified error.
- `DialExecution` cannot connect to a port in the allocator's own namespace (existing isolation
  regression stays green).
- `go build ./...`, `go vet ./...`, `go test -race ./...`, `make check` pass.

# F25-01 — Bootstrap known allocator destinations

**Status:** ✅ Implemented; live shared-daemon acceptance verified on `mytecor-homelab` (2026-10-04)

## Outcome

Cluster membership carries or resolves a bounded set of known allocator destination hashes, and
every new run sends its first execution demand to those destinations immediately while continuing
to accept authenticated presence announces.

## Scope

- Define a bootstrap representation for one or more allocator destination hashes. Keep
  the realm secret separate from public routing hints.
- Persist the validated bootstrap set in cluster state or maintain it in the long-lived authority
  broker with an explicit durable source; an in-memory cache populated only by a future announce is
  insufficient for a fresh broker.
- Seed each broker-created run endpoint/controller with the known destinations before
  `requestAttempt` starts its offer timer.
- Send the initial execution request to every compatible known destination and continue collecting
  new `Discoveries()` until `offerWait` so additional allocators still participate.
- Bound destination count, validate 16-byte hashes, deduplicate entries, and ensure stale or
  unreachable hints do not suppress newly announced candidates.
- Preserve authenticated realm proof, allocator capacity checks, deterministic offer selection,
  release of unselected offers, and rescheduling semantics.
- Keep foreground and detached startup on the same single-`Start` lifecycle.

## Acceptance

- An integration test starts an allocator with a long announce interval, lets its startup announce
  pass, then starts a fresh broker and run. Assignment completes without a daemon/allocator restart
  and without a second announce.
- The same scenario passes for foreground and detached runs, with the transport started exactly
  once in each run process.
- A stale bootstrap destination times out or fails without preventing a later valid candidate from
  offering; duplicate hints do not duplicate sends or offers.
- A foreign-realm destination never reaches the r1s handler, and serialized sender fields remain
  non-authoritative.
- Live acceptance uses the deployment Reticulum-Go shared daemon and runs both known-destination
  PathRequest and complete request/offer/assign flow.
- `go build ./...`, `go vet ./...`, `go test -race ./...`, and `make check` pass.

## Notes

The public meshbus adapter already performs `RequestPath(destinationHash)` when sending to a known
destination. F25 should consume that behavior rather than add an aspect-based discovery API.

## Implementation

- Credential state stores at most 32 deduplicated 16-byte destination hashes separately
  from its `r1s1:` realm key; allocator startup atomically records its own destination.
- `cluster token <cluster>` emits an `r1s1:` transfer bundle containing the unchanged realm key and
  current public hints. `cluster join` accepts `r1s1:` tokens and merges validated hints.
- The authority-broker handshake gives every fresh endpoint the durable set. The public direct
  client API accepts the same set through `client.Config`.
- Initial bootstrap and newly announced sends run independently inside the offer window. Duplicate
  destinations are sent once; stale send failures do not end collection. Authenticated remote
  rejections remain observable if no allocator offers.
- Deterministic tests cover bounds and validation, broker propagation,
  no-announce bootstrap, stale-plus-later-discovery behavior, deduplication, and a single transport
  start. Existing RNS tests retain the foreign-realm and sender-authority coverage.

### Live verification

Verified on `root@mytecor-homelab.local` (NixOS Linux x86_64) on 2026-10-04 against the platform
shared Reticulum-Go daemon (`rnsd`/`rns-server` 0.3.3 listening on abstract socket `@rns/default`):
Go 1.27.1, containerd 2.3.4, digest-pinned Alpine fixture
`docker.io/library/alpine@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce`.

- The full `./internal/acceptance/` live suite ran with `RUN_PARTITION_RECOVERY=1`, `-race -count=1 -v`,
  and no skipped gates: `TestLiveResourceLimitsEnforced` (18.53s), `TestLiveRetainedLogsSurviveRestart`
  (6.39s), `TestLiveIdentityQuotaEnforced` (5.93s), `TestLiveRejectionUnderLossNoDuplicateExecution`
  (5.56s), `TestLiveSweepCrashPreservesCapacityAndAuthority` (18.60s), and
  `TestPartitionRecovery` (20.78s), total 76.826s.
- `TestPartitionRecovery` drives the complete known-destination request/offer/assign flow over the
  shared daemon: every control send performs a known-destination `RequestPath` (observed in the
  transcript as `shared-instance client path request` diagnostics), and the request/offer/select/
  assign/lease/reschedule matrix completes against a real containerd task with no transport restart.
- No runner customization was needed beyond pointing `CC` at the host's nix-store GCC 15.3.0 for the
  race detector; the shared instance and containerd were already provisioned on the host.

Deterministic gates continue to cover the bootstrap-only scenarios that the shared-daemon suite cannot
perturb safely (no-announce bootstrap, stale-plus-later-discovery, deduplication, single `Start`) and
remain the authoritative proof for those acceptance bullets; the live run proves the deployment-mode
PathRequest and complete flow against the real shared daemon.

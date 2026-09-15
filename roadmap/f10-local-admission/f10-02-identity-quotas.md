# F10-02 — Authenticated admission and quotas

**Status:** ✅ Implemented; admission acceptance tests and live Linux enforcement pass

## Outcome

An allocator controls which clients can reserve capacity and how much they can reserve.

## Scope

- Add local identity admission policy and per-identity outstanding-offer and active-execution quotas.
- Use only the transport-authenticated identity; account transactionally across release, expiry, completion, and restart.
- Return explicit rejections and document trusted single-user defaults.

## Acceptance

- One client cannot exhaust another client's configured share.
- Spoofed payload identity cannot bypass quotas.
- Concurrent requests and repeated cleanup never over-allocate or double-release quota.
- Run `make check` and the feature-specific checks described above.

## Verification status

Covered by `internal/allocator/acceptance_test.go` (`TestAdmissionAllowlistAndIdentityQuotas`):

- A spoofed payload identity cannot bypass the transport-authenticated sender allowlist.
- Per-identity offer quotas are enforced; one client's quota cannot be exhausted by another, and
  over-quota requests return an explicit `CAPACITY` error.

Live verification (2026-09-15, `mytecor-homelab`, containerd 2.3.4 / runc 1.4.3 / Go 1.26.7) is covered by
`internal/acceptance/live_quota_replay_test.go` (`TestLiveIdentityQuotaEnforced`): a real `r1sd`
running a per-identity quota (1 offer / 1 execution at capacity 2) admits the first request, rejects an
over-quota request with a correlated `CAPACITY` code without starting work, enforces the execution
quota for a real running workload so a second assignment is refused and never creates a second
container, and — under repeated cleanup — replays the rejected assignment (still `CAPACITY`, no
allocation) and an idempotent explicit release (no double-release), then restarts with consistent
quota accounting and admits a fresh request. Two-client isolation and spoofed-payload authority stay
deterministically covered by `TestAdmissionAllowlistAndIdentityQuotas`.


## Notes

Track unresolved cross-feature decisions in [BACKLOG.md](../BACKLOG.md).

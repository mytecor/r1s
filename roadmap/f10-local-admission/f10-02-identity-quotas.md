# F10-02 — Authenticated admission and quotas

**Status:** 🚧 Implemented; acceptance tests and live Linux enforcement pending

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

## Notes

Track unresolved cross-feature decisions in [BACKLOG.md](../BACKLOG.md).

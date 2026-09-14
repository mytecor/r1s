# F9-02 — Explicit authenticated log retrieval

**Status:** 🚧 Implemented; direct acceptance tests and live Linux restart pending

## Outcome

An execution owner can separately request a bounded range of locally retained logs.

## Scope

- Add a separate explicit logs command and authenticated request with stream, offset, and maximum byte count.
- Return missing, expired, truncated, and end-of-stream states explicitly.
- Select the requested-transfer mechanism and checksum contract before implementing it.
- Resume requires another explicit request; no automatic transfer after reconnection.

## Acceptance

- Without a log request, no log bytes cross RNS.
- The owner can retrieve a requested range after failure and restart.
- Another identity is denied, offsets are validated, and the byte limit is enforced.
- An interrupted retrieval does not alter execution lifetime.
- Run `make check` and the feature-specific checks described above.

## Notes

Track unresolved cross-feature decisions in [BACKLOG.md](../BACKLOG.md).

# F9-02 — Explicit authenticated log retrieval

**Status:** ✅ Implemented; unit acceptance tests pass; live Linux restart verified on `mytecor-homelab` (2026-09-15)

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

## Verification status

Covered by `internal/client/acceptance_test.go` (`TestExplicitLogsRequestAndOwnerAuthorization`) and
`internal/allocator/acceptance_test.go` (`TestLogsOnlyByExplicitOwnerRequest`),
`internal/protocol/feedback_test.go` (`TestLogsRequestAndResponseValidation`,
`TestLogsResponseRangeMustMatchBytes`):

- A logs command is explicit and bounded (1..128 bytes, one stream, offset); no log bytes cross RNS
  unless the owner asks.
- Only the authenticated execution owner can retrieve; a foreign identity is denied without reading
  the store or touching state.
- Responses are validated for contiguous range and matching checksum; the byte limit is enforced.


## Notes

Track unresolved cross-feature decisions in [BACKLOG.md](../BACKLOG.md).

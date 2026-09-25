# F22-05 — Foreground and detached log continuity

**Status:** ⏳ Planned

## Outcome

Foreground runs continuously copy explicitly requested allocator-local stdout/stderr to the matching
terminal streams. Detached runs keep the same lease-holding process alive and append all attempts to
one local output file without introducing a client log database.

## Scope

- Keep `ExecutionLogsRequest`/`ExecutionLogsResponse` as an internal authenticated pull mechanism;
  never push logs automatically on completion, failure, inspection, result retrieval, or reconnect.
- Tail by byte offset so a temporary partition can be recovered from allocator-local log storage.
- Increase the protocol/request chunk cap from the current tiny default to a bounded 16–64 KiB
  implementation choice, preserving memory and message limits.
- In foreground mode, preserve stdout/stderr separation while the run loop owns the terminal.
- Implement `-d` with a short parent/child handshake. The parent prints the run ID, PID, and log
  path only after the child has safely taken ownership.
- Store minimal runtime files under `~/.local/state/r1s/runs/<run-id>/`: `pid` while active and
  `output.log` after creation. `--log-file` overrides only the output location.
- Append explicit attempt-boundary records such as `[r1s] rescheduled attempt=2` to the same file;
  do not copy cluster secrets or secret workload fields into runtime state.
- Remove the PID marker on clean exit; retain the output file.

## Acceptance

- Bytes produced during a temporary RNS partition are fetched by offset after reconnect with no
  duplicate or missing bytes within allocator retention limits.
- Rescheduling appends the new attempt to the same detached file with a bounded service marker.
- Parent failure before the child handshake never reports a live run; successful detach reports
  paths that exist with owner-only permissions.
- Log retrieval is rejected for a non-owner, and no completion/inspection path includes log bytes.
- Log chunks are bounded and tests cover stdout/stderr, partial reads, truncation, reconnect, and
  allocator restart.
- `go test -race ./...` and `make check` pass.

## Notes

- A later `r1s stop <run-id>` may use the PID marker, but it is not part of F22.

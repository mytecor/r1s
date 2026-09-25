# F22-05 — Foreground and detached log continuity

**Status:** ✅ Complete

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

## Implemented

- The protocol log chunk cap rose from 128 to 64 KiB (`protocol.MaxLogBytes`), an implementation
  choice inside the 16–64 KiB range that stays far below the bounded-memory envelope invariant.
  Validation messages and CLI help now read `1..65536 bytes`.
- `r1s run` spawns a foreground tail loop (`run_tail.go`) that pulls bounded chunks by byte offset
  for both `stdout` and `stderr` and writes them to the matching terminal streams, so workload
  stdout/stderr separation survives while the run loop owns the terminal. Run-control lines move
  to stderr. A partition, timeout, or expired retention is never rescheduling evidence from the
  tail; offsets stay put, so reconnect drains exactly the gap with no duplicate or missing bytes.
- `r1s run -d [--log-file path]` detaches: the parent re-executes this binary as the lease-holding
  child (never building its own RNS node), waits for a short ownership handshake over an inherited
  pipe, and only then prints `run=... pid=... log=...`. A child that exits before the handshake is
  reported as a failed detach, never a live run.
- The child writes its PID marker and opens the output file under `~/.local/state/r1s/runs/<run-id>/`
  (or the `--log-file` override), signals ownership only after those paths exist with owner-only
  permissions, and removes the PID marker on clean exit while retaining the output log.
- Rescheduling appends a bounded `[r1s] rescheduled attempt=N` service marker to the same detached
  file; the start marker is `[r1s] run=<run-id> attempt=1`. No cluster secret or secret workload
  field is copied into runtime state.
- No completion, inspection, result, or reconnect path attaches or auto-sends log bytes; retrieval
  stays an explicit authenticated pull, and the allocator's owner check and logstore bounds are
  unchanged.
- Tests cover stream separation, offset recovery across a simulated partition, bounded chunk
  requests, reschedule markers with offset reset, stale-chunk dropping after reschedule, the
  detached PID marker lifecycle, owner-only path verification, and parent handshake failure/
  success orchestration.

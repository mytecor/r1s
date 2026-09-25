# F22-07 — Legacy client removal and allocator-local retention

**Status:** ⏳ Planned

## Outcome

After the run path reaches feature parity, the legacy local client control plane and CRUD-oriented
CLI are removed. Allocator durability remains the sole execution database, and retention becomes an
allocator-owned operational policy rather than workload input.

## Scope

- Remove the public `request`, `serve`, `list`, `inspect`, `result`, `cancel`, `logs`, and `tunnel`
  commands after their run-loop replacements land.
- Remove `--socket`, `--state`, `--identity`, `--rns-config`, `--keep-alive`, and `--allocator` from
  client UX; remove the local gRPC server/client and durable client state, watch journal, lease
  intents, and rebind/recovery machinery that only support those surfaces.
- Remove `result_retention` from new workload input and configure terminal-record retention on
  `r1sd`. Preserve replay-safe tombstones for at least the command horizon.
- Keep allocator bbolt state, active executions, leases, offers/reservations, ownership, resource
  accounting, replay records, terminal records, log metadata, and containerd reattachment.
- Keep wire `Inspect`, `Cancel`, `OfferRelease`, lease, and log operations that the run engine uses;
  a removed CLI command does not imply a removed protocol primitive.
- Preserve Protobuf numbers and reserve removed names/numbers, but do not implement compatibility
  shims, legacy request handling, old state migration, or dual old/new operating modes.
- Update [README.md](../../README.md), [ARCHITECTURE.md](../../ARCHITECTURE.md), help snapshots, and
  completed feature docs to distinguish historical surfaces from the F22 interface.

## Acceptance

- `r1s --help` contains only `cluster`, `run`, and non-workflow utility commands intentionally kept.
- No production code opens a local client command socket or persists client execution/lease state.
- Allocator restart still reattaches active containerd tasks and preserves lease, replay, tombstone,
  retention, and log invariants.
- A workload cannot choose allocator bookkeeping retention; operator configuration is bounded by
  the replay-safety minimum.
- Repository docs contain no current-usage examples for removed commands/flags, while historical
  feature records remain clearly marked as superseded.
- Legacy client state and cluster files are neither read nor rewritten by the new binaries.
- `go build ./...`, `go vet ./...`, `go test -race ./...`, and `make check` pass.

## Notes

- Cleanup is last so intermediate commits remain testable; the release boundary itself is a clean,
  intentionally incompatible cutover.

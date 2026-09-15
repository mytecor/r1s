# F15-01 — Implement the HTTP artifact service

**Status:** ⏳ Planned

## Outcome

Provide bounded HTTP upload and download operations that consume F14 capabilities and publish only
verified artifacts.

## Scope

- Implement staged upload, digest verification, atomic publication, download, and cleanup.
- Enforce capability scope before accepting or returning bytes.
- Keep storage and HTTP behavior behind interfaces usable by deterministic tests.
- Do not implement an OCI registry or custom image-transfer protocol.

## Acceptance

- Tests cover successful upload/download, interruption, retry, expiry, overrun, mismatch, and cleanup.
- A failed or partial upload is never visible as a valid artifact.
- Execution lifetime remains independent of the transfer connection.
- `make check` passes.

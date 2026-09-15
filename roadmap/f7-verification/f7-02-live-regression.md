# F7-02 — Regular live regression

**Status:** ✅ Complete; procedure documented 2026-09-15, live suite green on `mytecor-homelab`

## Outcome

A repeatable Linux run verifies Python RNS interoperability and real containerd partition recovery, all
without adding a hosted job to GitHub Actions: `mytecor-homelab` is a development host, not a CI
dependency, so every run is performed and recorded manually at a known commit.

## Scope

- Provide an isolated Linux runner, digest-pinned fixture, and pinned Python RNS environment.
- Run the existing gated lifecycle, recovery, Python Channel, and partition harnesses.
- Retain diagnostic metadata; do not introduce automatic workload-log transfer over RNS.

## Acceptance

- All live gates run without skips on the configured runner.
- Failures retain enough diagnostics to reproduce the scenario.
- Run `make check` and the feature-specific checks described above.

## Procedure

The live acceptance is a manual, repeatable job, run on a Linux host with a reachable containerd
daemon and recorded in the roadmap as it passes. It is deliberately *not* a GitHub Actions job:
`mytecor-homelab` is a private development host and is not exposed to the shared CI runner pool.

### Prerequisites (once per host)

- Linux (`x86_64`/`aarch64`) with a running containerd daemon
  (`systemctl status containerd`). A non-default socket is selected with
  `CONTAINERD_ADDRESS`; a non-default snapshotter with `CONTAINERD_SNAPSHOTTER`.
- Go toolchain (the version is recorded per run; 1.26.7 on 2026-09-15).
- A Python interpreter that can `import RNS` (the upstream Python reference), e.g. a
  `rns` pipx venv. It is selected via `PYTHON_INTEROP`.
- A digest-pinned fixture image, currently
  `docker.io/library/alpine@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce`
  (fixed in [F3-01](../f3-oci-runtime/f3-01-containerd-adapter.md)). Set
  `R1S_CONTAINERD_TEST_IMAGE` to it so every gate exercises the same fixture.

### Run (in order)

Each gate is named explicitly so a skipped test becomes a hard failure instead of a silent gap.
Run every gate below with `-race -count=1` and the `-v` flag so per-step progress and the identity
of each skipped test are visible in the transcript.

1. Containerd adapter lifecycle + recovery (`F3`):

   ```sh
   RUN_CONTAINERD_INTEGRATION=1 \
   R1S_CONTAINERD_TEST_IMAGE='docker.io/library/alpine@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce' \
   go test -race -count=1 -v ./internal/runtime/containerd/
   ```

2. Python-reference interoperability, discovery + reliable Channel (`F2`/`F12`):

   ```sh
   RUN_LIVE_INTEROP=1 \
   PYTHON_INTEROP="$HOME/.local/pipx/venvs/rns/bin/python" \
   go test -race -count=1 -run 'TestPythonReference(Discovery|ChannelEnvelope)' -v ./internal/transport/rns/
   ```

   The Channel leg injects channel-packet loss and asserts the Go client recovers; the echoed
   envelope sender is the authenticated Python node identity. See
   [F12-01](../f12-cluster-membership/f12-01-cluster-authentication.md).

3. Partition recovery + the F8–F11 live legs (`F5`, `F6`, `F8`, `F9`, `F10`, `F11`), one `go test`
   invocation so the whole suite runs together as it did on 2026-09-15:

   ```sh
   RUN_PARTITION_RECOVERY=1 \
   R1S_CONTAINERD_TEST_IMAGE='docker.io/library/alpine@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce' \
   go test -race -count=1 -v ./internal/acceptance/
   ```

   The acceptance package contains the complete live matrix:

   - `TestPartitionRecovery` — replay safety, offline survival, restart reconciliation, result
     recovery, duplicate cancellation (F5).
   - `TestLiveRetainedLogsSurviveRestart` — F9 logs survive a real daemon restart.
   - `TestLiveResourceLimitsEnforced` — F10 OCI memory/CPU/pids limits kept across restart.
   - `TestLiveIdentityQuotaEnforced` — F10 per-identity admission quota, explicit release,
     restart consistency.
   - `TestLiveRejectionUnderLossNoDuplicateExecution` — F8 rejection in flight never becomes an
     execution (also under message loss).
   - `TestLiveSweepCrashPreservesCapacityAndAuthority` — F11 SIGKILL during the sweep window;
     collected assignments stay `EXPIRED` and capacity is freed.

4. `make check` (protoc 36.0, lychee, `generate-check`, `go test -race ./...`).

### Record the result

Record each successful run in the roadmap exactly as the 2026-09-15 run is recorded: host
(`mytecor-homelab`), containerd/runc/Go versions, the pinned fixture digest, the `git rev-parse
--short HEAD`, the runtime/link of each live gate, and any skipped tests. Link the F6/F8/F9/F10/F11
task and feature files that assert live verification. A run that skips a live gate is not a pass.

## Implementation

There is no hosted runner or new script: the live gates are pre-existing acceptance tests that
self-skip unless their `RUN_*` gate is set, and this task records the exact manual procedure that
makes them run together without skips on an isolated Linux host. The digest-pinned fixture and
pinned Python RNS environment are described in the prerequisites; diagnostics are retained by the
per-run transcript and by the `-v` output of every gate. Logs remain allocator-local: the gates
read retained logstore files on disk only after an explicit owner request and never transfer
workload logs over RNS.

## Verification

- All live gates above run without skips on the configured host (2026-09-15 run: containerd 2.3.4 /
  runc 1.4.3 / Go 1.26.7 / digest-pinned Alpine fixture; whole `-race` suite green).
- Failures retain diagnostics: each `go test -v` transcript names the failing gate, and the
  acceptance tests embed per-step state and allocator stderr on failure.
- `make check` passes (protoc 36.0, lychee 0.24.2, go1.27.1).

## Notes

Track unresolved cross-feature decisions in [BACKLOG.md](../BACKLOG.md).

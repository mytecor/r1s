# F22-03 — Multi-cluster credential store and explicit selection

**Status:** ⏳ Planned

## Outcome

Cluster credentials remain local trust material, separate from workload manifests. Users may keep
credentials for several clusters, while every `run` and allocator startup names exactly one cluster.

## Scope

- Replace the single default cluster file with a credential directory such as
  `~/.config/r1s/clusters/<cluster-id>` containing the secret key indexed by its derived public ID.
- Keep `r1s cluster init` and `r1s cluster join <token>`, and add `r1s cluster list`.
- Make the run grammar `r1s run <cluster> [options] <workload>` with no implicit default cluster.
- Accept a unique cluster-ID prefix for convenience; reject missing and ambiguous prefixes.
- Do not accept a join token as the `run` cluster argument, so secrets do not become normal argv or
  shell-history material.
- Start an allocator as `r1sd <cluster>`; one allocator process authenticates to one cluster.
  Multi-cluster allocation requires a future shared capacity manager and is out of scope.
- Keep cluster ID/key out of workload data and `ExecutionRequest`; cluster membership remains a
  transport-boundary concern.
- Do not migrate or import the existing single cluster file automatically; users initialize or join
  credentials in the new store explicitly.

## Acceptance

- Init/join store credentials under the full derived ID, list never prints secret material, and
  duplicate imports are idempotent.
- `run` fails before network activity when the cluster is absent or ambiguous.
- The same workload file can be run unchanged against two selected clusters.
- `r1sd` rejects missing or multiple cluster operands.
- An existing legacy cluster file is ignored and never selected as an implicit default.
- File permissions and atomic writes meet the existing cluster-secret protections.
- `go test -race ./...` and `make check` pass.

## Notes

- Cluster is where a workload is sent, not part of what the workload is.

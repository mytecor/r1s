# F12-01 — Cluster bootstrap and transport authentication

**Status:** ✅ Complete

## Outcome

`r1s` and `r1sd` can create or join a cluster with one secret token, and the RNS transport rejects
control traffic from peers that cannot prove possession of that secret.

## Scope

- Add `cluster init`, `cluster join`, and non-secret `cluster show` commands to both binaries.
- Store one versioned cluster key with owner-only file permissions. Both binaries default to
  `~/.config/r1s/cluster` for the current OS account and accept a custom path through `--cluster`.
- Accept an inline `r1s1:<secret>` value through `--cluster` for runtime commands without persisting
  it; first load the value as a state file, then fall back to token parsing only when the file does
  not exist. Cluster management commands always treat the option as a state file path.
- Encode join tokens as `r1s1:<base64url-key>` and derive
  `SHA-256("r1s-cluster-id-v1" || ClusterKey)` as the public cluster ID.
- Include only the public cluster ID in allocator descriptors and filter foreign descriptors.
- Run a mutual per-link HMAC challenge-response over RNS Channel messages. Each proof binds the
  nonce, challenger RNS identity, and responder RNS identity with the `r1s-auth-v1` domain.
- Gate every protobuf envelope on successful cluster authentication while continuing to replace
  its serialized sender with the transport-authenticated RNS identity.

## Acceptance

- `go test ./...`
- `RUN_LIVE_INTEROP=1 go test ./internal/transport/rns/ -run 'TestPythonReference(Discovery|ChannelEnvelope)' -count=1 -v`
- `make check`

## Notes

All members currently have the same cluster-level authority. Per-identity allocator admission from
[F10](../f10-local-admission/README.md) can narrow that authority locally. Individual membership
revocation and safe key rotation are deferred in [BACKLOG.md](../BACKLOG.md).

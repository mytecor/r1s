# F24-09 — Remove r1s identity and stack policy from meshbus

**Status:** ✅ Complete

## Outcome

`meshbus/rns` exposes generic identity and Reticulum stack configuration. r1s owns its decision to
require a shared-instance client and its use of RNS identity material for execution tunnels.

## Scope

- Remove the F14 tunnel-key helper and roadmap terminology from `meshbus/rns`.
- Keep tunnel identity derivation in the r1s layer.
- Make shared-client and standalone RNS modes explicit generic adapter choices.
- Keep r1s configured to fail closed when its required shared instance is unavailable.
- Forward all generic Node lifecycle and directory bounds through `rns.NewNode`.

## Acceptance

- No meshbus source comment or exported helper refers to r1s features or roadmap IDs.
- Standalone RNS is a supported adapter mode rather than a test-only exception.
- r1s retains the same identity persistence and shared-instance behavior.
- `make check` passes.

## Implementation notes

The F14 seed helper now belongs to `internal/transport/rns`. The public adapter exposes explicit
`StackSharedClient` and `StackStandalone` modes, while the r1s adapter continues to select shared
client mode in production. `rns.NewNode` forwards directory, peer lifetime, sweep, and peer-error
configuration to the generic Node.

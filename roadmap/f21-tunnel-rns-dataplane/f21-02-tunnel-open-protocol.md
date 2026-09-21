# F21-02 — Control-plane advertisement and tunnel Open protocol

**Status:** ⏳ Planned

## Outcome

The control plane advertises the minimum needed to create a private tunnel transport, and the first
application message on an identified tunnel Link is an explicit `Open { execution_id, port }`,
replacing the minted-grant flow entirely.

## Scope

- **Advertisement through the control plane**: `r1sd` advertises `[ygg-ipv6]:port` (the allocator's
  tunnel Backbone/TCP listener) plus the *tunnel RNS destination hash* — and never an Ygg public
  key. If the remote RNS identity can be reached safely via the stock announce/path flow inside the
  Backbone interface, do not transfer the RNS public key by hand. Update the control-plane
  protocol/descriptor types that today carry `ExecutionTunnelGrantAck.allocator_endpoint[_pubkey]`.
- **New Open/result messages**: replace grant messages with tunnel-open protocol:
  `Open { execution_id, port }` as the first application message after Link establishment and
  identification, and a reply of `OK` or a classified error (mirrored in
  [`LocalTunnelClose.Reason`](../../api/proto/r1s/v1/local.proto)). After `OK` the byte stream
  begins; there is no preamble, no grant ID, no single-use token.
- **Protocol cleanup, direct (no back-compat)**: delete `ExecutionTunnelGrant`,
  `ExecutionTunnelGrantAck`, the `ygg_peer_pubkey` field, grant ID/TTL and the preamble from
  [`control.proto`](../../api/proto/r1s/v1/control.proto) and
  [`local.proto`](../../api/proto/r1s/v1/local.proto); rework `LocalTunnelOpen` to carry
  `execution_id` + `target_port` only, with no grant-carried target list. Remove
  `Registry.Mint`/`Accept`, grant fields, and `Preamble{ExecutionID, GrantID}` from the core.
- **No backward compatibility is kept**: field numbers and names may be cleaned directly.
  Removed grant/ygg fields are deleted outright, not reserved; surviving messages are renumbered if
  that is the simplest representation. This is an explicit, deliberate exception to the
  renumber/reserve rule in [CONTRIBUTING.md](../../CONTRIBUTING.md), recorded as such because the
  tunnel switches to a new identity/Open scheme with no supported in-flight users.

## Acceptance

- Control-plane advertisement carries the allocator Ygg IPv6 `host:port` and the tunnel RNS
  destination hash; no `ygg_peer_pubkey`/`allocator_endpoint_pubkey` remains anywhere in the
  control protocol.
- First message after identification is `Open{execution_id, port}`; the allocator responds `OK` or a
  classified error; a stream is not spliced before `OK`.
- `Registry.Mint`/`Accept`, grant messages, preamble types, and grant fields are gone from the
  build; tests that referenced them are migrated to the Open flow.
- `go generate` output is regenerated and `make check` (including `generate-check`) passes.

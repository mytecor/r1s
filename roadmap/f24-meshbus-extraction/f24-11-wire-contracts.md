# F24-11 — Stabilize and harden the meshbus wire contracts

**Status:** ✅ Complete

## Outcome

Realm derivation, realm authentication, RNS Channel messages, presence, and event frames have an
explicit compatibility contract that can be versioned independently from r1s.

## Scope

- Document the version markers, domain separators, integer encoding, bounds, and sender-authority rules.
- Add golden vectors for realm IDs/proofs, presence, authentication frames, and event frames.
- Fuzz every decoder that accepts untrusted network bytes.
- State the pre-v1 compatibility policy and the process for introducing a new wire version.

## Acceptance

- Another implementation can produce and consume the documented frames without importing r1s.
- Golden tests detect accidental byte changes.
- Fuzz tests reject malformed input without panics or unbounded allocation.
- `make check` passes.

## Implementation notes

[`WIRE.md`](https://github.com/mytecor/meshbus/blob/v0.1.0/WIRE.md) specifies realm, authentication, Channel, presence, and event
formats. Realm, authentication, and event golden vectors pin bytes; event, presence, and
authentication decoders have fuzz targets. The public presence codec supports independent
implementations without exposing adapter internals.

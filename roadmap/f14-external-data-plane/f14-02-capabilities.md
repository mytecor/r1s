# F14-02 — Define data-access capabilities

**Status:** ⏳ Planned

## Outcome

An authenticated allocator can authorize exactly one bounded class of data-plane action without
making routability, an endpoint address, or the cluster key sufficient for access.

## Scope

- Issue capabilities only through an authenticated RNS control exchange.
- Bind each capability to execution ID, transfer direction, artifact digest, byte ceiling, and expiry.
- Define replay, renewal, revocation, clock-skew, and secret-redaction behavior.
- Keep cluster-key verification at the RNS boundary; do not require the data service to accept the
  cluster key as a bearer credential.

## Acceptance

- Tests reject the wrong execution, direction, digest, byte count, issuer, and expired capability.
- Capability reuse follows one documented rule and remains safe across allocator restart.
- Secrets never appear in descriptors, diagnostics, metrics, or durable records unless explicitly
  required by the chosen renewal design.
- `make check` passes.

## Notes

The public data network is treated as untrusted even when its links are encrypted. Transport
encryption complements, but never replaces, capability checks and operator network policy.

# F25. Deterministic allocator bootstrap

Corresponds to [F25 in the roadmap](../../ROADMAP.md#f25-deterministic-allocator-bootstrap).

**Status:** ✅ Complete — F25-01 implemented and live shared-daemon acceptance verified on `mytecor-homelab` (2026-10-04)

## Outcome

A new run receives one or more authenticated bootstrap allocator destinations without waiting for
the next periodic presence announce. Announces remain advisory discovery and topology-repair input;
their timing is no longer a prerequisite for the first execution request.

## Dependencies

- [F22](../f22-rns-shared-instance/README.md) — per-run broker endpoints and the run engine.
- [F24](../f24-meshbus-extraction/README.md) — known-destination PathRequest, authenticated direct
  messages, and the external meshbus boundary.

## Non-goals

- Service discovery by RNS aspect: a PathRequest requires an already-known destination hash.
- A global scheduler, allocator registry service, or cluster-wide source of truth.
- A second `Start` call in the detached child; the common command path already starts its transport.
- Depending on a short allocator announce interval for correctness.

## Tasks

- [F25-01 — Bootstrap known allocator destinations](./f25-01-bootstrap-destinations.md)

## Completion criteria

- A fresh foreground or detached run reaches an already-running allocator whose last presence
  announce predates `offerWait`, without restarting the allocator or waiting for another announce.
- Stale bootstrap destinations fail independently while fresh announces can still add candidates.
- The behavior passes against the full Reticulum-Go shared daemon used by deployment, not only an
  in-process transport double.

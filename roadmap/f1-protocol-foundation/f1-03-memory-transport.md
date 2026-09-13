# F1-03 — Add deterministic transport

**Status:** Complete

## Outcome

Implement an in-memory transport that delivers cloned Protobuf envelopes between named endpoints
for tests and future integration harnesses.

## Acceptance

- A message sent to a registered destination arrives with the authenticated source endpoint.
- Unknown and closed destinations produce explicit errors.

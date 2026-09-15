# F15-02 — Verify Yggdrasil deployment and isolation

**Status:** ⏳ Planned

## Outcome

Document and prove that the generic artifact service works over Yggdrasil IPv6 without coupling the
core protocol to Yggdrasil.

## Scope

- Add a gated live two-peer Yggdrasil transfer harness.
- Document endpoint configuration, TLS expectations, firewall rules, and operator-run registry use.
- Verify RNS discovery and capability issuance independently from Yggdrasil reachability.

## Acceptance

- The live harness moves verified inputs and outputs over Yggdrasil and records its environment.
- A peer with network reachability but no valid capability receives no artifact bytes.
- Losing Yggdrasil reachability affects only data transfer, not execution lifecycle or RNS control.
- The deterministic suite and `make check` pass.

## Notes

An OCI registry reachable over Yggdrasil is an operator deployment option. containerd, not r1s,
speaks the OCI Distribution protocol.

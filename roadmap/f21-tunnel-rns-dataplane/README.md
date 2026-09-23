# F21. Evaluate a private-RNS tunnel data plane

Corresponds to the F21 milestone in [ROADMAP.md](../../ROADMAP.md).

**Status:** 🔄 No-go recorded — F21-05 demonstrated that a Python-compatible RNS
`Link`/`Channel`/`Buffer` stream is not suitable for the execution-tunnel data plane. F21-06 now
rolls back the experimental private-RNS path and retains the embedded Yggdrasil tunnel implemented
by F19/F20.

## Decision

The experiment keeps the architecture's original transport boundary:

- **RNS remains the control plane** for discovery, authenticated commands, lifecycle, and tunnel
  grant exchange.
- **Yggdrasil remains the tunnel data plane** carrying SSH, HTTP, and arbitrary application bytes.
- The private RNS-over-system-Ygg transport under [`internal/tunnel/rns`](../../internal/tunnel/rns)
  is benchmark evidence and temporary code only; it does not replace the working
  [`internal/tunnel/yggdrasil`](../../internal/tunnel/yggdrasil) adapter.
- F21-06 does **not** remove `yggdrasil-go`, Ironwood, tunnel grants, the authenticated mesh pair,
  or its multiplexed streams. It removes or reverts the experimental RNS tunnel integration while
  preserving the F19/F20 user-facing and container-isolation contracts.

The deciding F21-05 rows are 4.80 MB/s for a 10 MiB RNS single stream versus 159.25 MB/s for Ygg,
and 5.70 MB/s versus 227.16 MB/s for 10 concurrent streams. Disabling automatic bzip2 and reducing
the Go implementation's 5ms readiness poll improved the prototype substantially but did not change
the decision. The remaining gap is dominated by protocol-compatible RNS behavior: a 423-byte
stream payload, a maximum Channel window of 48, an explicit signed proof for every Channel packet,
IFAC work, and one small Backbone write per frame. Those costs also constrain interoperability with
Python RNS and worsen with RTT; they are not a temporary application-level tuning problem.

This no-go does not abandon the upstream Reticulum-Go fixes found during the experiment. Issues
[#17](https://github.com/Quad4-Software/Reticulum-Go/issues/17),
[#18](https://github.com/Quad4-Software/Reticulum-Go/issues/18), and
[#19](https://github.com/Quad4-Software/Reticulum-Go/issues/19) remain useful for RNS control-plane
and general Buffer correctness/performance, but F21 no longer depends on them for tunnel throughput.

## Preserved contracts

- `r1s tunnel <execution> --port <host>:<container>` remains service-backed and process-attached.
- The authenticated execution owner is the only principal allowed to open the tunnel.
- Client-selected ports are validated and dialled only inside the selected execution's network
  namespace; the allocator host namespace is never a fallback.
- Concurrent local TCP connections remain independent streams on the authenticated Ygg mesh pair.
- Closing the tunnel does not affect execution lifetime, which remains controlled by the durable
  client-held lease.
- Container stdout/stderr remains allocator-local and is never attached to tunnel teardown.

## Follow-up boundary

Replacing the embedded Ygg library with raw TCP over a system Ygg interface may be evaluated later,
but only as a separate data-plane design with an explicit authentication/capability protocol and
its own benchmark. It must not route application bytes through RNS `Channel`/`Buffer`, and it is not
part of this rollback.

## Tasks

- [F21-01 — Private tunnel RNS stack prototype](./f21-01-tunnel-rns-stack.md) — experimental,
  benchmark-only.
- [F21-02 — RNS tunnel Open protocol prototype](./f21-02-tunnel-open-protocol.md) — partial slice
  landed; production switch cancelled.
- [F21-03 — Allocator authorization and container splice](./f21-03-allocator-auth-splice.md) —
  cancelled.
- [F21-04 — Client edge and CLI/config cleanup](./f21-04-client-edge-cli-cleanup.md) — cancelled.
- [F21-05 — Benchmark and live acceptance](./f21-05-benchmark-live-acceptance.md) — complete,
  recorded no-go.
- [F21-06 — Roll back private RNS tunnel and retain Ygg](./f21-06-remove-ygg-adapter.md) — planned.

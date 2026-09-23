# Open decisions and deferred work

This file records unresolved choices so they do not remain implicit in implementation code.

## Open decisions

1. **External data plane and application artifacts** — whether and how bulk application data is
   transferred between client and allocator is deliberately undecided and out of scope for now; it
   may never become part of the system. RNS stays the discovery, identity, and control plane and is
   not a bulk-transfer path. stdout/stderr stays local and requires the explicit owner request
   established by [F9](./f9-local-logs/README.md). If a data plane is designed later, it must
   define URI scheme, authorization, transfer semantics, byte limits, resumability, truncation, and
   any integrity contract from scratch without assuming an artifact model.
2. **Speculative image preparation** — decide whether selected workload classes benefit from
   pulling or preparing an image before assignment. An offer itself remains a capacity lease and
   does not authorize workload start.
3. **Identity storage** — define secure creation, persistence, rotation, backup, and per-service
   identity boundaries.

4. **History retention and replay horizon defaults** — pinned with [F11](./f11-state-retention/README.md)
   completion: command replay horizon `CommandHorizon` = 7 days, default result retention = 24 hours
   (`DefaultRetention`, bounded by the horizon), tombstone lifetime = horizon, `defaultReplayTTL` = 10
   minutes, `defaultReplayCapacity` = 4096 commands, durable record budget
   `DefaultMaxRecords` = 10000, and a configurable `--sweep-interval` (default 1 minute) driving
   bounded-history cleanup. Expired results answer an explicit `EXPIRED` error and can never restart
   work. See [F11-01](./f11-state-retention/f11-01-retention-contract.md).
5. **Admission defaults and device profiles** — live enforcement of allocator-owned resource profiles
   and per-identity quotas is verified ([F10](./f10-local-admission/README.md)); trusted-client
   defaults remain open, while advertised device capabilities and placement constraints belong to
   [F16](./f16-node-placement/README.md). GPU ownership and isolation must be defined there before a
   GPU label can authorize device access.
6. **Live regression environment** — the gated Linux acceptance harness is actively run against
   `mytecor-homelab` (containerd 2.3.4 / runc 1.4.3 / Go 1.26.7 / digest-pinned Alpine fixture),
   most recently 2026-09-15 under `go test -race` for the full live suite. The manual, repeatable
   procedure — including the pinned Python RNS environment, the digest-pinned fixture, and the
   exact per-gate commands — is recorded in [F7-02](./f7-verification/f7-02-live-regression.md).
   `mytecor-homelab` is a development host, not a GitHub Actions self-hosted runner.
7. **Cluster membership rotation and revocation** — define an authenticated `cluster rotate`
   workflow, safe distribution of the replacement join token, transition windows for partitioned
   members, and whether individual member revocation warrants moving beyond the shared-key baseline
   established by [F12](./f12-cluster-membership/README.md).
8. **Execution lease parameters** — the direction landed with
   [F17](./f17-execution-lease/README.md): lifetime moved from the request-time
   `deadline`/`max_runtime` (fields now reserved) to a durable, explicitly renewed client-held
   lease, and unrenewed leases evict locally through the existing runtime stop boundary. Chosen
   defaults: an allocator-granted initial lease of 10 minutes, a renewal bound of the 7-day command
   replay horizon, and eviction when the local sweep observes expiry (the sweep interval is the
   implicit grace period, consistent with lazy offer expiry and retention). The terminal marker is
   a `FAILED` state with the stable detail `execution lease expired`, distinct from any
   client-supplied cancellation reason. Still open: clock semantics across allocator restart
   (wall-clock persisted expiry versus monotonic accounting) and whether a strict eviction grace
   period after one missed renewal is worth adding over the lazy sweep.
9. **Tunnel server-side target** — where the F14 direct-access tunnel terminates on the allocator is
   settled with [F14-01](./f14-direct-node-access/f14-01-access-grant.md): the `(host, port)` target
   is resolved **at grant time** from allocator-local configuration — a per-resource-class target
   map plus a mandatory default — and bound into the minted grant. It is never a client-supplied
   destination and never per-execution metadata populated at allocation time. When no target can be
   resolved (no class match and no default), minting fails with a clear `CommandError` instead of
   connecting by guesswork. Target auto-discovery from the running workload is a later improvement,
   out of scope for now.

   **⚠ Superseded by [F20-01](./f20-client-tunnel-targets/f20-01-client-supplied-target-slots.md) and
   then the `--port` model:** the destination source of truth moves from allocator config to the
   `r1s` client. The client supplies the container-port list in the tunnel grant and exposes each
   port under a Docker-style `--port <host>:<container>` mapping; the allocator stops resolving
   targets from its own configuration (`--tunnel-target` / `--tunnel-default-target` are removed)
   and becomes a proxy/splice point to grant-carried container ports. There are no named slots;
   `r1s tunnel <id> --port 8080:80` binds a local listener on `127.0.0.1:8080` and relays to
   container port `80`. Authorization stays owner-only on
   the grant; peer-key pinning and one-live-session-per-execution remain. Resolved by
   [F20-01](./f20-client-tunnel-targets/f20-01-client-supplied-target-slots.md).
10. **Tunnel data plane cannot sustain a >10s single-stream transfer (F21-05)** — found by the
    F21-05 old-vs-new benchmark on 2026-09-23 (macOS loopback, Reticulum-Go v1.2.0): a sustained
    10 MiB single-stream transfer over the private tunnel RNS transport fails with `link not
    ready` at exactly ~10.0s, with the client Link flipping ACTIVE → STALE at that instant while
    the allocator Link stays ACTIVE. Root cause is Reticulum-Go's keepalive/staleness model, not
    the Backbone evWrite race the compat adapter (entry 11) already contains: on a low-RTT link
    `keepalive` floors at `KeepaliveMinSec` (5s) and `staleTime = keepalive * 2` is therefore
    10s; during a one-direction transfer the sender's `lastInbound` ages as fast as the peer's
    Channel ACK/proof traffic refreshes it, which on this loopback path does not stay ahead of
    `staleTime`, so the watchdog CASes the link STALE and `WaitReady` fails the write. Transfers
    that finish inside `staleTime` pass (1 MiB ~2.3s, 4 MiB ~6.7s, 10×1 MiB concurrent ~9.4s);
    the defect is time-based, not size-based. Gated by
    `TestSustainedTransferOutlivesStaleTime` in [`internal/tunnel/rns`](../internal/tunnel/rns)
    (`R1S_TEST_SUSTAINED_TUNNEL=1`), which reproduces at ~10s. Fix directions to evaluate before
    F21-06 (the gate was no-go until then): upstream the keepalive/stale floor in Reticulum-Go (raise
    `KeepaliveMinSec` or scale `staleTime` on low-RTT links), or keep the sender's `lastInbound`
    fresh from its own outbound/delivery receipts — ideally by adding a per-conn keepalive
    workaround in `compat.go` (`newReticulumCompatStream`'s keepalive beacon) that forces the signal the watchdog needs.

    **Resolved (2026-09-23):** the private-tunnel compat adapter now keeps every established Link
    out of the staleness timeout. Each edge runs a liveness beacon (`newReticulumCompatStream` +
    `reticulumCompatStream.keepaliveLoop`) that sends a tiny user-range Channel frame
    (`compatKeepaliveType` = 0x4000, separate from the stream's system type) to the peer every
    `tunnelKeepaliveInterval` (3s, comfortably below the 10s floor). The peer's `Link.HandleInbound`
    receives it as ordinary inbound data and refreshes `lastInboundNs` — precisely the signal the
    watchdog needs — so a one-way writer is kept alive by the idle peer's beats even when its own
    TX window is saturated. Non-blocking (gated on `IsReadyToSend`, never `WaitReady`), does not
    disturb stream byte order (Channel sequence space is shared; the stream reader declines the
    keepalive type), and stops when the Link leaves Active. `TestSustainedTransferOutlivesStaleTime`
    (still env-gated) now passes: after the compatible compression/readiness workarounds make 10
    MiB finish before the stale window, the gate uses 64 MiB and completes byte-for-byte in ~12.7s.
    The test additionally samples the writer Link status throughout and asserts it never leaves
    ACTIVE. The recorded **root cause** is refined from the original note:
    the writer's Channel ACK/proof traffic is validated by `Transport.handleProofPacket` and never
    reaches `Link.HandleInbound`, so `lastInboundNs` sits frozen at establishment while
    `lastOutbound` advances; an upstream fix should refresh `lastInbound` (or treat a validated peer
    proof for outbound data as activity) — see decision 20 and
    [F21-05](./f21-tunnel-rns-dataplane/f21-05-benchmark-live-acceptance.md).

## Resolved

1. **Allocator persistence** — bbolt provides a local, transactional, pure-Go single-file store for
   versioned allocator snapshots. It reuses a storage technology already present through the
   containerd dependency graph, avoids a CGO requirement, and keeps the allocator core independent
   through its [`StateStore`](../internal/allocator/state.go) interface.
2. **Reticulum-Go Channel envelope delivery to the Python reference** — closed by
   [`internal/transport/rns/interop_python_test.go`](../internal/transport/rns/interop_python_test.go)
   (`TestPythonReferenceChannelEnvelope`): the Go endpoint discovers the upstream Python RNS node,
   initiates a link, and sends validated r1s envelopes over a Reticulum-Go Channel while a wrapped
   UDP interface injects channel-packet loss. The Python reference reassembles the envelopes, echoes
   the exact bytes back over the same Channel, and the Go endpoint re-validates them with the
   authenticated sender authority from the link identity. The wrapper is built on the canonical
   `pkg/interfaces` interception primitive (`NewUDPInterface` + `Send` override), so no Reticulum-Go
   internal dependencies are copied into this repository.
3. **Offer commitment model** — keep offers as hard, time-bounded capacity leases and add an
   authenticated explicit release for every known unselected offer in
   [F6](./f6-offer-release/README.md). Hard offers preserve the guarantee that a selected allocator
   still has capacity when assignment arrives; explicit release avoids holding losing reservations
   until expiry. Expiry remains the fallback when a release is lost or the client disconnects.
   Soft advisory offers were rejected because concurrent clients can consume the advertised slot
   before assignment, and failover after an ambiguous assignment timeout can start the same request
   on multiple independent allocators.
4. **Command errors are explicit, bounded, and correlated** — authenticated allocator rejections
   return a correlatable `CommandError` with a stable code and retry flag in
   [F8](./f8-protocol-feedback/README.md). Rejections never carry container stdout/stderr;
   duplicate rejection is stable across restart via the replay cache. An assignment timeout remains
   ambiguous and never authorizes a second allocation.
5. **Execution state is revision-ordered** — `ExecutionState.revision` monotonically orders durable
   transitions and survives allocator and client restart in
   [F8-02](./f8-protocol-feedback/f8-02-state-revisions.md). Legacy timestamp-only states
   keep backward-compatible ordering; contradictory equal revisions are rejected.
6. **Logs stay local; retrieval is explicit, authenticated, and bounded** — stdout/stderr are kept
   in local allocator storage and are transferred only after an explicit `ExecutionLogsRequest` with
   stream, offset, and byte cap from the authenticated owner in
   [F9](./f9-local-logs/README.md). Completion, failure, inspection, result, and reconnection
   never transfer logs. Transfer mechanism and checksum contract remain an open decision.
7. **Admission and resource profiles are allocator-owned** — resource classes map to runtime-neutral
   limits enforced by the containerd adapter and are preserved across restart in
   [F10-01](./f10-local-admission/f10-01-resource-profiles.md); per-identity quotas and
   an admission policy JSON gate reservation and execution in
   [F10-02](./f10-local-admission/f10-02-identity-quotas.md). GPU device allocation and
   trusted-client defaults remain open.
8. **History retention uses tombstones behind the replay horizon** — terminal executions gain a
   bounded `retain_until`, expired results are explicit, and cleanup sweeps offers, executions, and
   tombstones transactionally in [F11-01](./f11-state-retention/f11-01-retention-contract.md),
   so an old assignment replayed after cleanup cannot start another workload.
9. **Cluster membership uses one shared secret without a control plane** — `cluster init` creates a
   random 256-bit key, announces expose only its domain-separated public ID, and authenticated RNS
   peers mutually prove key possession before any control envelope is delivered in
   [F12](./f12-cluster-membership/README.md). Per-identity admission remains allocator-local.

10. **The live regression environment is a documented manual run, not a hosted CI job** —
    `mytecor-homelab` stays a private development host and is never registered as a GitHub Actions
    self-hosted runner. Live verification therefore remains an explicit manual step at a known
    commit, recorded in the roadmap; the automated pipeline continues to cover only the
    deterministic checks in [F7-01](./f7-verification/f7-01-ci-parity.md) (`make check`, go vet,
    cross-compile).
11. **Reticulum-Go is consumed through its canonical GitHub module path** — upstream `v1.2.0`
    publishes `github.com/Quad4-Software/Reticulum-Go` with externally resolvable first-party
    dependencies, and the baseline is the standard Go module graph without local `replace`
    directives, vendoring, copied dependencies, or a project-maintained fork; the transport
    adapter remains behind the interface described in [F2](./f2-rns-transport/README.md).
    **Rejected experimental deviation (F21 tunnel data plane):** the private tunnel RNS transport
    ([F21-01](./f21-tunnel-rns-dataplane/f21-01-tunnel-rns-stack.md)) depends on two defects in
    `v1.2.0` that are not upstream in `v1.3.0` either — (a) `pkg/channel` inbound
    emplace/drain/dispatch is not serialized, so the parallel packet-worker pool reorders stream
    data and corrupts tunnel bytes; (b) `pkg/backbone` hub can clobber its own `evWrite` interest
    when `QueueSend` races `writeStream`, stranding queued bytes and stalling both tunnel links at
    ~10s. r1s does not vendor, copy, or locally replace Reticulum-Go. Until the fixes land in a
    canonical release, the deliberately removable
    compatibility layer under [`internal/tunnel/rns`](../internal/tunnel/rns) (split across
    `backbone.go`, `compat.go`, and `writer.go`) registers a per-Link serial ingress proxy and
    selects Reticulum-Go's public synchronous, process-global
    Backbone Go backend whenever the tunnel is enabled; startup fails closed if another component
    already selected a native backend. It also contains the negotiated-MDU writer, blocking
    reader, and sender-side uncompressed Buffer policy required by the same version. The latter
    emits the existing `StreamDataMessage` form with `compressed=false`, which remains fully
    interoperable with Python RNS while avoiding three failed bzip2 probes per incompressible
    tunnel fragment. It does not change the wire format or add stream framing. Removal is a few
    attachment-point edits documented in those files. The regression coverage in
    [`internal/tunnel/rns/regression_test.go`](../internal/tunnel/rns/regression_test.go) gates the
    adapter and its eventual deletion; upstream tracking is
    [Reticulum-Go issue #17](https://github.com/Quad4-Software/Reticulum-Go/issues/17) for
    correctness/parity and
    [Reticulum-Go issue #18](https://github.com/Quad4-Software/Reticulum-Go/issues/18) for the
    compatible compression policy. The same adapter temporarily replaces Channel's 5ms
    `WaitReady` polling with a 100µs reusable-timer poll; this is wire-neutral and tracked for an
    event-driven upstream replacement in
    [Reticulum-Go issue #19](https://github.com/Quad4-Software/Reticulum-Go/issues/19). F21-05
    rejected this transport for production despite those workarounds; F21-06 removes the prototype,
    so none of these deviations become part of the selected tunnel data plane.
12. **Bulk application data transfer is out of scope** — the earlier plan for an external,
   capability-authorized data plane (artifact identity, endpoint URIs, Yggdrasil as the first
   network) was removed from the roadmap as undecided work. OCI image distribution remains
   containerd plus a standard registry and needs no r1s transport.
13. **Direct mode remains the CLI default; local API is an explicit opt-in** — `r1s` starts in
    direct mode unless `--socket <path>` is passed, and only `r1s serve`/`cluster` run in direct
    mode at all. The compatibility note in
    [F13-02](./f13-local-client-api/f13-02-cli-integration.md) is therefore resolved: direct mode
    stays the default and the local service stays an explicit opt-in. Socket discovery is now
    implemented (the missing migration piece): without `--socket`, a live service at the default
    `~/.config/r1s/client.sock` is transparently used, otherwise the command runs in direct mode;
    an explicit `--socket` is authoritative and never silently falls back. Remaining future work
    (system-level service supervision, finer error UX) is not required for the resolved default.
14. **Deployment desired state is the keep-alive intent, not a manifest** — [F15](./f15-deployment-reconciliation/README.md)
    was closed on 2026-09-16 without building `r1s deploy`: the durable, client-owned desired state
    for one execution is the lease-holding intent recorded by `request --keep-alive` and replayed
    by the shared client engine ([F17](./f17-execution-lease/README.md)). A lost lease converts the
    intent into a re-request of the recorded workload with its allocator pinning, which covers
    create-once, restart recovery, and self-healing. The manifest layer — `r1s deploy apply/status`,
    multiple named deployments, revision hashes, create-before-destroy replacement, and removal —
    was never built and moved to the deferred list.

15. **F14 tunnel simplification review** — the F14 design was reviewed and simplified before
    implementation (2026-09-17): `r1s tunnel` is service-backed only (a bidi `LocalTunnel` stream
    over the local Unix socket, added additively to `local.proto`; stdout never carries tunnel
    bytes) because only `r1s serve` holds the F17 keep-alive intent that keeps the execution alive
    mid-session; edge node keys are HKDF-derived from persistent identity seeds (separate `info`
    for the client and `r1sd`) instead of managed key files; a run-time routing preamble
    (execution ID + grant ID) disambiguates accept-time lookup before any payload is spliced;
    the grant and the active session are one registry record per execution (repeat mint replaces
    the outstanding grant, re-mint after a close is immediate, terminal sweep deletes the single
    record); TTL expiry is lazy (no sweeper goroutine) and the grant is consumed only on a
    successful splice; the allocator edge starts eagerly with `r1sd` when `tunnel.enabled`; there
    is no allocator-wide session cap in v1; the server-side target is resolved at grant time from
    allocator-local configuration. Rejected alternatives: direct-mode tunnels, per-command client
    edge startup, persisted node-key files independent of identity, and mint-time session-cap
    enforcement. See
    [F14-01](./f14-direct-node-access/f14-01-access-grant.md),
    [F14-02](./f14-direct-node-access/f14-02-tunnel-service.md).

16. **F14-02 edge: embedded `Core` as `net.PacketConn`; the tunnel contract stays a byte stream** —
    reviewed 2026-09-18 and settled the open path left by decision 15: where the packet-level
    Yggdrasil API meets the flow contract, keep `tunnel.Conn` as `io.ReadWriteCloser` (a reliable,
    ordered, bidirectional byte pipe with per-direction half-close, carrying arbitrary SSH/HTTP
    traffic) and build a thin **stream-adaptation layer**, not a reliability layer. Yggdrasil-go
    v0.5.14 already gives an ordered reliable session transport under the hood (ironwood `HandleConn`
    runs over a TCP-like `net.Conn`), so ordering and loss are not our concern — the only gap is
    **message boundaries**: `Core.ReadFrom`/`WriteTo` expose discrete datagrams, while `tunnel.Conn`
    expects a continuous stream with no framing. The adapter maps `net.PacketConn` to `tunnel.Conn`
    by reassembling reads, splitting writes by MTU, and emulating `CloseRead`/`CloseWrite` with a
    sentinel in the stream; this is ~50–100 lines and preserves the transport-neutral contract for
    the in-memory fake and future transports. Key model decisions:

    - **No host-level Yggdrasil daemon is consumed; no tun interface is required.** `r1sd` embeds a
      `Core` library node (HKDF-derived node key from the `r1sd` identity, `AllocatorNodeKeyContext`)
      and talks to it through `net.PacketConn` — `ReadFrom`/`WriteTo` — not through an OS network
      stack. If a host-level Yggdrasil runs on the same machine for other services (for example
      Caddy), the r1s node is a separate overlay address on the same mesh; it does not share the
      host `tun0` and does not conflict with it. This supersedes the F14-02 wording that the
      embedded node "joins public peers so no host-level daemon is required"; the real constraint
      is "the r1s node needs no tun and no shared host address".
    - **Containers need no overlay address.** The tunnel terminates on the allocator at the grant-time
      `(host, port)` target (resolved allocator-local, host namespace), so no per-container subnet
      allocation is ever required; the "assign addresses to containers in the subnet" idea was
      explicitly rejected.
    - **`internal/tunnel/yggdrasil` imports yggdrasil-go and is the only such package**; the stream
      adapter replaces the current stubbed `Dial`/`Accept` that return `ErrorDeferred`.
    - **One tunnel = one `Conn`; containers are differentiated by the Preamble, not by an in-stream
      multiplexer.** `Listener.Accept()`/`Dialer.Dial()` return a separate `tunnel.Conn` per tunnel
      (TCP-like connection model). Distinguishing which execution a session targets is the job of the
      one-time routing Preamble (execution ID + grant ID) resolved against the registry record
      (`executionID → {grant, session}`), already capped at one live session per execution
      (`ErrSessionBusy`). No application-level multiplexer multiplexes several containers inside a
      single `Conn`, and none is needed: containers live in the host namespace behind grant-time
      targets, and each parallel tunnel is its own `Conn`. Model A (one tunnel = one ygg stream, Preamble
      separates executions) was chosen over Model B (a multiplexer carrying many tunnels in one ygg
      stream); Model B would add framing/multiplexing complexity without a current need and
      contradicts how `Conn`/`Listener`/`Dialer` are shaped.
    - **Rejected alternative:** redefining `tunnel.Conn` as packet-based. That would leak message
      boundaries into the core and force SSH/HTTP (byte streams) to cope with framing anyway, plus
      break the in-memory fake for deterministic tests.
17. **F14-02 stream adapter implementation choices** — closed with the F14-02 implementation
    (2026-09-20). The adapter in [`internal/tunnel/yggdrasil`](../internal/tunnel/yggdrasil) maps
    `Core.ReadFrom`/`WriteTo` to `tunnel.Conn` as follows: one frame equals one packet
    (`[type:1][len:2 BE][payload]`, payload capped at 16 KB — well under the packet MTU), so no
    byte-level reassembly state machine is needed; ironwood's per-peer ordered queue guarantees
    frame ordering, so reassembly is pure concatenation. Frame types: data, write-EOF (half-close),
    close (classified reason + bounded detail over the wire), preamble, accept. A typed packet mux
    owns the node's single `ReadFrom` and demultiplexes by remote key; the session identity is the
    remote node key (Model A, decision 16), inbound sessions open as bounded pending records
    (preamble expected, deadline-bounded, capped count) that promote in place after allocator-core
    validation. The handshake retries the preamble on a fixed cadence (the mesh silently drops
    packets sent before a path exists; an unpromoted preamble is safe to repeat) and carries
    teardown reasons over the wire so `ReasonCloser` holds across the mesh. The client-side edge
    surface is settled alongside: `r1s serve --tunnel` starts the client edge eagerly (mirroring
    the allocator policy from decision 15) and `--tunnel-peer` configures bootstrap peer URIs —
    edge configuration, never a protocol feature; empty joins the public overlay.

18. **Execution tunnel isolation (2026-09-21)** — supersedes the host-namespace target assumption
    in decisions 16–17. Client-supplied ports are resolved by a runtime interface inside the
    selected execution's network namespace. The containerd implementation requires a local Linux
    daemon and namespace-entry permissions; it fails closed on unsupported configurations.
    See [F20](./f20-client-tunnel-targets/README.md).

19. **F21 proto-cleanup exception withdrawn (2026-09-23)** — the earlier F21 plan allowed direct
    deletion and renumbering when replacing the grant/preamble/peer-key tunnel with identity/Open.
    F21-05 rejected that replacement, so grants and `ygg_peer_pubkey` remain selected and the
    exception is no longer applicable. F21-06 removes only unused experimental Open/advertisement
    fields and follows the normal additive Protobuf rule from
    [CONTRIBUTING.md](../CONTRIBUTING.md): removed numbers and names are reserved and never reused.

20. **Keepalive/staleness workaround for the F21-05 sustained-transfer blocker (2026-09-23)** —
    the tunnel Link data plane prevents the Reticulum-Go v1.2.0 keepalive/staleness watchdog from
    STALEing an edge Link during a long one-way transfer by delivering a periodic, peer-visible
    inbound Channel frame from every edge (open decision 10). Chosen over the alternatives: r1s
    does not vendor or fork Reticulum-Go (decision 11), and the upstream-looking fix (raise
    `KeepaliveMinSec` or scale `staleTime` on low-RTT links, or refresh `lastInbound` when a peer
    proof for outbound data is validated) must land in a canonical Reticulum-Go release before it
    is used here. The workaround is an unmodifiable-with-behaviour delta, isolated behind
    `tunnelReticulumCompat` and removable when upstream issue
    [#17](https://github.com/Quad4-Software/Reticulum-Go/issues/17) ships the fix; removal is the
    beacon block in `newReticulumCompatStream` plus `TestSustainedKeepaliveBeaconInstalled` /
    `TestSustainedKeepaliveDoesNotCorruptStream` and the now-passing
    `TestSustainedTransferOutlivesStaleTime`. No wire format, Channel message, or application
    framing is changed; the beacon uses a dedicated user-range Channel message type distinct from
    the stream's. The workaround remains benchmark evidence only and is deleted with the rejected
    private-RNS tunnel package in F21-06.

21. **F21-05 selects Ygg for the execution-tunnel data plane (2026-09-23)** — the private
    Python-compatible RNS `Link`/`Channel`/`Buffer` prototype is a no-go. After removing automatic
    bzip2 work and reducing the Go readiness poll from 5ms to 100µs, the recorded 10 MiB row is
    still 4.80 MB/s versus 159.25 MB/s on Ygg; 10 concurrent streams are 5.70 MB/s versus 227.16
    MB/s. The remaining gap comes from protocol-compatible behavior — 423-byte stream payloads, a
    maximum Channel window of 48, an explicit signed proof for every Channel packet, IFAC work,
    and small Backbone writes — and worsens with RTT. F21-06 therefore retains the embedded Ygg
    adapter, grants, peer-key pinning, and multiplexed streams, and removes the experimental RNS
    data plane and unused Open/advertisement slice. RNS remains discovery, identity, and control;
    it does not carry execution-tunnel application bytes. A future system-Ygg/raw-TCP design, if
    pursued, requires a separate authenticated capability protocol and benchmark.

## Deferred

- VM and microVM runtime adapters.
- Broader multi-client fairness policy (admission and identity quotas are in [F10](./f10-local-admission/README.md)).
- Manifest-level deployment reconciliation (`r1s deploy apply/status`, named multi-deployment
  desired state, revision hashes, create-before-destroy replacement, removal semantics) — [F15](./f15-deployment-reconciliation/README.md)
  was closed because the keep-alive intent from [F17](./f17-execution-lease/README.md) already
  satisfies the durable single-execution need (resolved decision 14). Revisit only if
  multi-deployment or spec-replacement workflows appear; such a layer must stay client-side over
  the existing execution operations.
- Application-level event buses, agent hierarchy, and task decomposition.
- External data plane for bulk application data — requires a fresh decision on whether it belongs
  in the system at all (see open decision 1 above).
- Direct-mode client bridge for `r1s tunnel` (no live `r1s serve`): out of v1 scope; the
  service-backed-only model is resolved decision 15.
- TCP fallback or NAT-traversal plans for a tunnel edge deployment where the private peer set is
  not reachable.
- Per-session isolation of the mesh packet pump: the embedded edge's single `readLoop`
  goroutine both demultiplexes packets and feeds them into per-session cursors, and two shared
  stops can stall the whole node — inbound `ingest` blocks while a session's decoded inbox is
  over the high-water mark (a stalled splice consumer throttles every session, not just its own),
  and `openPending` blocks on the 8-slot inbound queue while the accept loop is busy. Malformed
  preambles no longer kill the accept loop (resolved decision 17), but a flood of distinct foreign
  keys can still hold `readLoop` for up to `pendingSessionTimeout`. A future refactor should move
  per-session feed and pending-open off the shared pump (per-session read goroutines or a
  non-blocking pending queue) so one slow or abusive peer cannot throttle the allocator's whole
  tunnel edge.

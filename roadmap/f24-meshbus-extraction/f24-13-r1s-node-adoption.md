# F24-13. Adopt the meshbus Node in r1s

**Status:** Implemented; deterministic verification complete, live shared-daemon/containerd rerun pending.

## Contract

The r1s core routes ordinary control by immutable `meshbus.PeerID`. Only the RNS adapter and
explicit bootstrap credentials know destination hashes. A Node composes discovery, authentication,
direct messaging, and advisory pub/sub while preserving all existing execution authority.

Discovery reads full candidate plus authenticated snapshots, including before any notification is
consumed. Notifications coalesce; periodic refresh repairs missed wake-ups. The local broker
forwards replacement snapshots and independently dispatches bounded concurrent sends. Its local
protocol is version 2; running brokers from older binaries must be restarted.

Allocator `ready`, `capacity`, and `draining` subjects carry advisory free slots, availability,
and revisions. The publisher coalesces changes and refreshes late interests. Clients use only the
transport sender, ignore stale revisions, and expire hints back to presence. Draining rejects new
reservations without terminating existing offers, executions, or leases.

`ExecutionRequest` stays direct, including concurrent destination bootstrap before interests are
known. Offers, assignment, release/retry, renewal/ACK, cancel, inspect, requested logs, and tunnel
control remain direct; application tunnel bytes remain on Yggdrasil.

## Verification

- `make check`: generated-code parity, vet, full race suite, and offline documentation links.
- Discovery bursts exceed the former 32-entry channel, preserve newest descriptors across
  authentication promotion, and return independent snapshots.
- Broker snapshots recover without notifications, and blocked destination bootstrap cannot block
  ordinary PeerID sends.
- Loopback RNS endpoints prove destination bootstrap, subsequent Node direct sends, and capacity
  events to an authenticated passive client.
- Status tests prove sender binding, revision ordering, hint expiry, and restart fallback.
- Allocator tests prove draining preserves reserved offers and execution lifetime while rejecting
  new demand; free-capacity snapshots cannot mutate accounting.
- Existing bootstrap, authority, lease, log, and tunnel regression tests remain enabled.

The full opt-in shared-daemon/Python/containerd procedure in
[F7-02](../f7-verification/f7-02-live-regression.md) still needs a Linux development host before
recording live acceptance or changing deployment pins. Skipped gates are not a live pass.

## Compatibility and follow-up

The Protobuf schema, realm proofs, membership tokens, and direct payload wire format are unchanged.
The meshbus v0.4.0 inbound passive-session routing workaround is confined to the RNS adapter;
its removal and remaining live acceptance are tracked in [the backlog](../BACKLOG.md).

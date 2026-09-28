# meshbus

`meshbus` is a small brokerless messaging layer for authenticated peers. It provides shared-secret
realm membership, bounded peer discovery, opaque direct messages, and best-effort one-hop pub/sub.

The core package and [`realm`](./realm) use only the Go standard library. [`rns`](./rns) is the
Reticulum adapter and is the only package that depends on Reticulum-Go.

## Guarantees

- The sender exposed to an application always comes from the authenticated transport session.
- Serialized payload identities are never authoritative.
- Discovery metadata is advisory and bounded.
- Pub/sub is in-memory, TTL-bounded, deduplicated, and best-effort; it has no replay, forwarding,
  offsets, consumer groups, or exactly-once guarantee.
- Transport routes remain adapter-private; applications address `PeerID` values.

See [WIRE.md](./WIRE.md) for the interoperable wire formats and compatibility policy.

## Verification

```sh
go test ./...
go test -race ./...
go vet ./...
```

## Status

The module is pre-v1 while its public API is exercised by external consumers. Published wire
version markers and domain separators are changed only by introducing a new version.

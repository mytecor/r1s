# F24-07 — Add the cohesive meshbus Node API

**Status:** ✅ Complete

## Outcome

Provide one small application-facing meshbus object that composes realm membership, transport,
peer discovery, direct messaging and pub/sub. Applications using meshbus should not need to
manually wire Bus peer snapshots into the transport.

## Scope

Introduce a high-level `Node` abstraction. Conceptually:

```go
node.Send(ctx, peer, payload)
node.Subscribe(topic, handler)
node.Publish(ctx, topic, payload, options)
node.Peers()
node.Close()
```

Exact API naming may differ. `Node` should compose interfaces rather than depend directly on
Reticulum; the Reticulum implementation is supplied by `meshbus/transport/rns`. Publishing should obtain its
authenticated peers and matching leased interests from `Node` automatically. Direct messages that are not
meshbus event frames must continue to reach the configured direct-message handler.

## Constraints

`Node` is orchestration only. Do not move transport implementation into `Node`. Do not introduce:

- durable event storage;
- replay;
- consumer groups;
- central broker;
- cluster-wide membership consensus;
- multi-hop event gossip.

## Acceptance

A test application can create two `Node`s using an in-memory fake transport and:

1. discover each other;
2. send an authenticated direct message;
3. subscribe to a topic;
4. publish an event without explicitly supplying peers;
5. receive the event with the authenticated sender.

The same API can be backed by `meshbus/transport/rns`. All resource bounds from Bus and `PeerDirectory`
remain enforced. `make check` passes.

## Implementation notes

[`core.Node`](https://github.com/mytecor/meshbus/blob/v0.4.0/core/node.go) composes a transport through the small `NodeTransport`
interface. A `TransportFactory` receives the already-composed inbound handler, while a
`PeerObserver` reports advisory discovery and successful realm authentication separately. Node owns
a bounded candidate directory and a session-lifetime authenticated directory; only authenticated,
ready peers enter pub/sub fan-out. Since v0.3.0, meshbus adds direct interest queries and periodic leased renewals,
so publication targets only authenticated peers with a matching non-expired interest.

The application-facing surface provides `Start`, peer-addressed `Send`, `Subscribe`, `Publish`,
`DiscoveredPeers`, authenticated `Peers`, and idempotent `Close`. Lifecycle transitions are explicit:
send/publish before `Start`, repeated `Start`, and use after `Close` return distinct errors. Node
automatically expires stale candidates; authenticated peers remain until their transport session
ends. `Publish` matches concrete subjects against local wildcard subscriptions and leased remote
interests, delivers locally unless `RemoteOnly` is set, and reports attempted, delivered, and
failed remote sends. Existing per-item,
queue, deduplication, discovery, and concurrency bounds remain configurable.

[`meshbus/transport/rns.NewNode`](https://github.com/mytecor/meshbus/blob/v0.4.0/transport/rns/node.go) is the Reticulum-backed constructor. Applications
provide realm, identity, and presence settings in the endpoint config; the constructor installs the
composed inbound handler and directory wiring itself.

Deterministic in-memory tests create two Nodes, discover them in both directions, exchange an
authenticated direct message, and publish an event without supplying peers. They also prove that
the configured discovery and per-subscription bounds remain active. A UDP-loopback integration test
constructs the same API with `meshbus/transport/rns.Endpoint` and exercises RNS discovery, direct delivery,
and pub/sub with the authenticated sender.

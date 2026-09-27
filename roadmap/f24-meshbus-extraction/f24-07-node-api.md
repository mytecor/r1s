# F24-07 — Add the cohesive meshbus Node API

**Status:** ⏳ Planned

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
Reticulum; the Reticulum implementation is supplied by `meshbus/rns`. Publishing should obtain its
peer snapshot from the `Node`'s `PeerDirectory` automatically. Direct messages that are not
meshbus event frames must continue to reach the configured direct-message handler.

## Constraints

`Node` is orchestration only. Do not move transport implementation into `Node`. Do not introduce:

- durable event storage;
- replay;
- consumer groups;
- central broker;
- cluster-wide membership consensus;
- subscription advertisements;
- multi-hop event gossip.

## Acceptance

A test application can create two `Node`s using an in-memory fake transport and:

1. discover each other;
2. send an authenticated direct message;
3. subscribe to a topic;
4. publish an event without explicitly supplying peers;
5. receive the event with the authenticated sender.

The same API can be backed by `meshbus/rns`. All resource bounds from Bus and `PeerDirectory`
remain enforced. `make check` passes.

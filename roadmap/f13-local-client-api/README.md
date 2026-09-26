# F13. Local client API

Corresponds to [milestone F13](../../ROADMAP.md#f13-local-client-api).

**Status:** 🚫 superseded — the local gRPC service, `r1s serve`, the `--socket` routing, and the
`Watch` journal were removed by [F22-07](../f22-rns-shared-instance/f22-07-client-cleanup.md).
Historical record only; the client is now an ephemeral in-memory run process with no local API.

## Outcome

Applications use a long-running local r1s client over a Unix socket instead of rebuilding RNS,
identity, and durable client state for every command. The service represents exactly one local
client identity; it is not a cluster API server or a new source of global state.

## Dependencies

- [F4. Client workflow](../f4-client-workflow/README.md)
- [F5. Partition recovery](../f5-partition-recovery/README.md)
- [F9. Local logs and explicit retrieval](../f9-local-logs/README.md)

## Scope

- Add an `r1s serve` mode with a local gRPC API over a Unix socket.
- Expose request, inspect, cancel, list, result, logs, and execution watch operations.
- Stream durable execution-state changes through `Watch` so applications need not poll.
- Reuse the existing client authority, persistence, replay, and reconnect behavior.
- Allow the CLI to use the local service while preserving an explicit direct mode.

## Completion criteria

- A local application completes the existing client workflow through the socket.
- Restarting the service restores the same identity-scoped state without duplicate assignment.
- A watcher reconnects from a durable position and does not invent or reorder revisions.
- Socket permissions and peer access have secure defaults and are documented.
- The service never becomes a remotely reachable cluster-wide control endpoint by default.

## Tasks

- [F13-01 — Add the persistent local client service](./f13-01-local-service.md)
- [F13-02 — Route CLI workflows through the local API](./f13-02-cli-integration.md)

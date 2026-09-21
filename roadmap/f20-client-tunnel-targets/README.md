# F20. Client-managed tunnel targets

Corresponds to a future milestone in [ROADMAP.md](../../ROADMAP.md#f20-client-managed-tunnel-targets).

**Status:** ✅ Complete — the tunnel's destinations stop being hard-coded config on the allocator
(`r1sd --tunnel-target`). The `r1s` client owns the destination ports: `r1s tunnel <id> --port
<host>:<container>` binds a local listener on `127.0.0.1:<host>` and relays each inbound connection
to the container port `<container>` over its own tunnel stream, and the allocator only
proxies/splices to whatever port the grant names. There are no named slots and no interactive pipe:
every tunnel requires at least one `--port`. The allocator keeps its authorization role (peer-key
pinning, one live session per execution) but no longer decides *where* a stream terminates.

This reverses the F14-01/BACKLOG decision that targets are resolved at grant time exclusively
from allocator-local configuration.

## Dependencies

- F19 — the multiplexed stream transport and the grant/ack flow.
- F14 — the tunnel edge, grant registry, preamble, mesh node.

## Relation to F19

F19-01 kept the F14 server-resolved target model and added multiplexing on top. F20 changes *where
the destination list comes from*: from `r1sd` configuration to the `r1s` client, and narrows each
target to a container port under a Docker-style `--port host:container` map instead of a named slot.
Everything about the multiplexed transport, per-stream flow control, and stream-open
authorization stays.

## Tasks

- [F20-01 — Client-supplied target slots in the tunnel grant](./f20-01-client-supplied-target-slots.md)

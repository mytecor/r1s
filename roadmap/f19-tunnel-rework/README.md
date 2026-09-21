# F19. Universal tunnel rework (r1s-tunneld)

Corresponds to a future milestone in [ROADMAP.md](../../ROADMAP.md#f19-universal-tunnel-rework).

**Status:** ✅ Complete — the F14 direct-access tunnel (`r1s tunnel`) is reworked from a single
interactive byte pipe (SSH-oriented: one live session per execution, one target per grant) into a
general-purpose, addressable, multiplexed stream transport **behind a single `r1s tunnel`
command** (ngrok-style UX, client→allocator only). See
[F19-01](./f19-01-universal-tunnel.md); `make check` passes.

## Outcome

`r1s tunnel` stays the **only** user-facing command, and it exposes the execution as a set of
**addressable multiplexed streams** on top of the existing F14 mesh: `r1s tunnel <id> --port
<host>:<container>` binds a local listener on `127.0.0.1:<host>` and relays each inbound connection
to the container port `<container>` over its own stream — always client→allocator, while keeping the
F14 authorization model (peer-key pinning, one-time grants, transport-neutral `internal/tunnel`
contract). There is no reverse, listen, or publish surface: the client connects to the container,
never the other way around.

> **Follow-up:** the allocator-side slot resolution landed here is being reversed by
> [F20 — Client-managed tunnel targets](../f20-client-tunnel-targets/README.md): the destination
> source of truth moves from `r1sd` config to the `r1s` client, which supplies the container-port
> list in the grant and exposes it as Docker-style `--port host:container` mappings.

## Dependencies

- F14 — the existing tunnel edge, grant, preamble, mesh node (`internal/tunnel/yggdrasil`).
- F17 — execution lease (lifetime authority stays there).
- F13 — local client API surface (additive RPCs only).

## Relation to F14

The rework is organized as **additive layers** so the existing flow keeps working while the
general machinery lands (see [f19-01](./f19-01-universal-tunnel.md)); the interactive pipe is
then removed entirely when `--port` becomes the only surface (F20).

## Tasks

- [F19-01 — Universal tunnel: single multiplexed `r1s tunnel` command](./f19-01-universal-tunnel.md)

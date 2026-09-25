# F22-06 — Run-owned published ports and tunnel rebinding

**Status:** ✅ Complete

## Outcome

`-p/--publish host:container` belongs to a logical run. Its loopback listener remains bound across
execution attempts, while new streams are authenticated and spliced to the current execution.

## Scope

- Move the user-facing tunnel surface into repeatable `r1s run -p host:container`; remove the
  standalone `r1s tunnel <execution>` command.
- Keep each local listener owned by the run process. On reschedule, close the old execution tunnel,
  preserve the listener, authenticate the replacement execution, and route new streams to it.
- Existing streams may terminate during rebinding; never silently route an established stream to a
  different execution.
- Replace the mint/grant round trip with an additive authenticated Open-style control handshake.
  The allocator authorizes the transport-verified run identity against the current execution owner
  and validates that the execution is running before entering its network namespace.
- Retain the selected Yggdrasil application data plane and F19 multiplexing. Do not revive the
  rejected private-RNS data plane from F21.
- Remove grant ID/TTL registries and peer-key binding only after the replacement handshake proves
  equivalent owner authentication. Do not reuse field numbers or names reserved by F21-06.
- Keep target ports client-selected and resolve them only inside the selected execution's network
  namespace through the runtime interface.

## Acceptance

- Only the authenticated owner of a running execution can open a stream; forged payload identity,
  stale execution IDs, foreign runs, and host-namespace targets fail closed.
- A local listener remains bound while attempt 1 is replaced by attempt 2; old streams close and
  new connections reach attempt 2.
- Multiple published ports and concurrent streams retain F19 ordering, half-close, and isolation
  behavior.
- Allocator restart and run reconnect do not grant access without a fresh authenticated open.
- Protocol evolution is additive and generated artifacts are current.
- The live Linux container-namespace tunnel test and `make check` pass.

## Notes

- This task starts only after [F21-06](../f21-tunnel-rns-dataplane/f21-06-remove-ygg-adapter.md)
  establishes the retained Ygg baseline.

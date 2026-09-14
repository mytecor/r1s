# F5 — Partition recovery

## Outcome

The complete system tolerates duplicate messages, process restarts, and temporary loss of client
connectivity without duplicate execution or premature termination.

## Completion criteria

- An assigned workload completes while its client is offline.
- The client later discovers final state and retrieves the retained result.
- Replayed assignment and cancellation messages are harmless.
- Allocator and client restarts recover from durable state.

## Tasks

- [F5-01 — Add partition recovery acceptance](./f5-01-recovery-acceptance.md)


# F5-01 — Add partition recovery acceptance

**Status:** Planned

## Outcome

Create an end-to-end harness that interrupts RNS connectivity and restarts both sides during a real
container execution.

## Acceptance

- The workload starts exactly once.
- It is not stopped by owner disconnection.
- Its terminal state and result are available after reconnection.


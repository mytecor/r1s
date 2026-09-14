# F1-02 — Implement allocator transitions

**Status:** Complete

## Outcome

Implement an in-memory allocator that reserves class capacity, turns a selected offer into a
runtime start, checks client authority, and cancels an execution through the runtime boundary.

## Acceptance

- The request → offer → assign → running → cancelled path passes a unit test.
- Outstanding offers consume capacity.
- A different client cannot cancel an execution.

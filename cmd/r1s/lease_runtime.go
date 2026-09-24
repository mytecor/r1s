package main

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/mytecor/r1s/internal/protocol"
)

// Client-side lease-holding runtime: the continuous renewal loop of
// `r1s serve` and the foreground hold loop of `r1s request --keep-alive`.
// Both replay the durable lease-holding intents recorded by the client engine
// (internal/client/lease.go) and re-request the recorded workload whenever a
// lease is lost. The loops are the only callers of renewOrReRequest (defined
// in lease_handshake.go); the Backend adapter delegates watch queries through
// the same client engine.

// maintainTick paces the serve-mode renewal loop. Every recorded lease intent
// is replayed from durable state on each tick, so a service restart resumes
// exactly the duties it recorded before stopping.
const maintainTick = 15 * time.Second

// defaultOfferWait paces re-request offer collection.
const defaultOfferWait = 10 * time.Second

// keepAliveTick paces the foreground keep-alive hold loop and the serve renewal loop.
const keepAliveTick = 5 * time.Second

// defaultRenewWait bounds one lease-renewal ack wait in the hold loop.
const defaultRenewWait = 30 * time.Second

// grantAckTimeout bounds one tunnel-grant mint ack wait. It is deliberately
// shorter than the grant TTL the allocator grants (the client does not know
// that TTL); a mint that does not come back in time is aborted so the caller
// can surface a clear error and retry.
const grantAckTimeout = 30 * time.Second

// runLeaseMaintainer keeps every recorded lease-holding intent alive while
// `r1s serve` runs. It is the only continuous renewal holder; one-shot CLI
// invocations never launch it.
func (a *application) runLeaseMaintainer(ctx context.Context, diagnostics io.Writer) {
	ticker := time.NewTicker(maintainTick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		for _, executionID := range a.client.DueLeaseRenewals() {
			if ctx.Err() != nil {
				return
			}
			destination, envelope, lost, err := a.client.Maintain(executionID, 0)
			if err != nil || lost || envelope == nil {
				continue
			}
			if err := a.send(destination, envelope); err != nil {
				fmt.Fprintf(diagnostics, "lease renewal for %s: %v\n", executionID, err)
			}
		}
		for _, executionID := range a.client.LostLeaseIntents() {
			if ctx.Err() != nil {
				return
			}
			activeID, _, rerequested, err := a.renewOrReRequest(ctx, executionID, 0, 30*time.Second)
			if err != nil {
				fmt.Fprintf(diagnostics, "lease re-request for %s failed: %v\n", executionID, err)
				continue
			}
			if rerequested {
				fmt.Fprintf(diagnostics, "lease lost for %s; re-requested as %s\n", executionID, activeID)
			}
		}
	}
}

// holdLease is the foreground workflow of `r1s request --keep-alive` in direct
// mode. It renews the execution's client-held lease when due and re-requests
// the recorded workload whenever the lease is lost, then exits when the
// workload reaches a terminal state or the caller interrupts it. It never
// spawns a background process: the command process itself is the loop.
func (a *application) holdLease(ctx context.Context, executionID string, lease time.Duration, stdout io.Writer) error {
	activeID := executionID
	ticker := time.NewTicker(keepAliveTick)
	defer ticker.Stop()
	for {
		if a.client.LeaseIntentLost(activeID) {
			previous := activeID
			newID, _, _, err := a.renewOrReRequest(ctx, activeID, 0, defaultRenewWait)
			if err != nil {
				return err
			}
			if newID != activeID {
				fmt.Fprintf(stdout, "execution=%s status=lease-lost\n", previous)
				fmt.Fprintf(stdout, "execution=%s status=rerequested\n", newID)
			}
			activeID = newID
		} else if a.client.LeaseDue(activeID) {
			if _, _, _, err := a.renewOrReRequest(ctx, activeID, 0, defaultRenewWait); err != nil {
				fmt.Fprintf(stdout, "execution=%s lease-renewal-error=%v\n", activeID, err)
			}
		}
		if snapshot, ok := a.client.Execution(activeID); ok && snapshot.State != nil && protocol.Terminal(snapshot.State.GetPhase()) {
			printState(stdout, snapshot)
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/client"
	"github.com/mytecor/r1s/internal/protocol"
)

// Client-side lease-holding runtime: the continuous renewal loop of
// `r1s serve` and the foreground hold loop of `r1s request --keep-alive`.
// Both replay the durable lease-holding intents recorded by the client engine
// (internal/client/lease.go) and re-request the recorded workload whenever a
// lease is lost. The loops are the only callers of renewOrReRequest; the
// Backend adapter delegates watch queries through the same client engine.

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

// renewOrReRequest renews one execution's client-held lease. When the lease
// was already lost (the execution was evicted before a renewal landed), it
// re-requests the recorded workload as a fresh request, rebinds the durable
// lease-holding intent to the replacement execution, and renews the
// replacement so the returned expiry is authoritative. The replacement always
// starts on a valid initial lease, so the loop iterates at most once in
// practice instead of recursing.
func (a *application) renewOrReRequest(ctx context.Context, executionID string, leaseDuration, wait time.Duration) (activeID string, expiresAt time.Time, rerequested bool, err error) {
	activeID = executionID
	for {
		destination, envelope, lost, err := a.client.Maintain(activeID, leaseDuration)
		if err != nil {
			return "", time.Time{}, rerequested, err
		}
		var newExpiry time.Time
		if !lost {
			ch, cancel := a.registerWaiter(envelope.GetMessageId())
			if err := a.send(destination, envelope); err != nil {
				cancel()
				return "", time.Time{}, rerequested, err
			}
			newExpiry, err = a.awaitLeaseAck(ctx, activeID, ch, wait)
			cancel()
			if err == nil {
				return activeID, newExpiry, rerequested, nil
			}
			// The renewal may have been rejected because the lease was lost
			// while the command was in flight; the durable intent records that.
			// Recover below.
			if !a.client.LeaseIntentLost(activeID) {
				if rerequested {
					return activeID, time.Time{}, rerequested, errors.Join(err, fmt.Errorf("lease intent rebound to %s; renewal will retry", activeID))
				}
				return "", time.Time{}, rerequested, err
			}
		}
		// The lease was lost: re-request the recorded workload and continue
		// with the replacement, which carries the rebound intent.
		if activeID, err = a.reRequestLostLease(ctx, activeID); err != nil {
			return "", time.Time{}, rerequested, err
		}
		rerequested = true
	}
}

// awaitLeaseAck waits for the allocator ack of one renewal command on the given
// waiter channel (registered for the renewal's correlation ID before send).
func (a *application) awaitLeaseAck(ctx context.Context, executionID string, ch chan *r1sv1.Envelope, timeout time.Duration) (time.Time, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return time.Time{}, ctx.Err()
		case <-timer.C:
			return time.Time{}, fmt.Errorf("timed out waiting for execution %s lease renewal", executionID)
		case envelope := <-ch:
			if err := client.RemoteFailure(envelope); err != nil {
				return time.Time{}, err
			}
			if ack := envelope.GetExecutionLeaseRenewAck(); ack != nil && ack.GetExecutionId() == executionID {
				return ack.GetExpiresAt().AsTime(), nil
			}
		}
	}
}

// reRequestLostLease creates a fresh request from the recorded workload of a
// lease-expired execution, preserving the allocator destinations recorded with
// the lease-holding intent, and rebinds the intent to the replacement
// execution. Renewing the replacement is the caller's next step.
func (a *application) reRequestLostLease(ctx context.Context, executionID string) (string, error) {
	request, ok := a.client.RequestForExecution(executionID)
	if !ok {
		return "", fmt.Errorf("%w: execution %q has no recorded request to re-request", client.ErrExecutionNotFound, executionID)
	}
	_, replacement, _, err := a.runRequest(ctx, request.GetWorkload(), request.GetPolicy(), request.GetResourceClass(), defaultOfferWait, a.client.LeaseIntentAllocators(executionID), 0, request.GetConstraints())
	if err != nil {
		return "", fmt.Errorf("re-request after lease expiry: %w", err)
	}
	if err := a.client.RebindLeaseIntent(executionID, replacement); err != nil {
		return "", fmt.Errorf("lease intent not rebound to %s: %w", replacement, err)
	}
	return replacement, nil
}

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

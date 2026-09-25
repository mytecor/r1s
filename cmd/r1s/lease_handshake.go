package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/client"
)

// The lease handshake: one renewal attempt, its ack wait, and the re-request
// of a recorded workload whose lease was lost. The renewal loops in
// lease_runtime.go call these; the durable lease-holding intent itself lives
// in the client engine (internal/client/lease_intent.go).

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

// reRequestLostLease creates the next attempt from the recorded workload of a
// lease-expired execution. It preserves the logical run ID, advances the
// attempt, and uses every currently known compatible allocator instead of
// pinning placement to the allocator set from the failed attempt.
func (a *application) reRequestLostLease(ctx context.Context, executionID string) (string, error) {
	request, ok := a.client.RequestForExecution(executionID)
	if !ok {
		return "", fmt.Errorf("%w: execution %q has no recorded request to re-request", client.ErrExecutionNotFound, executionID)
	}
	allocators := a.client.AllocatorDestinations(request.GetResourceClass(), request.GetConstraints())
	_, replacement, _, err := a.runNextAttempt(ctx, request, defaultOfferWait, allocators)
	if err != nil {
		return "", fmt.Errorf("re-request after lease expiry: %w", err)
	}
	if err := a.client.RebindLeaseIntent(executionID, replacement); err != nil {
		return "", fmt.Errorf("lease intent not rebound to %s: %w", replacement, err)
	}
	return replacement, nil
}

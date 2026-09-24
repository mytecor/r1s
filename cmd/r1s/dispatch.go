package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/client"
)

// registerWaiter creates a one-shot delivery channel for the envelope whose
// correlation ID matches correlationID. The caller must register BEFORE sending
// the request so a reply that arrives promptly is still routed (dispatch retains
// the envelope only for already-registered waiters). The returned cancel unregisters
// the channel so a timed-out or errored await does not leak it.
func (a *application) registerWaiter(correlationID string) (chan *r1sv1.Envelope, func()) {
	ch := make(chan *r1sv1.Envelope, 1)
	a.waitMu.Lock()
	if a.waiters == nil {
		a.waiters = make(map[string][]chan *r1sv1.Envelope)
	}
	a.waiters[correlationID] = append(a.waiters[correlationID], ch)
	a.waitMu.Unlock()

	var cancelled bool
	cancel := func() {
		a.waitMu.Lock()
		defer a.waitMu.Unlock()
		if cancelled {
			return
		}
		cancelled = true
		chans := a.waiters[correlationID]
		for i, c := range chans {
			if c == ch {
				a.waiters[correlationID] = append(chans[:i], chans[i+1:]...)
				break
			}
		}
		if len(a.waiters[correlationID]) == 0 {
			delete(a.waiters, correlationID)
		}
	}
	return ch, cancel
}

// dispatchEnvelope routes an inbound envelope to every waiter registered for
// its correlation ID. It never blocks: a waiter that raced cancellation simply
// does not receive it. Envelopes with no correlation ID (or no registered
// waiter) are dropped — their effects were already applied to the durable
// client store by handleEnvelope, so nothing is lost for a wait that has not
// started.
func (a *application) dispatchEnvelope(envelope *r1sv1.Envelope) {
	corr := envelope.GetCorrelationId()
	if corr == "" {
		return
	}
	a.waitMu.Lock()
	chans := a.waiters[corr]
	delete(a.waiters, corr)
	a.waitMu.Unlock()
	for _, ch := range chans {
		select {
		case ch <- envelope:
		default:
		}
	}
}

// handleEnvelope applies an inbound control-plane envelope to the durable
// client store and routes it to any awaiting workflow.
func (a *application) handleEnvelope(ctx context.Context, envelope *r1sv1.Envelope) error {
	identityKey := hex.EncodeToString(envelope.GetSender())
	if destination, ok := a.endpoint.DestinationForIdentity(identityKey); ok {
		if err := a.client.RegisterAllocator(client.Allocator{Identity: envelope.GetSender(), Destination: destination}); err != nil {
			return err
		}
	}
	if err := a.client.Handle(ctx, envelope); err != nil {
		return err
	}
	a.dispatchEnvelope(envelope)
	return nil
}

// send relays one envelope to an allocator destination with a fixed timeout.
func (a *application) send(destination string, envelope *r1sv1.Envelope) error {
	ctx, cancel := context.WithTimeout(a.ctx, 30*time.Second)
	defer cancel()
	return a.endpoint.Send(ctx, destination, envelope)
}

// awaitState waits for the execution's latest durable state on the waiter
// channel registered for the requesting correlation ID.
func (a *application) awaitState(executionID string, ch chan *r1sv1.Envelope, timeout time.Duration) (client.ExecutionSnapshot, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case <-a.ctx.Done():
			return client.ExecutionSnapshot{}, a.ctx.Err()
		case <-timer.C:
			return client.ExecutionSnapshot{}, fmt.Errorf("timed out waiting for execution %s state", executionID)
		case envelope := <-ch:
			if err := client.RemoteFailure(envelope); err != nil {
				return client.ExecutionSnapshot{}, err
			}
			if envelope.GetExecutionState().GetExecutionId() == executionID {
				snapshot, _ := a.client.Execution(executionID)
				return snapshot, nil
			}
		}
	}
}

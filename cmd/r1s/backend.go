package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/client"
)

// The r1s application implements localserver.Backend so both the persistent
// `r1s serve` mode and the direct CLI share the same durable client engine.

// runRequest runs the full request -> offer collection -> selection ->
// assignment workflow and returns after the assignment is sent. It mirrors the
// direct `r1s request` flow without printing, so both the CLI and the local API
// can present the same outcome.
func (a *application) runRequest(ctx context.Context, workload *r1sv1.Workload, policy *r1sv1.ExecutionPolicy, resourceClass string, offerWait time.Duration, allocators []string, keepAlive time.Duration) (requestID, executionID string, allocator []byte, err error) {
	requestID, envelope, err := a.client.CreateRequest(workload, policy, resourceClass)
	if err != nil {
		return "", "", nil, err
	}
	sent := make(map[string]bool)
	for _, destination := range allocators {
		if err := a.send(destination, envelope); err != nil {
			return "", "", nil, err
		}
		sent[destination] = true
	}
	timer := time.NewTimer(offerWait)
	defer timer.Stop()
collect:
	for {
		select {
		case <-ctx.Done():
			return "", "", nil, ctx.Err()
		case <-timer.C:
			break collect
		case response := <-a.events:
			if response.GetCorrelationId() == envelope.GetMessageId() {
				if err := client.RemoteFailure(response); err != nil {
					return "", "", nil, err
				}
			}
		case service := <-a.endpoint.Discoveries():
			if service.Descriptor.Capacity[resourceClass] == 0 {
				continue
			}
			identity, decodeErr := hexDecodeIdentity(service.Identity)
			if decodeErr != nil {
				continue
			}
			if err := a.client.RegisterAllocator(client.Allocator{Identity: identity, Destination: service.Destination, Hops: service.Hops, Capacity: service.Descriptor.Capacity}); err != nil {
				return "", "", nil, err
			}
			if !sent[service.Destination] {
				if err := a.send(service.Destination, envelope); err != nil {
					continue
				}
				sent[service.Destination] = true
			}
		}
	}
	destination, assignment, err := a.client.Select(requestID)
	if err != nil {
		return "", "", nil, err
	}
	if err := a.send(destination, assignment); err != nil {
		return "", "", nil, err
	}
	executionID = assignment.GetExecutionAssign().GetExecutionId()
	if keepAlive > 0 {
		// Durably record the lease-holding intent so the shared renewal loop
		// (serve, or a foreground keep-alive request) keeps this execution.
		if err := a.client.RecordLeaseIntent(executionID, keepAlive); err != nil {
			return "", "", nil, err
		}
	}
	snapshot, _ := a.client.Execution(executionID)
	return requestID, executionID, snapshot.Allocator, nil
}

// inspectState sends an Inspect and waits for the allocator state.
func (a *application) inspectState(ctx context.Context, executionID string, wait time.Duration) (*r1sv1.ExecutionState, error) {
	destination, envelope, err := a.client.Inspect(executionID)
	if err != nil {
		return nil, err
	}
	if err := a.send(destination, envelope); err != nil {
		return nil, err
	}
	snapshot, err := a.awaitState(executionID, envelope.GetMessageId(), wait)
	if err != nil {
		return nil, err
	}
	return snapshot.State, nil
}

// cancelState cancels an execution and waits for its state.
func (a *application) cancelState(ctx context.Context, executionID, reason string, wait time.Duration) (*r1sv1.ExecutionState, error) {
	destination, envelope, err := a.client.Cancel(executionID, reason)
	if err != nil {
		return nil, err
	}
	if err := a.send(destination, envelope); err != nil {
		return nil, err
	}
	snapshot, err := a.awaitState(executionID, envelope.GetMessageId(), wait)
	if err != nil {
		return nil, err
	}
	return snapshot.State, nil
}

// renewOrReRequest renews one execution's client-held lease. When the lease was
// already lost (the execution was evicted before a renewal landed), it
// re-requests the recorded workload as a fresh request, rebinds the durable
// lease-holding intent to the replacement execution, and returns its expiry.
func (a *application) renewOrReRequest(ctx context.Context, executionID string, leaseDuration, wait time.Duration) (activeID string, expiresAt time.Time, rerequested bool, err error) {
	destination, envelope, lost, err := a.client.Maintain(executionID, leaseDuration)
	if err != nil {
		return "", time.Time{}, false, err
	}
	if lost {
		return a.reRequestLostLease(ctx, executionID, leaseDuration, wait)
	}
	if err := a.send(destination, envelope); err != nil {
		return "", time.Time{}, false, err
	}
	newExpiry, err := a.awaitLeaseAck(ctx, executionID, envelope.GetMessageId(), wait)
	if err == nil {
		return executionID, newExpiry, false, nil
	}
	// The renewal may have been rejected because the lease was lost while the
	// command was in flight; the durable intent records that. Recover there.
	if !a.client.LeaseIntentLost(executionID) {
		return "", time.Time{}, false, err
	}
	return a.reRequestLostLease(ctx, executionID, leaseDuration, wait)
}

// awaitLeaseAck waits for the allocator ack of one renewal command.
func (a *application) awaitLeaseAck(ctx context.Context, executionID, correlationID string, timeout time.Duration) (time.Time, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return time.Time{}, ctx.Err()
		case <-timer.C:
			return time.Time{}, fmt.Errorf("timed out waiting for execution %s lease renewal", executionID)
		case envelope := <-a.events:
			if envelope.GetCorrelationId() != correlationID {
				continue
			}
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
// lease-expired execution, rebinds the durable lease intent to the replacement,
// and renews it once so the returned expiry is authoritative.
func (a *application) reRequestLostLease(ctx context.Context, executionID string, leaseDuration, wait time.Duration) (string, time.Time, bool, error) {
	request, ok := a.client.RequestForExecution(executionID)
	if !ok {
		return "", time.Time{}, false, fmt.Errorf("%w: execution %q has no recorded request to re-request", client.ErrExecutionNotFound, executionID)
	}
	_, replacement, _, err := a.runRequest(ctx, request.GetWorkload(), request.GetPolicy(), request.GetResourceClass(), defaultOfferWait, nil, 0)
	if err != nil {
		return "", time.Time{}, false, fmt.Errorf("re-request after lease expiry: %w", err)
	}
	if err := a.client.RebindLeaseIntent(executionID, replacement); err != nil {
		return "", time.Time{}, false, err
	}
	activeID, expiresAt, _, err := a.renewOrReRequest(ctx, replacement, leaseDuration, wait)
	if err != nil {
		return replacement, time.Time{}, false, errors.Join(err, fmt.Errorf("lease intent rebound to %s; renewal will retry", replacement))
	}
	return activeID, expiresAt, true, nil
}

// maintainTick paces the serve-mode renewal loop. Every recorded lease intent
// is replayed from durable state on each tick, so a service restart resumes
// exactly the duties it recorded before stopping.
const maintainTick = 15 * time.Second

// defaultOfferWait paces re-request offer collection.
const defaultOfferWait = 10 * time.Second

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
			if _, _, _, err := a.renewOrReRequest(ctx, executionID, 0, 30*time.Second); err != nil {
				fmt.Fprintf(diagnostics, "lease re-request for %s failed: %v\n", executionID, err)
			}
		}
	}
}

// keepAliveTick paces the foreground keep-alive hold loop and the serve renewal loop.
const keepAliveTick = 5 * time.Second

// defaultRenewWait bounds one lease-renewal ack wait in the hold loop.
const defaultRenewWait = 30 * time.Second

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
		if snapshot, ok := a.client.Execution(activeID); ok && snapshot.State != nil && terminal(snapshot.State.GetPhase()) {
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

// retrieveLogs sends one bounded explicit log read and returns the response.
func (a *application) retrieveLogs(ctx context.Context, executionID, stream string, offset uint64, maxBytes uint32, wait time.Duration) (*r1sv1.ExecutionLogsResponse, error) {
	destination, request, err := a.client.Logs(executionID, stream, offset, maxBytes)
	if err != nil {
		return nil, err
	}
	if err := a.send(destination, request); err != nil {
		return nil, err
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			return nil, errLogTimeout(offset)
		case response := <-a.events:
			if response.GetCorrelationId() != request.GetMessageId() {
				continue
			}
			if err := client.RemoteFailure(response); err != nil {
				return nil, err
			}
			if chunk := response.GetExecutionLogsResponse(); chunk != nil {
				return chunk, nil
			}
		}
	}
}

func hexDecodeIdentity(identity string) ([]byte, error) {
	return hex.DecodeString(identity)
}

func errLogTimeout(offset uint64) error {
	return fmt.Errorf("log request timed out; retry explicitly with --offset %d", offset)
}

// The following methods complete application's implementation of
// localserver.Backend: the RPC surface maps onto the same durable client
// engine the direct CLI uses, so service-backed mode shares authority,
// persistence, replay, and reconnect behavior with direct mode.

func (a *application) RunRequest(ctx context.Context, workload *r1sv1.Workload, policy *r1sv1.ExecutionPolicy, resourceClass string, offerWait time.Duration, allocators []string, keepAlive time.Duration) (string, string, []byte, error) {
	return a.runRequest(ctx, workload, policy, resourceClass, offerWait, allocators, keepAlive)
}

func (a *application) Inspect(ctx context.Context, executionID string, wait time.Duration) (*r1sv1.ExecutionState, error) {
	return a.inspectState(ctx, executionID, wait)
}

func (a *application) Cancel(ctx context.Context, executionID, reason string, wait time.Duration) (*r1sv1.ExecutionState, error) {
	return a.cancelState(ctx, executionID, reason, wait)
}

func (a *application) Logs(ctx context.Context, executionID, stream string, offset uint64, maxBytes uint32, wait time.Duration) (*r1sv1.ExecutionLogsResponse, error) {
	return a.retrieveLogs(ctx, executionID, stream, offset, maxBytes, wait)
}

func (a *application) Requests(ctx context.Context) []client.RequestSnapshot {
	return a.client.Requests()
}

func (a *application) State(ctx context.Context, executionID string) (*r1sv1.ExecutionState, bool, error) {
	snapshot, ok := a.client.Execution(executionID)
	if !ok {
		return nil, false, nil
	}
	return snapshot.State, true, nil
}

func (a *application) WatchSeq(ctx context.Context) uint64 {
	return a.client.WatchSeq()
}

func (a *application) WatchAfter(ctx context.Context, after uint64) ([]client.WatchEvent, bool) {
	return a.client.WatchAfter(after)
}

func (a *application) SubscribeWatch(ctx context.Context, observer func(client.WatchEvent)) (cancel func()) {
	return a.client.SubscribeWatch(observer)
}

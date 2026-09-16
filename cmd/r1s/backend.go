package main

import (
	"context"
	"encoding/hex"
	"fmt"
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
func (a *application) runRequest(ctx context.Context, workload *r1sv1.Workload, policy *r1sv1.ExecutionPolicy, resourceClass string, offerWait time.Duration, allocators []string) (requestID, executionID string, allocator []byte, err error) {
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

func (a *application) RunRequest(ctx context.Context, workload *r1sv1.Workload, policy *r1sv1.ExecutionPolicy, resourceClass string, offerWait time.Duration, allocators []string) (string, string, []byte, error) {
	return a.runRequest(ctx, workload, policy, resourceClass, offerWait, allocators)
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

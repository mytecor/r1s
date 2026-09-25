package main

import (
	"context"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/client"
)

// The r1s application implements localserver.Backend so both the persistent
// `r1s serve` mode and the direct CLI share the same durable client engine.
// This file is only the adapter: the workflows it delegates to live in
// workflow.go, the lease-holding loops in lease_runtime.go, and the tunnel
// session cache in tunnel_session.go.

func (a *application) RunRequest(ctx context.Context, workload *r1sv1.Workload, policy *r1sv1.ExecutionPolicy, resourceClass string, offerWait time.Duration, allocators []string, keepAlive time.Duration, constraints *r1sv1.PlacementConstraints) (string, string, []byte, error) {
	return a.runRequest(ctx, workload, policy, resourceClass, offerWait, allocators, keepAlive, constraints)
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

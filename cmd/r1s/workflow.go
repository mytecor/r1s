package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/client"
	"github.com/mytecor/r1s/internal/protocol"
)

// Client workflows shared by the direct CLI and the persistent serve mode:
// the request -> offer collection -> selection -> assignment lifecycle and
// the single-command inspection, cancellation, and log retrieval paths. The
// Backend adapter in backend.go delegates here; the lease-holding loops live
// in lease_runtime.go and the tunnel session cache in tunnel_session.go.

// runRequest runs the full request -> offer collection -> selection ->
// assignment workflow and returns after the assignment is sent. It mirrors the
// direct `r1s request` flow without printing, so both the CLI and the local API
// can present the same outcome. constraints narrows which allocators may offer;
// nil requests any node.
func (a *application) runRequest(ctx context.Context, workload *r1sv1.Workload, policy *r1sv1.ExecutionPolicy, resourceClass string, offerWait time.Duration, allocators []string, keepAlive time.Duration, constraints *r1sv1.PlacementConstraints) (requestID, executionID string, allocator []byte, err error) {
	requestID, envelope, err := a.client.CreateRequestWithConstraints(workload, policy, resourceClass, constraints)
	if err != nil {
		return "", "", nil, err
	}
	// Register the reply waiter before the first send so a prompt offer or
	// rejection is still routed to this collection loop.
	ch, cancel := a.registerWaiter(envelope.GetMessageId())
	defer cancel()
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
		case response := <-ch:
			if err := client.RemoteFailure(response); err != nil {
				return "", "", nil, err
			}
		case service := <-a.endpoint.Discoveries():
			if service.Descriptor.Capacity[resourceClass] == 0 {
				continue
			}
			identity, decodeErr := hexDecodeIdentity(service.Identity)
			if decodeErr != nil {
				continue
			}
			// Register the route with the coarse announce summary so selection
			// has the freshest node evidence; offers carry the full metadata.
			node := summaryNode(service.Descriptor.OS, service.Descriptor.Arch, service.Descriptor.Runtime)
			alloc := client.Allocator{
				Identity: identity, Destination: service.Destination, Hops: service.Hops,
				Capacity: service.Descriptor.Capacity, Node: node,
			}
			// F21-02: populate the tunnel endpoint advertisement from the descriptor.
			if host, port, dest, ok := service.TunnelEndpoint(); ok {
				alloc.TunnelHost = host
				alloc.TunnelPort = port
				alloc.TunnelDestination = dest
			}
			if err := a.client.RegisterAllocator(alloc); err != nil {
				return "", "", nil, err
			}
			// Skip sending to a discovery whose summary positively contradicts
			// the placement constraints. A node without a summary is still tried
			// (unknown, not incompatible); the allocator's request-time check is
			// authoritative either way.
			if !protocol.PlacementCompatible(constraints, node) {
				continue
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
		// The explicit allocator destinations ride with the intent so a
		// re-request after lease loss preserves the pinning.
		if err := a.client.RecordLeaseIntent(executionID, keepAlive, allocators); err != nil {
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
	ch, cancel := a.registerWaiter(envelope.GetMessageId())
	defer cancel()
	if err := a.send(destination, envelope); err != nil {
		return nil, err
	}
	snapshot, err := a.awaitState(executionID, ch, wait)
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
	ch, cancel := a.registerWaiter(envelope.GetMessageId())
	defer cancel()
	if err := a.send(destination, envelope); err != nil {
		return nil, err
	}
	snapshot, err := a.awaitState(executionID, ch, wait)
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
	ch, cancel := a.registerWaiter(request.GetMessageId())
	defer cancel()
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
		case response := <-ch:
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

// summaryNode builds an advisory NodeCapabilities from the coarse announce
// descriptor summary. Nil is returned when nothing was advertised.
func summaryNode(os, arch, runtime string) *r1sv1.NodeCapabilities {
	if os == "" && arch == "" && runtime == "" {
		return nil
	}
	return &r1sv1.NodeCapabilities{Os: os, Arch: arch, Runtime: runtime}
}

func errLogTimeout(offset uint64) error {
	return fmt.Errorf("log request timed out; retry explicitly with --offset %d", offset)
}

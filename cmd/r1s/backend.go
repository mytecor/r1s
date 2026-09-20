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
	"github.com/mytecor/r1s/internal/protocol"
	"github.com/mytecor/r1s/internal/tunnel"
	"github.com/mytecor/r1s/internal/tunnel/yggdrasil"
)

// The r1s application implements localserver.Backend so both the persistent
// `r1s serve` mode and the direct CLI share the same durable client engine.

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
			if err := a.client.RegisterAllocator(client.Allocator{
				Identity: identity, Destination: service.Destination, Hops: service.Hops,
				Capacity: service.Descriptor.Capacity, Node: node,
			}); err != nil {
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

// keepAliveTick paces the foreground keep-alive hold loop and the serve renewal loop.
const keepAliveTick = 5 * time.Second

// defaultRenewWait bounds one lease-renewal ack wait in the hold loop.
const defaultRenewWait = 30 * time.Second

// grantAckTimeout bounds one tunnel-grant mint ack wait. It is deliberately
// shorter than the grant TTL the allocator grants (the client does not know
// that TTL); a mint that does not come back in time is aborted so the caller
// can surface a clear error and retry.
const grantAckTimeout = 30 * time.Second

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

// The following methods complete application's implementation of
// localserver.Backend: the RPC surface maps onto the same durable client
// engine the direct CLI uses, so service-backed mode shares authority,
// persistence, replay, and reconnect behavior with direct mode.

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

// Tunnel mints an F14 access grant for the running execution and dials the
// allocator edge, returning the connected byte pipe the serve process relays to
// the CLI, plus the minted grant ID. It is the service-backed path for
// `r1s tunnel`: only a serve process holds the F17 keep-alive intent that keeps
// the execution alive for the session, so a direct-mode client is rejected
// before this point.
//
// The grant ID is surfaced (not dropped) so a preamble-aware edge can write the
// one-time routing header; a Conn that implements tunnel.PreambleWriter is
// handed the preamble here, before any payload byte is relayed. The in-memory
// fake does not implement it, keeping the test relay byte-clean.
func (a *application) Tunnel(ctx context.Context, executionID string) (tunnel.Conn, string, error) {
	if a.tunnelDialer == nil {
		return nil, "", errors.New("tunnel: client edge is not configured; start 'r1s serve' with the tunnel edge enabled")
	}
	peerKey, err := yggdrasil.NodePubKey(a.identity, yggdrasil.ClientNodeKeyContext)
	if err != nil {
		return nil, "", fmt.Errorf("tunnel: derive client edge key: %w", err)
	}
	destination, envelope, err := a.client.TunnelGrant(executionID, peerKey)
	if err != nil {
		return nil, "", err
	}
	ch, cancel := a.registerWaiter(envelope.GetMessageId())
	defer cancel()
	if err := a.send(destination, envelope); err != nil {
		return nil, "", fmt.Errorf("tunnel: request grant: %w", err)
	}
	ack, err := a.awaitTunnelGrantAck(ctx, executionID, ch)
	if err != nil {
		return nil, "", err
	}
	endpoint := tunnel.Endpoint{Address: ack.GetAllocatorEndpoint(), PubKey: ack.GetAllocatorEndpointPubkey()}
	if len(endpoint.Address) == 0 || len(endpoint.PubKey) == 0 {
		return nil, "", errors.New("tunnel: allocator advertised no tunnel endpoint")
	}
	conn, err := a.tunnelDialer.Dial(ctx, endpoint)
	if err != nil {
		return nil, "", fmt.Errorf("tunnel: dial allocator edge: %w", err)
	}
	if pw, ok := conn.(tunnel.PreambleWriter); ok {
		if err := pw.WritePreamble(tunnel.Preamble{ExecutionID: executionID, GrantID: ack.GetGrantId()}); err != nil {
			_ = conn.Close()
			return nil, "", fmt.Errorf("tunnel: write routing preamble: %w", err)
		}
	}
	return conn, ack.GetGrantId(), nil
}

// awaitTunnelGrantAck waits for the allocator's mint reply on the waiter
// channel registered for the grant request's correlation ID.
func (a *application) awaitTunnelGrantAck(ctx context.Context, executionID string, ch chan *r1sv1.Envelope) (*r1sv1.ExecutionTunnelGrantAck, error) {
	timer := time.NewTimer(grantAckTimeout)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			return nil, fmt.Errorf("tunnel: timed out waiting for a grant for execution %s", executionID)
		case envelope := <-ch:
			if err := client.RemoteFailure(envelope); err != nil {
				return nil, fmt.Errorf("tunnel: grant rejected: %w", err)
			}
			if ack := envelope.GetExecutionTunnelGrantAck(); ack != nil && ack.GetExecutionId() == executionID {
				return ack, nil
			}
		}
	}
}

package client

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	coreclient "github.com/mytecor/r1s/internal/client"
	"github.com/mytecor/r1s/internal/protocol"
)

const (
	defaultOfferWait     = 10 * time.Second
	defaultInspectEvery  = 5 * time.Second
	defaultOperationWait = 30 * time.Second
)

// RunOptions configures one logical run.
type RunOptions struct {
	// OfferWait is the discovery and offer-collection window for every attempt.
	// Zero uses 10 seconds.
	OfferWait time.Duration
	// LeaseDuration is the allocator-side lease requested and renewed by this
	// controller. Zero uses the protocol default.
	LeaseDuration time.Duration
	// OnEvent is called synchronously as the run is assigned, rescheduled, or
	// encounters an inconclusive renewal error. Callbacks must return promptly.
	OnEvent func(Event)
}

// EventKind classifies a run-controller observation.
type EventKind string

const (
	EventAttemptAssigned EventKind = "attempt-assigned"
	EventLeaseLost       EventKind = "lease-lost"
	EventRenewalError    EventKind = "renewal-error"
)

// Event reports progress without transferring workload stdout or stderr.
// Logs remain available only through an explicit Logs call.
type Event struct {
	Kind     EventKind
	Attempt  Attempt
	Previous string
	Err      error
}

// Attempt identifies one immutable execution attempt of a logical run.
type Attempt struct {
	RunID       string
	Number      uint64
	RequestID   string
	ExecutionID string
	Allocator   []byte
}

// Result is the terminal result of the final execution attempt.
type Result struct {
	Attempt Attempt
	State   *r1sv1.ExecutionState
}

// ExitStatus reports the workload exit status when the allocator supplied one.
func (r Result) ExitStatus() (int, bool) {
	if r.State == nil || r.State.ExitCode == nil {
		return 0, false
	}
	return int(r.State.GetExitCode()), true
}

// Run publishes demand, selects and assigns an allocator, holds the renewable
// lease, and reschedules only after authenticated conclusive loss. It blocks
// until the workload reaches a terminal state or ctx ends. Canceling ctx sends
// a best-effort authenticated execution cancellation before returning.
func (c *Client) Run(ctx context.Context, request *r1sv1.ExecutionRequest, options RunOptions) (Result, error) {
	if ctx == nil {
		return Result{}, errors.New("client: context is required")
	}
	if request == nil {
		return Result{}, errors.New("client: execution request is required")
	}
	if options.OfferWait == 0 {
		options.OfferWait = defaultOfferWait
	}
	if options.OfferWait < 0 {
		return Result{}, errors.New("client: offer wait must be positive")
	}
	if options.LeaseDuration == 0 {
		options.LeaseDuration = protocol.DefaultLease
	}
	if options.LeaseDuration < 0 {
		return Result{}, errors.New("client: lease duration must be positive")
	}
	if err := c.beginRun(); err != nil {
		return Result{}, err
	}
	defer c.endRun()

	attempt, err := c.requestAttempt(ctx, nil, request, options.OfferWait, nil, options.LeaseDuration)
	if err != nil {
		return Result{}, err
	}
	emit(options.OnEvent, Event{Kind: EventAttemptAssigned, Attempt: attempt})
	result, err := c.holdRun(ctx, attempt, options)
	if err != nil && ctx.Err() != nil {
		c.bestEffortCancel(result.Attempt.ExecutionID)
	}
	return result, err
}

func emit(handler func(Event), event Event) {
	if handler != nil {
		handler(event)
	}
}

func (c *Client) requestAttempt(ctx context.Context, previous *r1sv1.ExecutionRequest, initial *r1sv1.ExecutionRequest, offerWait time.Duration, allocators []string, leaseDuration time.Duration) (Attempt, error) {
	var (
		requestID string
		envelope  *r1sv1.Envelope
		err       error
	)
	request := initial
	if previous == nil {
		requestID, envelope, err = c.core.CreateRequestWithConstraints(request.GetWorkload(), request.GetPolicy(), request.GetResourceClass(), request.GetConstraints())
	} else {
		request = previous
		requestID, envelope, err = c.core.CreateNextAttempt(previous)
	}
	if err != nil {
		return Attempt{}, err
	}
	waiter, cancel := c.registerWaiter(envelope.GetMessageId())
	defer cancel()
	sent := make(map[string]bool)
	for _, destination := range allocators {
		if err := c.send(ctx, destination, envelope); err != nil {
			return Attempt{}, err
		}
		sent[destination] = true
	}
	timer := time.NewTimer(offerWait)
	defer timer.Stop()
collect:
	for {
		select {
		case <-ctx.Done():
			return Attempt{}, ctx.Err()
		case <-timer.C:
			break collect
		case response := <-waiter:
			if err := coreclient.RemoteFailure(response); err != nil {
				return Attempt{}, err
			}
		case service := <-c.endpoint.Discoveries():
			if service.Descriptor.Capacity[request.GetResourceClass()] == 0 {
				continue
			}
			identity, decodeErr := hex.DecodeString(service.Identity)
			if decodeErr != nil {
				continue
			}
			node := summaryNode(service.Descriptor.OS, service.Descriptor.Arch, service.Descriptor.Runtime)
			allocator := coreclient.Allocator{
				Identity: identity, Destination: service.Destination, Hops: service.Hops,
				Capacity: service.Descriptor.Capacity, Node: node,
			}
			if host, port, destination, ok := service.TunnelEndpoint(); ok {
				allocator.TunnelHost = host
				allocator.TunnelPort = port
				allocator.TunnelDestination = destination
			}
			if err := c.core.RegisterAllocator(allocator); err != nil {
				return Attempt{}, err
			}
			if !protocol.PlacementCompatible(request.GetConstraints(), node) || sent[service.Destination] {
				continue
			}
			if err := c.send(ctx, service.Destination, envelope); err == nil {
				sent[service.Destination] = true
			}
		}
	}
	destination, assignment, err := c.core.Select(requestID)
	if err != nil {
		return Attempt{}, err
	}
	executionID := assignment.GetExecutionAssign().GetExecutionId()
	// Establish the in-memory lease duty before assignment can produce a prompt
	// running or terminal state. A very short workload must not race the
	// controller into treating its already-terminal execution as an invalid
	// lease-intent registration.
	if err := c.core.RecordLeaseIntent(executionID, leaseDuration, allocators); err != nil {
		return Attempt{}, err
	}
	if err := c.send(ctx, destination, assignment); err != nil {
		return Attempt{}, err
	}
	created, ok := c.core.RequestForExecution(executionID)
	if !ok {
		return Attempt{}, fmt.Errorf("client: execution %s has no in-memory request", executionID)
	}
	snapshot, _ := c.core.Execution(executionID)
	return Attempt{
		RunID: created.GetRunId(), Number: created.GetAttempt(), RequestID: requestID,
		ExecutionID: executionID, Allocator: snapshot.Allocator,
	}, nil
}

func (c *Client) holdRun(ctx context.Context, active Attempt, options RunOptions) (Result, error) {
	ticker := time.NewTicker(defaultInspectEvery)
	defer ticker.Stop()
	for {
		if snapshot, ok := c.core.Execution(active.ExecutionID); ok && snapshot.State != nil && protocol.Terminal(snapshot.State.GetPhase()) && !c.core.LeaseIntentLost(active.ExecutionID) {
			return Result{Attempt: active, State: snapshot.State}, nil
		}
		if c.core.LeaseIntentLost(active.ExecutionID) {
			previous := active
			replacement, err := c.reRequestLost(ctx, active.ExecutionID, options)
			if err != nil {
				return Result{Attempt: active}, err
			}
			emit(options.OnEvent, Event{Kind: EventLeaseLost, Attempt: replacement, Previous: previous.ExecutionID})
			emit(options.OnEvent, Event{Kind: EventAttemptAssigned, Attempt: replacement, Previous: previous.ExecutionID})
			active = replacement
			continue
		}
		if c.core.LeaseDue(active.ExecutionID) {
			previous := active
			replacement, rerequested, err := c.renewOrReRequest(ctx, active.ExecutionID, options)
			if err != nil && !errors.Is(err, context.Canceled) {
				emit(options.OnEvent, Event{Kind: EventRenewalError, Attempt: active, Err: err})
			} else if err == nil && rerequested {
				emit(options.OnEvent, Event{Kind: EventLeaseLost, Attempt: replacement, Previous: previous.ExecutionID})
				emit(options.OnEvent, Event{Kind: EventAttemptAssigned, Attempt: replacement, Previous: previous.ExecutionID})
				active = replacement
			}
		}
		select {
		case <-ctx.Done():
			return Result{Attempt: active}, ctx.Err()
		case <-c.stateChanged:
			continue
		case <-ticker.C:
		}
		if _, err := c.Inspect(ctx, active.ExecutionID, defaultInspectEvery); err != nil && errors.Is(err, context.Canceled) {
			return Result{Attempt: active}, err
		}
	}
}

func (c *Client) renewOrReRequest(ctx context.Context, executionID string, options RunOptions) (Attempt, bool, error) {
	activeID := executionID
	rerequested := false
	for {
		destination, envelope, lost, err := c.core.Maintain(activeID, options.LeaseDuration)
		if err != nil {
			return Attempt{}, rerequested, err
		}
		if !lost {
			waiter, cancel := c.registerWaiter(envelope.GetMessageId())
			if err := c.send(ctx, destination, envelope); err != nil {
				cancel()
				return Attempt{}, rerequested, err
			}
			err = c.awaitLeaseAck(ctx, activeID, waiter, defaultOperationWait)
			cancel()
			if err == nil {
				return c.attemptFor(activeID), rerequested, nil
			}
			if !c.core.LeaseIntentLost(activeID) {
				return Attempt{}, rerequested, err
			}
		}
		replacement, err := c.reRequestLost(ctx, activeID, options)
		if err != nil {
			return Attempt{}, rerequested, err
		}
		activeID = replacement.ExecutionID
		rerequested = true
	}
}

func (c *Client) awaitLeaseAck(ctx context.Context, executionID string, waiter <-chan *r1sv1.Envelope, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return fmt.Errorf("timed out waiting for execution %s lease renewal", executionID)
		case envelope := <-waiter:
			if err := coreclient.RemoteFailure(envelope); err != nil {
				return err
			}
			if ack := envelope.GetExecutionLeaseRenewAck(); ack != nil && ack.GetExecutionId() == executionID {
				return nil
			}
		}
	}
}

func (c *Client) reRequestLost(ctx context.Context, executionID string, options RunOptions) (Attempt, error) {
	request, ok := c.core.RequestForExecution(executionID)
	if !ok {
		return Attempt{}, fmt.Errorf("%w: execution %q has no recorded request", coreclient.ErrExecutionNotFound, executionID)
	}
	allocators := c.core.AllocatorDestinations(request.GetResourceClass(), request.GetConstraints())
	replacement, err := c.requestAttempt(ctx, request, nil, options.OfferWait, allocators, options.LeaseDuration)
	if err != nil {
		return Attempt{}, fmt.Errorf("re-request after lease expiry: %w", err)
	}
	if err := c.core.RebindLeaseIntent(executionID, replacement.ExecutionID); err != nil {
		return Attempt{}, fmt.Errorf("lease intent not rebound to %s: %w", replacement.ExecutionID, err)
	}
	return replacement, nil
}

func (c *Client) attemptFor(executionID string) Attempt {
	request, _ := c.core.RequestForExecution(executionID)
	snapshot, _ := c.core.Execution(executionID)
	return Attempt{
		RunID: request.GetRunId(), Number: request.GetAttempt(), RequestID: request.GetRequestId(),
		ExecutionID: executionID, Allocator: snapshot.Allocator,
	}
}

// Inspect performs an explicit authenticated state read.
func (c *Client) Inspect(ctx context.Context, executionID string, wait time.Duration) (*r1sv1.ExecutionState, error) {
	if ctx == nil {
		return nil, errors.New("client: context is required")
	}
	if wait == 0 {
		wait = defaultOperationWait
	}
	if wait < 0 {
		return nil, errors.New("client: inspect wait must be positive")
	}
	destination, envelope, err := c.core.Inspect(executionID)
	if err != nil {
		return nil, err
	}
	waiter, cancel := c.registerWaiter(envelope.GetMessageId())
	defer cancel()
	if err := c.send(ctx, destination, envelope); err != nil {
		return nil, err
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			return nil, fmt.Errorf("timed out waiting for execution %s state", executionID)
		case response := <-waiter:
			if err := coreclient.RemoteFailure(response); err != nil {
				return nil, err
			}
			if response.GetExecutionState().GetExecutionId() == executionID {
				snapshot, _ := c.core.Execution(executionID)
				return snapshot.State, nil
			}
		}
	}
}

// Logs performs one explicit, bounded, authenticated allocator-local log read.
// It is never called implicitly by Run, inspection, completion, or recovery.
func (c *Client) Logs(ctx context.Context, executionID, stream string, offset uint64, maxBytes uint32, wait time.Duration) (*r1sv1.ExecutionLogsResponse, error) {
	if ctx == nil {
		return nil, errors.New("client: context is required")
	}
	if wait == 0 {
		wait = defaultOperationWait
	}
	if wait < 0 {
		return nil, errors.New("client: log wait must be positive")
	}
	// The protocol core deliberately tracks only one explicit log request. Keep
	// the public API concurrency-safe by serializing complete request/reply
	// exchanges instead of letting concurrent callers overwrite that authority.
	c.logMu.Lock()
	defer c.logMu.Unlock()
	destination, envelope, err := c.core.Logs(executionID, stream, offset, maxBytes)
	if err != nil {
		return nil, err
	}
	waiter, cancel := c.registerWaiter(envelope.GetMessageId())
	defer cancel()
	if err := c.send(ctx, destination, envelope); err != nil {
		return nil, err
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			return nil, fmt.Errorf("log request timed out; retry explicitly from offset %d", offset)
		case response := <-waiter:
			if err := coreclient.RemoteFailure(response); err != nil {
				return nil, err
			}
			if chunk := response.GetExecutionLogsResponse(); chunk != nil {
				return chunk, nil
			}
		}
	}
}

func (c *Client) bestEffortCancel(executionID string) {
	if executionID == "" {
		return
	}
	destination, envelope, err := c.core.Cancel(executionID, "run controller context canceled")
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = c.send(ctx, destination, envelope)
}

func summaryNode(os, arch, runtime string) *r1sv1.NodeCapabilities {
	if os == "" && arch == "" && runtime == "" {
		return nil
	}
	return &r1sv1.NodeCapabilities{Os: os, Arch: arch, Runtime: runtime}
}

package client

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/allocator"
	r1sruntime "github.com/mytecor/r1s/internal/runtime"
	"github.com/mytecor/r1s/internal/transport"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestSelectIsDeterministic(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	core, err := New(Config{Identity: []byte("client"), Now: func() time.Time { return now }, NewID: sequenceIDs("request", "request-message", "execution", "assign-message")})
	if err != nil {
		t.Fatal(err)
	}
	registerAllocator(t, core, "allocator-far", "far", 3)
	registerAllocator(t, core, "allocator-near", "near", 1)
	requestID, requestEnvelope, err := core.CreateRequest(testWorkload(), testPolicy(), "default")
	if err != nil {
		t.Fatal(err)
	}
	if requestID != "request" {
		t.Fatalf("request ID = %q", requestID)
	}
	mustObserveOffer(t, core, now, requestEnvelope, "allocator-far", "offer-far")
	mustObserveOffer(t, core, now, requestEnvelope, "allocator-near", "offer-near")
	destination, assignment, err := core.Select(requestID)
	if err != nil {
		t.Fatal(err)
	}
	if destination != "near" || assignment.GetExecutionAssign().GetOfferId() != "offer-near" {
		t.Fatalf("selection destination=%q assignment=%v", destination, assignment)
	}
	// Re-selecting the same request returns the identical assignment without
	// re-ranking (the client records the selection in memory for the run).
	repeatDestination, repeatAssignment, err := core.Select(requestID)
	if err != nil {
		t.Fatal(err)
	}
	if repeatDestination != destination || !proto.Equal(repeatAssignment, assignment) {
		t.Fatalf("re-selection changed assignment:\nfirst=%v\nrepeat=%v", assignment, repeatAssignment)
	}
	if len(core.Executions()) != 1 {
		t.Fatalf("executions = %d, want 1", len(core.Executions()))
	}
}

func TestSelectIgnoresRandomizedAllocatorAndOfferOrder(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	identities := []string{"allocator-d", "allocator-b", "allocator-a", "allocator-c"}
	for seed := int64(0); seed < 100; seed++ {
		random := rand.New(rand.NewSource(seed))
		order := random.Perm(len(identities))
		core, err := New(Config{
			Identity: []byte("client"),
			Now:      func() time.Time { return now },
			NewID:    sequenceIDs("request", "request-message", "execution", "assignment", "release-1", "release-2", "release-3"),
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, index := range order {
			identity := identities[index]
			registerAllocator(t, core, identity, "destination-"+identity, 1)
		}
		requestID, request, err := core.CreateRequest(testWorkload(), testPolicy(), "default")
		if err != nil {
			t.Fatal(err)
		}
		offerOrder := random.Perm(len(identities))
		for _, index := range offerOrder {
			identity := identities[index]
			mustObserveOffer(t, core, now, request, identity, "offer-"+identity)
		}
		destination, assignment, err := core.Select(requestID)
		if err != nil {
			t.Fatal(err)
		}
		if destination != "destination-allocator-a" || assignment.GetExecutionAssign().GetOfferId() != "offer-allocator-a" {
			t.Fatalf("seed %d selected destination=%q offer=%q", seed, destination, assignment.GetExecutionAssign().GetOfferId())
		}
		if pending := core.PendingReleases(); len(pending) != len(identities)-1 {
			t.Fatalf("seed %d pending releases=%d, want %d", seed, len(pending), len(identities)-1)
		}
	}
}

func TestCreateNextAttemptPreservesRunIdentity(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	core, err := New(Config{Identity: []byte("client"), Now: func() time.Time { return now }, NewID: sequenceIDs("request-1", "message-1", "execution-1", "assign-1", "request-2", "message-2", "execution-2", "assign-2")})
	if err != nil {
		t.Fatal(err)
	}
	registerAllocator(t, core, "allocator", "destination", 1)
	firstID, first, err := core.CreateRequestWithConstraints(testWorkload(), testPolicy(), "default", nil)
	if err != nil {
		t.Fatal(err)
	}
	mustObserveOffer(t, core, now, first, "allocator", "offer-1")
	_, firstAssignment, err := core.Select(firstID)
	if err != nil {
		t.Fatal(err)
	}
	secondID, second, err := core.CreateNextAttempt(first.GetExecutionRequest())
	if err != nil {
		t.Fatal(err)
	}
	mustObserveOffer(t, core, now, second, "allocator", "offer-2")
	_, secondAssignment, err := core.Select(secondID)
	if err != nil {
		t.Fatal(err)
	}
	firstRequest := first.GetExecutionRequest()
	secondRequest := second.GetExecutionRequest()
	if secondRequest.GetRunId() != firstRequest.GetRunId() || secondRequest.GetAttempt() != 2 {
		t.Fatalf("next attempt run=%q attempt=%d, want run=%q attempt=2", secondRequest.GetRunId(), secondRequest.GetAttempt(), firstRequest.GetRunId())
	}
	if secondRequest.GetRequestId() == firstRequest.GetRequestId() || second.GetMessageId() == first.GetMessageId() {
		t.Fatal("next attempt reused a request or message ID")
	}
	if secondAssignment.GetExecutionAssign().GetExecutionId() == firstAssignment.GetExecutionAssign().GetExecutionId() {
		t.Fatal("next attempt reused an execution ID")
	}
	if !proto.Equal(secondRequest.GetWorkload(), firstRequest.GetWorkload()) || !proto.Equal(secondRequest.GetConstraints(), firstRequest.GetConstraints()) {
		t.Fatal("next attempt changed the workload or placement constraints")
	}
}

func TestHandleRejectsUnknownAllocatorAndStaleState(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	core, err := New(Config{Identity: []byte("client"), Now: func() time.Time { return now }, NewID: sequenceIDs("request", "request-message", "execution", "assign-message")})
	if err != nil {
		t.Fatal(err)
	}
	registerAllocator(t, core, "allocator", "destination", 1)
	requestID, requestEnvelope, err := core.CreateRequest(testWorkload(), testPolicy(), "default")
	if err != nil {
		t.Fatal(err)
	}
	unknown := offerEnvelope(now, requestEnvelope, "unknown", "offer-unknown")
	if err := core.Handle(context.Background(), unknown); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("unknown allocator error = %v", err)
	}
	mustObserveOffer(t, core, now, requestEnvelope, "allocator", "offer")
	_, assignment, err := core.Select(requestID)
	if err != nil {
		t.Fatal(err)
	}
	executionID := assignment.GetExecutionAssign().GetExecutionId()
	running := stateEnvelope(now.Add(time.Minute), "allocator", executionID, r1sv1.ExecutionPhase_EXECUTION_PHASE_RUNNING, 0)
	if err := core.Handle(context.Background(), running); err != nil {
		t.Fatal(err)
	}
	stale := stateEnvelope(now.Add(30*time.Second), "allocator", executionID, r1sv1.ExecutionPhase_EXECUTION_PHASE_STARTING, 0)
	if err := core.Handle(context.Background(), stale); err != nil {
		t.Fatal(err)
	}
	snapshot, _ := core.Execution(executionID)
	if snapshot.State.GetPhase() != r1sv1.ExecutionPhase_EXECUTION_PHASE_RUNNING {
		t.Fatalf("stale state regressed execution: %v", snapshot.State)
	}
	forged := stateEnvelope(now.Add(2*time.Minute), "other", executionID, r1sv1.ExecutionPhase_EXECUTION_PHASE_COMPLETED, 0)
	if err := core.Handle(context.Background(), forged); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("forged state error = %v", err)
	}
}

func TestTwoAllocatorWorkflowAndRetainedTerminalState(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	network := transport.NewMemory()
	clientCore, err := New(Config{Identity: []byte("client"), Now: func() time.Time { return now }, NewID: sequenceIDs("request", "request-message", "execution", "assign-message", "inspect-message")})
	if err != nil {
		t.Fatal(err)
	}
	clientEndpoint, err := network.Register("client", func(ctx context.Context, envelope *r1sv1.Envelope) error {
		return clientCore.Handle(ctx, envelope)
	})
	if err != nil {
		t.Fatal(err)
	}
	runtimes := map[string]*testRuntime{"allocator-a": {}, "allocator-b": {}}
	cores := make(map[string]*allocator.Allocator)
	for index, name := range []string{"allocator-a", "allocator-b"} {
		name := name
		var endpoint *transport.MemoryEndpoint
		allocatorCore, err := allocator.New(allocator.Config{
			Identity: []byte(name), Capacity: map[string]uint32{"default": 1}, Now: func() time.Time { return now },
			NewID: sequenceIDs(fmt.Sprintf("offer-%d", index), fmt.Sprintf("response-%d", index), fmt.Sprintf("state-%d", index), fmt.Sprintf("inspect-state-%d", index)),
		}, runtimes[name])
		if err != nil {
			t.Fatal(err)
		}
		cores[name] = allocatorCore
		endpoint, err = network.Register(name, func(ctx context.Context, envelope *r1sv1.Envelope) error {
			responses, handleErr := allocatorCore.Handle(ctx, envelope)
			for _, response := range responses {
				if sendErr := endpoint.Send(ctx, "client", response); sendErr != nil {
					return sendErr
				}
			}
			return handleErr
		})
		if err != nil {
			t.Fatal(err)
		}
		registerAllocator(t, clientCore, name, name, uint8(2-index))
	}

	requestID, requestEnvelope, err := clientCore.CreateRequest(testWorkload(), testPolicy(), "default")
	if err != nil {
		t.Fatal(err)
	}
	for _, destination := range []string{"allocator-a", "allocator-b"} {
		if err := clientEndpoint.Send(context.Background(), destination, requestEnvelope); err != nil {
			t.Fatal(err)
		}
	}
	destination, assignment, err := clientCore.Select(requestID)
	if err != nil {
		t.Fatal(err)
	}
	if destination != "allocator-b" {
		t.Fatalf("selected %q, want allocator-b", destination)
	}
	// Deliver loser cleanup before assignment to prove ordering cannot free the winner.
	for _, release := range clientCore.PendingReleases() {
		if err := clientEndpoint.Send(context.Background(), release.Destination, release.Envelope); err != nil {
			t.Fatal(err)
		}
	}
	if cores["allocator-a"].Available("default") != 1 || cores["allocator-b"].Available("default") != 0 || len(clientCore.PendingReleases()) != 0 {
		t.Fatal("release did not immediately return only losing capacity")
	}
	if err := clientEndpoint.Send(context.Background(), destination, assignment); err != nil {
		t.Fatal(err)
	}
	executionID := assignment.GetExecutionAssign().GetExecutionId()
	now = now.Add(time.Second)
	exitCode := int32(7)
	if err := runtimes[destination].complete(executionID, &exitCode); err != nil {
		t.Fatal(err)
	}
	_, inspect, err := clientCore.Inspect(executionID)
	if err != nil {
		t.Fatal(err)
	}
	if err := clientEndpoint.Send(context.Background(), destination, inspect); err != nil {
		t.Fatal(err)
	}
	snapshot, ok := clientCore.Execution(executionID)
	if !ok || snapshot.State.GetPhase() != r1sv1.ExecutionPhase_EXECUTION_PHASE_COMPLETED || snapshot.State.GetExitCode() != exitCode {
		t.Fatalf("retained result = %+v", snapshot)
	}
}

func registerAllocator(t *testing.T, core *Client, identity, destination string, hops uint8) {
	t.Helper()
	if err := core.RegisterAllocator(Allocator{Identity: []byte(identity), Destination: destination, Hops: hops, Capacity: map[string]uint32{"default": 1}}); err != nil {
		t.Fatal(err)
	}
}

func mustObserveOffer(t *testing.T, core *Client, now time.Time, request *r1sv1.Envelope, allocatorID, offerID string) {
	t.Helper()
	if err := core.Handle(context.Background(), offerEnvelope(now, request, allocatorID, offerID)); err != nil {
		t.Fatal(err)
	}
}

func offerEnvelope(now time.Time, request *r1sv1.Envelope, allocatorID, offerID string) *r1sv1.Envelope {
	return &r1sv1.Envelope{
		MessageId: "message-" + offerID, Sender: []byte(allocatorID), CorrelationId: request.GetMessageId(), SentAt: timestamppb.New(now),
		Payload: &r1sv1.Envelope_ExecutionOffer{ExecutionOffer: &r1sv1.ExecutionOffer{
			OfferId: offerID, RequestId: request.GetExecutionRequest().GetRequestId(), ResourceClass: "default", ExpiresAt: timestamppb.New(now.Add(time.Minute)),
		}},
	}
}

func stateEnvelope(now time.Time, allocatorID, executionID string, phase r1sv1.ExecutionPhase, exitCode int32) *r1sv1.Envelope {
	state := &r1sv1.ExecutionState{ExecutionId: executionID, Phase: phase, OccurredAt: timestamppb.New(now)}
	if phase == r1sv1.ExecutionPhase_EXECUTION_PHASE_COMPLETED {
		state.ExitCode = &exitCode
	}
	return &r1sv1.Envelope{
		MessageId: fmt.Sprintf("state-%d", now.UnixNano()), Sender: []byte(allocatorID), SentAt: timestamppb.New(now),
		Payload: &r1sv1.Envelope_ExecutionState{ExecutionState: state},
	}
}

func testWorkload() *r1sv1.Workload {
	return &r1sv1.Workload{Image: "example.test/workload@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
}

func testPolicy() *r1sv1.ExecutionPolicy {
	return &r1sv1.ExecutionPolicy{}
}

func sequenceIDs(values ...string) func() string {
	var mu sync.Mutex
	index := 0
	return func() string {
		mu.Lock()
		defer mu.Unlock()
		if index >= len(values) {
			value := fmt.Sprintf("generated-%d", index)
			index++
			return value
		}
		value := values[index]
		index++
		return value
	}
}

type testRuntime struct {
	mu        sync.Mutex
	reporters map[string]r1sruntime.Reporter
}

func (r *testRuntime) Start(_ context.Context, request r1sruntime.StartRequest, reporter r1sruntime.Reporter) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.reporters == nil {
		r.reporters = make(map[string]r1sruntime.Reporter)
	}
	if r.reporters[request.ExecutionID] == nil {
		r.reporters[request.ExecutionID] = reporter
	}
	return nil
}

func (r *testRuntime) Stop(context.Context, string) error { return nil }

func (r *testRuntime) complete(executionID string, exitCode *int32) error {
	r.mu.Lock()
	reporter := r.reporters[executionID]
	r.mu.Unlock()
	if reporter == nil {
		return errors.New("missing reporter")
	}
	return reporter(r1sruntime.Completion{ExecutionID: executionID, ExitCode: exitCode})
}

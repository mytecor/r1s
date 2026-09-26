package allocator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
	r1sruntime "github.com/mytecor/r1s/internal/runtime"
	statebolt "github.com/mytecor/r1s/internal/store/bolt"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestLifecycleAndClientAuthority(t *testing.T) {
	clock := newFakeClock()
	runtime := newFakeRuntime()
	allocator := newTestAllocator(t, clock, runtime, 1)

	request := requestEnvelope(clock.Now(), "request-message", "client", "request")
	offerResponse := mustHandle(t, allocator, request)
	offer := offerResponse.GetExecutionOffer()
	if offer == nil || offer.GetRequestId() != "request" {
		t.Fatalf("response = %v, want offer", offerResponse)
	}
	if available := allocator.Available("default"); available != 0 {
		t.Fatalf("available after offer = %d, want 0", available)
	}

	assign := assignEnvelope(clock.Now(), "assign-message", "client", "request", offer.GetOfferId(), "execution")
	stateResponse := mustHandle(t, allocator, assign)
	if phase := stateResponse.GetExecutionState().GetPhase(); phase != r1sv1.ExecutionPhase_EXECUTION_PHASE_RUNNING {
		t.Fatalf("phase after assignment = %v, want running", phase)
	}
	if runtime.startCount() != 1 {
		t.Fatalf("runtime starts = %d, want 1", runtime.startCount())
	}

	unauthorized := cancelEnvelope(clock.Now(), "bad-cancel", "other-client", "execution")
	if _, err := allocator.Handle(context.Background(), unauthorized); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("unauthorized cancel error = %v", err)
	}
	if _, err := allocator.Handle(context.Background(), unauthorized); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("replayed unauthorized cancel error = %v", err)
	}
	if runtime.stopCount() != 0 {
		t.Fatalf("runtime stops after unauthorized cancel = %d, want 0", runtime.stopCount())
	}

	cancel := cancelEnvelope(clock.Now(), "cancel-message", "client", "execution")
	cancelResponse := mustHandle(t, allocator, cancel)
	if phase := cancelResponse.GetExecutionState().GetPhase(); phase != r1sv1.ExecutionPhase_EXECUTION_PHASE_CANCELLED {
		t.Fatalf("phase after cancellation = %v, want cancelled", phase)
	}
	if runtime.stopCount() != 1 {
		t.Fatalf("runtime stops = %d, want 1", runtime.stopCount())
	}
	if available := allocator.Available("default"); available != 1 {
		t.Fatalf("available after cancellation = %d, want 1", available)
	}
}

func TestOutstandingOffersConsumeAndReleaseCapacity(t *testing.T) {
	clock := newFakeClock()
	allocator := newTestAllocator(t, clock, newFakeRuntime(), 1)

	mustHandle(t, allocator, requestEnvelope(clock.Now(), "message-1", "client-1", "request-1"))
	_, err := allocator.Handle(context.Background(), requestEnvelope(clock.Now(), "message-2", "client-2", "request-2"))
	if !errors.Is(err, ErrCapacityExhausted) {
		t.Fatalf("second request error = %v, want ErrCapacityExhausted", err)
	}

	clock.Advance(time.Minute)
	if expired := allocator.SweepExpired(); expired != 1 {
		t.Fatalf("expired offers = %d, want 1", expired)
	}
	if available := allocator.Available("default"); available != 1 {
		t.Fatalf("available after expiry = %d, want 1", available)
	}
	mustHandle(t, allocator, requestEnvelope(clock.Now(), "message-3", "client-2", "request-2"))
}

func TestExpiredOfferCannotBeAssigned(t *testing.T) {
	clock := newFakeClock()
	allocator := newTestAllocator(t, clock, newFakeRuntime(), 1)
	offer := mustHandle(t, allocator, requestEnvelope(clock.Now(), "request-message", "client", "request")).GetExecutionOffer()
	clock.Advance(time.Minute)

	assign := assignEnvelope(clock.Now(), "assign-message", "client", "request", offer.GetOfferId(), "execution")
	if _, err := allocator.Handle(context.Background(), assign); !errors.Is(err, ErrOfferExpired) {
		t.Fatalf("expired assignment error = %v, want ErrOfferExpired", err)
	}
}

func TestDuplicateMessagesAreIdempotent(t *testing.T) {
	clock := newFakeClock()
	runtime := newFakeRuntime()
	allocator := newTestAllocator(t, clock, runtime, 1)

	request := requestEnvelope(clock.Now(), "request-message", "client", "request")
	firstOffer := mustHandle(t, allocator, request)
	secondOffer := mustHandle(t, allocator, request)
	if !proto.Equal(firstOffer, secondOffer) {
		t.Fatalf("duplicate request response changed:\nfirst: %v\nsecond: %v", firstOffer, secondOffer)
	}

	assign := assignEnvelope(clock.Now(), "assign-message", "client", "request", firstOffer.GetExecutionOffer().GetOfferId(), "execution")
	firstState := mustHandle(t, allocator, assign)
	secondState := mustHandle(t, allocator, assign)
	if !proto.Equal(firstState, secondState) || runtime.startCount() != 1 {
		t.Fatalf("duplicate assignment was not replayed: starts=%d", runtime.startCount())
	}

	cancel := cancelEnvelope(clock.Now(), "cancel-message", "client", "execution")
	firstCancel := mustHandle(t, allocator, cancel)
	secondCancel := mustHandle(t, allocator, cancel)
	if !proto.Equal(firstCancel, secondCancel) || runtime.stopCount() != 1 {
		t.Fatalf("duplicate cancellation was not replayed: stops=%d", runtime.stopCount())
	}
}

func TestLogicalDuplicateAssignmentDoesNotRestart(t *testing.T) {
	clock := newFakeClock()
	runtime := newFakeRuntime()
	allocator := newTestAllocator(t, clock, runtime, 1)
	offer := mustHandle(t, allocator, requestEnvelope(clock.Now(), "request-message", "client", "request")).GetExecutionOffer()
	mustHandle(t, allocator, assignEnvelope(clock.Now(), "assign-1", "client", "request", offer.GetOfferId(), "execution"))
	response := mustHandle(t, allocator, assignEnvelope(clock.Now(), "assign-2", "client", "request", offer.GetOfferId(), "execution"))
	if response.GetExecutionState().GetPhase() != r1sv1.ExecutionPhase_EXECUTION_PHASE_RUNNING || runtime.startCount() != 1 {
		t.Fatalf("logical duplicate restarted execution: state=%v starts=%d", response, runtime.startCount())
	}
}

func TestInspectReturnsLatestStateAndChecksClient(t *testing.T) {
	clock := newFakeClock()
	allocator := newTestAllocator(t, clock, newFakeRuntime(), 1)
	offer := mustHandle(t, allocator, requestEnvelope(clock.Now(), "request-message", "client", "request")).GetExecutionOffer()
	mustHandle(t, allocator, assignEnvelope(clock.Now(), "assign-message", "client", "request", offer.GetOfferId(), "execution"))

	inspect := inspectEnvelope(clock.Now(), "inspect-message", "client", "execution")
	response := mustHandle(t, allocator, inspect)
	if response.GetCorrelationId() != inspect.GetMessageId() || response.GetExecutionState().GetPhase() != r1sv1.ExecutionPhase_EXECUTION_PHASE_RUNNING {
		t.Fatalf("inspect response = %v", response)
	}
	if _, err := allocator.Handle(context.Background(), inspectEnvelope(clock.Now(), "bad-inspect", "other-client", "execution")); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("unauthorized inspect error = %v, want ErrUnauthorized", err)
	}
}

func TestReusedRequestAndMessageIDsRejectConflictingContent(t *testing.T) {
	clock := newFakeClock()
	allocator := newTestAllocator(t, clock, newFakeRuntime(), 2)
	original := requestEnvelope(clock.Now(), "message", "client", "request")
	mustHandle(t, allocator, original)

	reusedRequest := requestEnvelope(clock.Now(), "different-message", "client", "request")
	reusedRequest.GetExecutionRequest().Workload.Image = "example.test/different:latest"
	if _, err := allocator.Handle(context.Background(), reusedRequest); !errors.Is(err, ErrExecutionConflict) {
		t.Fatalf("reused request ID error = %v, want ErrExecutionConflict", err)
	}
	reusedRun := requestEnvelope(clock.Now(), "run-message", "client", "request")
	reusedRun.GetExecutionRequest().RunId = "fedcba9876543210fedcba9876543210"
	if _, err := allocator.Handle(context.Background(), reusedRun); !errors.Is(err, ErrExecutionConflict) {
		t.Fatalf("mutated run ID error = %v, want ErrExecutionConflict", err)
	}
	reusedAttempt := requestEnvelope(clock.Now(), "attempt-message", "client", "request")
	reusedAttempt.GetExecutionRequest().Attempt = 2
	if _, err := allocator.Handle(context.Background(), reusedAttempt); !errors.Is(err, ErrExecutionConflict) {
		t.Fatalf("mutated attempt error = %v, want ErrExecutionConflict", err)
	}
	reusedMessage := requestEnvelope(clock.Now(), "message", "client", "different-request")
	if _, err := allocator.Handle(context.Background(), reusedMessage); !errors.Is(err, ErrReplayConflict) {
		t.Fatalf("reused message ID error = %v, want ErrReplayConflict", err)
	}
	if available := allocator.Available("default"); available != 1 {
		t.Fatalf("conflicts changed capacity: available=%d, want 1", available)
	}
}

func TestConcurrentDuplicateAssignmentStartsOnce(t *testing.T) {
	clock := newFakeClock()
	runtime := newFakeRuntime()
	runtime.blockStart = make(chan struct{})
	allocator := newTestAllocator(t, clock, runtime, 1)
	offer := mustHandle(t, allocator, requestEnvelope(clock.Now(), "request-message", "client", "request")).GetExecutionOffer()
	assign := assignEnvelope(clock.Now(), "assign-message", "client", "request", offer.GetOfferId(), "execution")

	const callers = 16
	var wait sync.WaitGroup
	errorsSeen := make(chan error, callers)
	for range callers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			responses, err := allocator.Handle(context.Background(), assign)
			if err == nil && (len(responses) != 1 || responses[0].GetExecutionState().GetPhase() != r1sv1.ExecutionPhase_EXECUTION_PHASE_RUNNING) {
				err = fmt.Errorf("unexpected responses: %v", responses)
			}
			errorsSeen <- err
		}()
	}
	<-runtime.started
	close(runtime.blockStart)
	wait.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		if err != nil {
			t.Errorf("Handle() error = %v", err)
		}
	}
	if runtime.startCount() != 1 {
		t.Fatalf("runtime starts = %d, want 1", runtime.startCount())
	}
}

func TestInvalidEnvelopeAndUnauthorizedAssignmentDoNotMutate(t *testing.T) {
	clock := newFakeClock()
	runtime := newFakeRuntime()
	allocator := newTestAllocator(t, clock, runtime, 1)
	invalid := requestEnvelope(clock.Now(), "invalid", "client", "request")
	invalid.GetExecutionRequest().Workload.Image = ""
	if _, err := allocator.Handle(context.Background(), invalid); !errors.Is(err, protocol.ErrInvalidEnvelope) {
		t.Fatalf("invalid request error = %v", err)
	}
	if available := allocator.Available("default"); available != 1 {
		t.Fatalf("invalid envelope consumed capacity: available=%d", available)
	}

	offer := mustHandle(t, allocator, requestEnvelope(clock.Now(), "request-message", "client", "request")).GetExecutionOffer()
	assign := assignEnvelope(clock.Now(), "assign-message", "attacker", "request", offer.GetOfferId(), "execution")
	if _, err := allocator.Handle(context.Background(), assign); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("unauthorized assignment error = %v", err)
	}
	if runtime.startCount() != 0 {
		t.Fatalf("unauthorized assignment started runtime %d times", runtime.startCount())
	}
}

func TestRuntimeStartFailureReleasesCapacity(t *testing.T) {
	clock := newFakeClock()
	runtime := newFakeRuntime()
	runtime.startErr = errors.New("image unavailable")
	allocator := newTestAllocator(t, clock, runtime, 1)
	offer := mustHandle(t, allocator, requestEnvelope(clock.Now(), "request-message", "client", "request")).GetExecutionOffer()

	responses, err := allocator.Handle(context.Background(), assignEnvelope(clock.Now(), "assign-message", "client", "request", offer.GetOfferId(), "execution"))
	if !errors.Is(err, ErrRuntimeStart) {
		t.Fatalf("assignment error = %v, want ErrRuntimeStart", err)
	}
	if len(responses) != 1 || responses[0].GetExecutionState().GetPhase() != r1sv1.ExecutionPhase_EXECUTION_PHASE_FAILED {
		t.Fatalf("failure response = %v", responses)
	}
	if available := allocator.Available("default"); available != 1 {
		t.Fatalf("available after start failure = %d, want 1", available)
	}
}

func TestRuntimeCompletionAndFailureCallbacks(t *testing.T) {
	for _, test := range []struct {
		name  string
		err   error
		phase r1sv1.ExecutionPhase
	}{
		{name: "completion", phase: r1sv1.ExecutionPhase_EXECUTION_PHASE_COMPLETED},
		{name: "failure", err: errors.New("process crashed"), phase: r1sv1.ExecutionPhase_EXECUTION_PHASE_FAILED},
	} {
		t.Run(test.name, func(t *testing.T) {
			clock := newFakeClock()
			runtime := newFakeRuntime()
			allocator := newTestAllocator(t, clock, runtime, 1)
			offer := mustHandle(t, allocator, requestEnvelope(clock.Now(), "request-message", "client", "request")).GetExecutionOffer()
			mustHandle(t, allocator, assignEnvelope(clock.Now(), "assign-message", "client", "request", offer.GetOfferId(), "execution"))
			exitCode := int32(17)
			if err := runtime.complete("execution", r1sruntime.Completion{Err: test.err, ExitCode: &exitCode}); err != nil {
				t.Fatalf("completion callback error = %v", err)
			}
			snapshot, ok := allocator.Execution("execution")
			if !ok || snapshot.State.GetPhase() != test.phase || snapshot.State.GetExitCode() != exitCode {
				t.Fatalf("execution snapshot = %+v, want phase %v and exit %d", snapshot, test.phase, exitCode)
			}
			if available := allocator.Available("default"); available != 1 {
				t.Fatalf("available after terminal callback = %d, want 1", available)
			}
		})
	}
}

func TestRuntimeStopFailureCanBeRetriedWithNewMessage(t *testing.T) {
	clock := newFakeClock()
	runtime := newFakeRuntime()
	runtime.stopErr = errors.New("temporary stop failure")
	allocator := newTestAllocator(t, clock, runtime, 1)
	offer := mustHandle(t, allocator, requestEnvelope(clock.Now(), "request-message", "client", "request")).GetExecutionOffer()
	mustHandle(t, allocator, assignEnvelope(clock.Now(), "assign-message", "client", "request", offer.GetOfferId(), "execution"))

	if _, err := allocator.Handle(context.Background(), cancelEnvelope(clock.Now(), "cancel-1", "client", "execution")); !errors.Is(err, ErrRuntimeStop) {
		t.Fatalf("cancel error = %v, want ErrRuntimeStop", err)
	}
	snapshot, _ := allocator.Execution("execution")
	if snapshot.State.GetPhase() != r1sv1.ExecutionPhase_EXECUTION_PHASE_RUNNING || allocator.Available("default") != 0 {
		t.Fatalf("failed stop changed state or capacity: %+v", snapshot)
	}
	runtime.setStopError(nil)
	mustHandle(t, allocator, cancelEnvelope(clock.Now(), "cancel-2", "client", "execution"))
	if runtime.stopCount() != 2 || allocator.Available("default") != 1 {
		t.Fatalf("stop retry: calls=%d available=%d", runtime.stopCount(), allocator.Available("default"))
	}
}

func TestReplayRetentionIsBounded(t *testing.T) {
	clock := newFakeClock()
	allocator, err := New(Config{
		Identity:       []byte("allocator"),
		Capacity:       map[string]uint32{"default": 1},
		ReplayCapacity: 2,
		ReplayTTL:      time.Minute,
		Now:            clock.Now,
		NewID:          sequenceIDs(),
	}, newFakeRuntime())
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 10; index++ {
		envelope := requestEnvelope(clock.Now(), fmt.Sprintf("message-%d", index), "client", fmt.Sprintf("request-%d", index))
		envelope.GetExecutionRequest().ResourceClass = "unknown"
		if _, err := allocator.Handle(context.Background(), envelope); !errors.Is(err, ErrCapacityExhausted) {
			t.Fatalf("request %d error = %v", index, err)
		}
	}
	allocator.mu.Lock()
	entries := len(allocator.replay.entries)
	allocator.mu.Unlock()
	if entries > 2 {
		t.Fatalf("replay entries = %d, want at most 2", entries)
	}
}

func TestDurableStateRecoversRunningExecutionAndReplay(t *testing.T) {
	clock := newFakeClock()
	path := t.TempDir() + "/allocator.db"
	firstStore, err := statebolt.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	firstRuntime := newFakeRuntime()
	first, err := New(Config{
		Identity: []byte("allocator"), Capacity: map[string]uint32{"default": 1},
		OfferTTL: 30 * time.Second, Now: clock.Now, NewID: sequenceIDs(), Store: firstStore,
	}, firstRuntime)
	if err != nil {
		t.Fatal(err)
	}
	request := requestEnvelope(clock.Now(), "request-message", "client", "request")
	offerResponse := mustHandle(t, first, request)
	offer := offerResponse.GetExecutionOffer()
	mustHandle(t, first, assignEnvelope(clock.Now(), "assign-message", "client", "request", offer.GetOfferId(), "execution"))
	if err := firstStore.Close(); err != nil {
		t.Fatal(err)
	}

	secondStore, err := statebolt.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer secondStore.Close()
	secondRuntime := newFakeRuntime()
	second, err := New(Config{
		Identity: []byte("allocator"), Capacity: map[string]uint32{"default": 1},
		OfferTTL: 30 * time.Second, Now: clock.Now, NewID: sequenceIDs(), Store: secondStore,
	}, secondRuntime)
	if err != nil {
		t.Fatal(err)
	}
	if available := second.Available("default"); available != 0 {
		t.Fatalf("available after restart = %d, want 0", available)
	}
	replayed := mustHandle(t, second, request)
	if !proto.Equal(replayed, offerResponse) {
		t.Fatalf("durable replay changed response:\nfirst: %v\nsecond: %v", offerResponse, replayed)
	}
	if err := second.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if secondRuntime.recoverCount() != 1 || secondRuntime.startCount() != 0 {
		t.Fatalf("recovery calls=%d starts=%d, want 1 and 0", secondRuntime.recoverCount(), secondRuntime.startCount())
	}
	secondRuntime.mu.Lock()
	recoveredRequest := secondRuntime.recoveries[0]
	secondRuntime.mu.Unlock()
	if recoveredRequest.RunID != request.GetExecutionRequest().GetRunId() || recoveredRequest.Attempt != request.GetExecutionRequest().GetAttempt() {
		t.Fatalf("recovered run metadata = %q/%d, want %q/%d", recoveredRequest.RunID, recoveredRequest.Attempt, request.GetExecutionRequest().GetRunId(), request.GetExecutionRequest().GetAttempt())
	}
	exitCode := int32(23)
	if err := secondRuntime.complete("execution", r1sruntime.Completion{ExitCode: &exitCode}); err != nil {
		t.Fatal(err)
	}
	snapshot, ok := second.Execution("execution")
	if !ok || snapshot.State.GetPhase() != r1sv1.ExecutionPhase_EXECUTION_PHASE_COMPLETED || snapshot.State.GetExitCode() != exitCode {
		t.Fatalf("recovered completion = %+v", snapshot)
	}
	if available := second.Available("default"); available != 1 {
		t.Fatalf("available after recovered completion = %d, want 1", available)
	}
}

func TestRecoveryResolvesOfflineCompletionAndMissingRuntime(t *testing.T) {
	for _, test := range []struct {
		name       string
		recoverErr error
		completion *r1sruntime.Completion
		phase      r1sv1.ExecutionPhase
		exitCode   int32
	}{
		{name: "offline completion", completion: &r1sruntime.Completion{ExitCode: int32Pointer(7)}, phase: r1sv1.ExecutionPhase_EXECUTION_PHASE_COMPLETED, exitCode: 7},
		{name: "missing", recoverErr: r1sruntime.ErrExecutionMissing, phase: r1sv1.ExecutionPhase_EXECUTION_PHASE_FAILED},
		{name: "conflicting labels", recoverErr: r1sruntime.ErrExecutionConflict, phase: r1sv1.ExecutionPhase_EXECUTION_PHASE_FAILED},
	} {
		t.Run(test.name, func(t *testing.T) {
			clock := newFakeClock()
			store := newMemoryStateStore()
			first := newStoredTestAllocator(t, clock, newFakeRuntime(), store)
			offer := mustHandle(t, first, requestEnvelope(clock.Now(), "request-message", "client", "request")).GetExecutionOffer()
			mustHandle(t, first, assignEnvelope(clock.Now(), "assign-message", "client", "request", offer.GetOfferId(), "execution"))

			recoveredRuntime := newFakeRuntime()
			recoveredRuntime.recoverErr = test.recoverErr
			recoveredRuntime.recoverCompletion = test.completion
			restarted := newStoredTestAllocator(t, clock, recoveredRuntime, store)
			if err := restarted.Recover(context.Background()); err != nil {
				t.Fatal(err)
			}
			snapshot, _ := restarted.Execution("execution")
			if snapshot.State.GetPhase() != test.phase || snapshot.State.GetExitCode() != test.exitCode {
				t.Fatalf("recovered state = %v, want phase=%v exit=%d", snapshot.State, test.phase, test.exitCode)
			}
			if restarted.Available("default") != 1 {
				t.Fatal("terminal recovery did not release capacity")
			}
		})
	}
}

func TestDurableStateRejectsDifferentAllocatorIdentity(t *testing.T) {
	clock := newFakeClock()
	store := newMemoryStateStore()
	first := newStoredTestAllocator(t, clock, newFakeRuntime(), store)
	mustHandle(t, first, requestEnvelope(clock.Now(), "message", "client", "request"))
	_, err := New(Config{Identity: []byte("different"), Capacity: map[string]uint32{"default": 1}, Store: store}, newFakeRuntime())
	if !errors.Is(err, ErrStore) {
		t.Fatalf("identity mismatch error = %v, want ErrStore", err)
	}
}

func TestDurableReplayPreservesClassifiableErrors(t *testing.T) {
	clock := newFakeClock()
	store := newMemoryStateStore()
	first := newStoredTestAllocator(t, clock, newFakeRuntime(), store)
	offer := mustHandle(t, first, requestEnvelope(clock.Now(), "request", "client", "request")).GetExecutionOffer()
	unauthorized := assignEnvelope(clock.Now(), "assign", "attacker", "request", offer.GetOfferId(), "execution")
	if _, err := first.Handle(context.Background(), unauthorized); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("first error = %v, want ErrUnauthorized", err)
	}
	restarted := newStoredTestAllocator(t, clock, newFakeRuntime(), store)
	if _, err := restarted.Handle(context.Background(), unauthorized); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("replayed durable error = %v, want ErrUnauthorized", err)
	}
}

func mustHandle(t *testing.T, allocator *Allocator, envelope *r1sv1.Envelope) *r1sv1.Envelope {
	t.Helper()
	responses, err := allocator.Handle(context.Background(), envelope)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if len(responses) != 1 {
		t.Fatalf("Handle() responses = %d, want 1", len(responses))
	}
	return responses[0]
}

func newTestAllocator(t *testing.T, clock *fakeClock, runtime *fakeRuntime, slots uint32) *Allocator {
	t.Helper()
	allocator, err := New(Config{
		Identity: []byte("allocator"),
		Capacity: map[string]uint32{"default": slots},
		OfferTTL: 30 * time.Second,
		Now:      clock.Now,
		NewID:    sequenceIDs(),
	}, runtime)
	if err != nil {
		t.Fatal(err)
	}
	return allocator
}

func requestEnvelope(now time.Time, messageID, client, requestID string) *r1sv1.Envelope {
	return &r1sv1.Envelope{
		MessageId: messageID,
		Sender:    []byte(client),
		SentAt:    timestamppb.New(now),
		Payload: &r1sv1.Envelope_ExecutionRequest{ExecutionRequest: &r1sv1.ExecutionRequest{
			RequestId:     requestID,
			RunId:         "0123456789abcdef0123456789abcdef",
			Attempt:       1,
			ResourceClass: "default",
			Workload:      &r1sv1.Workload{Image: "example.test/image:latest"},
			Policy:        &r1sv1.ExecutionPolicy{},
		}},
	}
}

func assignEnvelope(now time.Time, messageID, client, requestID, offerID, executionID string) *r1sv1.Envelope {
	return &r1sv1.Envelope{
		MessageId: messageID,
		Sender:    []byte(client),
		SentAt:    timestamppb.New(now),
		Payload: &r1sv1.Envelope_ExecutionAssign{ExecutionAssign: &r1sv1.ExecutionAssign{
			RequestId: requestID, OfferId: offerID, ExecutionId: executionID,
		}},
	}
}

func cancelEnvelope(now time.Time, messageID, client, executionID string) *r1sv1.Envelope {
	return &r1sv1.Envelope{
		MessageId: messageID,
		Sender:    []byte(client),
		SentAt:    timestamppb.New(now),
		Payload: &r1sv1.Envelope_ExecutionCancel{ExecutionCancel: &r1sv1.ExecutionCancel{
			ExecutionId: executionID, Reason: "user requested",
		}},
	}
}

func inspectEnvelope(now time.Time, messageID, client, executionID string) *r1sv1.Envelope {
	return &r1sv1.Envelope{
		MessageId: messageID,
		Sender:    []byte(client),
		SentAt:    timestamppb.New(now),
		Payload: &r1sv1.Envelope_ExecutionInspect{
			ExecutionInspect: &r1sv1.ExecutionInspect{ExecutionId: executionID},
		},
	}
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Unix(1_700_000_000, 0).UTC()}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	c.mu.Unlock()
}

func sequenceIDs() func() string {
	var next atomic.Uint64
	return func() string { return fmt.Sprintf("generated-%d", next.Add(1)) }
}

type fakeRuntime struct {
	mu                sync.Mutex
	starts            []r1sruntime.StartRequest
	stops             []string
	reporters         map[string]r1sruntime.Reporter
	startErr          error
	stopErr           error
	recoverErr        error
	recoverCompletion *r1sruntime.Completion
	recoveries        []r1sruntime.StartRequest
	blockStart        chan struct{}
	started           chan struct{}
}

func newFakeRuntime() *fakeRuntime {
	return &fakeRuntime{reporters: make(map[string]r1sruntime.Reporter), started: make(chan struct{}, 1)}
}

func (r *fakeRuntime) Start(ctx context.Context, request r1sruntime.StartRequest, reporter r1sruntime.Reporter) error {
	r.mu.Lock()
	r.starts = append(r.starts, request)
	r.reporters[request.ExecutionID] = reporter
	startErr := r.startErr
	block := r.blockStart
	r.mu.Unlock()
	select {
	case r.started <- struct{}{}:
	default:
	}
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return startErr
}

func (r *fakeRuntime) Stop(_ context.Context, executionID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stops = append(r.stops, executionID)
	return r.stopErr
}

func (r *fakeRuntime) Recover(_ context.Context, request r1sruntime.StartRequest, reporter r1sruntime.Reporter) error {
	r.mu.Lock()
	r.recoveries = append(r.recoveries, request)
	r.reporters[request.ExecutionID] = reporter
	recoverErr := r.recoverErr
	completion := r.recoverCompletion
	r.mu.Unlock()
	if recoverErr != nil {
		return recoverErr
	}
	if completion != nil {
		copy := *completion
		copy.ExecutionID = request.ExecutionID
		return reporter(copy)
	}
	return nil
}

func (r *fakeRuntime) complete(executionID string, completion r1sruntime.Completion) error {
	r.mu.Lock()
	reporter := r.reporters[executionID]
	r.mu.Unlock()
	if reporter == nil {
		return errors.New("missing reporter")
	}
	completion.ExecutionID = executionID
	return reporter(completion)
}

func (r *fakeRuntime) startCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.starts)
}

func (r *fakeRuntime) stopCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.stops)
}

func (r *fakeRuntime) recoverCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.recoveries)
}

func (r *fakeRuntime) setStopError(err error) {
	r.mu.Lock()
	r.stopErr = err
	r.mu.Unlock()
}

type memoryStateStore struct {
	mu   sync.Mutex
	data []byte
}

func newMemoryStateStore() *memoryStateStore { return &memoryStateStore{} }

func (s *memoryStateStore) Load(context.Context) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.data...), nil
}

func (s *memoryStateStore) Save(_ context.Context, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = append(s.data[:0], data...)
	return nil
}

func newStoredTestAllocator(t *testing.T, clock *fakeClock, runtime *fakeRuntime, store StateStore) *Allocator {
	t.Helper()
	result, err := New(Config{
		Identity: []byte("allocator"), Capacity: map[string]uint32{"default": 1},
		OfferTTL: 30 * time.Second, Now: clock.Now, NewID: sequenceIDs(), Store: store,
	}, runtime)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func int32Pointer(value int32) *int32 { return &value }

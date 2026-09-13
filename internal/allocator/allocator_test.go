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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestLifecycleAndOwnerAuthority(t *testing.T) {
	clock := newFakeClock()
	runtime := newFakeRuntime()
	allocator := newTestAllocator(t, clock, runtime, 1)

	request := requestEnvelope(clock.Now(), "request-message", "owner", "request")
	offerResponse := mustHandle(t, allocator, request)
	offer := offerResponse.GetExecutionOffer()
	if offer == nil || offer.GetRequestId() != "request" {
		t.Fatalf("response = %v, want offer", offerResponse)
	}
	if available := allocator.Available("default"); available != 0 {
		t.Fatalf("available after offer = %d, want 0", available)
	}

	assign := assignEnvelope(clock.Now(), "assign-message", "owner", "request", offer.GetOfferId(), "execution")
	stateResponse := mustHandle(t, allocator, assign)
	if phase := stateResponse.GetExecutionState().GetPhase(); phase != r1sv1.ExecutionPhase_EXECUTION_PHASE_RUNNING {
		t.Fatalf("phase after assignment = %v, want running", phase)
	}
	if runtime.startCount() != 1 {
		t.Fatalf("runtime starts = %d, want 1", runtime.startCount())
	}

	unauthorized := cancelEnvelope(clock.Now(), "bad-cancel", "other-owner", "execution")
	if _, err := allocator.Handle(context.Background(), unauthorized); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("unauthorized cancel error = %v", err)
	}
	if _, err := allocator.Handle(context.Background(), unauthorized); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("replayed unauthorized cancel error = %v", err)
	}
	if runtime.stopCount() != 0 {
		t.Fatalf("runtime stops after unauthorized cancel = %d, want 0", runtime.stopCount())
	}

	cancel := cancelEnvelope(clock.Now(), "cancel-message", "owner", "execution")
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

	mustHandle(t, allocator, requestEnvelope(clock.Now(), "message-1", "owner-1", "request-1"))
	_, err := allocator.Handle(context.Background(), requestEnvelope(clock.Now(), "message-2", "owner-2", "request-2"))
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
	mustHandle(t, allocator, requestEnvelope(clock.Now(), "message-3", "owner-2", "request-2"))
}

func TestExpiredOfferCannotBeAssigned(t *testing.T) {
	clock := newFakeClock()
	allocator := newTestAllocator(t, clock, newFakeRuntime(), 1)
	offer := mustHandle(t, allocator, requestEnvelope(clock.Now(), "request-message", "owner", "request")).GetExecutionOffer()
	clock.Advance(time.Minute)

	assign := assignEnvelope(clock.Now(), "assign-message", "owner", "request", offer.GetOfferId(), "execution")
	if _, err := allocator.Handle(context.Background(), assign); !errors.Is(err, ErrOfferExpired) {
		t.Fatalf("expired assignment error = %v, want ErrOfferExpired", err)
	}
}

func TestDuplicateMessagesAreIdempotent(t *testing.T) {
	clock := newFakeClock()
	runtime := newFakeRuntime()
	allocator := newTestAllocator(t, clock, runtime, 1)

	request := requestEnvelope(clock.Now(), "request-message", "owner", "request")
	firstOffer := mustHandle(t, allocator, request)
	secondOffer := mustHandle(t, allocator, request)
	if !proto.Equal(firstOffer, secondOffer) {
		t.Fatalf("duplicate request response changed:\nfirst: %v\nsecond: %v", firstOffer, secondOffer)
	}

	assign := assignEnvelope(clock.Now(), "assign-message", "owner", "request", firstOffer.GetExecutionOffer().GetOfferId(), "execution")
	firstState := mustHandle(t, allocator, assign)
	secondState := mustHandle(t, allocator, assign)
	if !proto.Equal(firstState, secondState) || runtime.startCount() != 1 {
		t.Fatalf("duplicate assignment was not replayed: starts=%d", runtime.startCount())
	}

	cancel := cancelEnvelope(clock.Now(), "cancel-message", "owner", "execution")
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
	offer := mustHandle(t, allocator, requestEnvelope(clock.Now(), "request-message", "owner", "request")).GetExecutionOffer()
	mustHandle(t, allocator, assignEnvelope(clock.Now(), "assign-1", "owner", "request", offer.GetOfferId(), "execution"))
	response := mustHandle(t, allocator, assignEnvelope(clock.Now(), "assign-2", "owner", "request", offer.GetOfferId(), "execution"))
	if response.GetExecutionState().GetPhase() != r1sv1.ExecutionPhase_EXECUTION_PHASE_RUNNING || runtime.startCount() != 1 {
		t.Fatalf("logical duplicate restarted execution: state=%v starts=%d", response, runtime.startCount())
	}
}

func TestReusedRequestAndMessageIDsRejectConflictingContent(t *testing.T) {
	clock := newFakeClock()
	allocator := newTestAllocator(t, clock, newFakeRuntime(), 2)
	original := requestEnvelope(clock.Now(), "message", "owner", "request")
	mustHandle(t, allocator, original)

	reusedRequest := requestEnvelope(clock.Now(), "different-message", "owner", "request")
	reusedRequest.GetExecutionRequest().Workload.Image = "example.test/different:latest"
	if _, err := allocator.Handle(context.Background(), reusedRequest); !errors.Is(err, ErrExecutionConflict) {
		t.Fatalf("reused request ID error = %v, want ErrExecutionConflict", err)
	}
	reusedMessage := requestEnvelope(clock.Now(), "message", "owner", "different-request")
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
	offer := mustHandle(t, allocator, requestEnvelope(clock.Now(), "request-message", "owner", "request")).GetExecutionOffer()
	assign := assignEnvelope(clock.Now(), "assign-message", "owner", "request", offer.GetOfferId(), "execution")

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
	invalid := requestEnvelope(clock.Now(), "invalid", "owner", "request")
	invalid.GetExecutionRequest().Workload.Image = ""
	if _, err := allocator.Handle(context.Background(), invalid); !errors.Is(err, protocol.ErrInvalidEnvelope) {
		t.Fatalf("invalid request error = %v", err)
	}
	if available := allocator.Available("default"); available != 1 {
		t.Fatalf("invalid envelope consumed capacity: available=%d", available)
	}

	offer := mustHandle(t, allocator, requestEnvelope(clock.Now(), "request-message", "owner", "request")).GetExecutionOffer()
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
	offer := mustHandle(t, allocator, requestEnvelope(clock.Now(), "request-message", "owner", "request")).GetExecutionOffer()

	responses, err := allocator.Handle(context.Background(), assignEnvelope(clock.Now(), "assign-message", "owner", "request", offer.GetOfferId(), "execution"))
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
			offer := mustHandle(t, allocator, requestEnvelope(clock.Now(), "request-message", "owner", "request")).GetExecutionOffer()
			mustHandle(t, allocator, assignEnvelope(clock.Now(), "assign-message", "owner", "request", offer.GetOfferId(), "execution"))
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
	offer := mustHandle(t, allocator, requestEnvelope(clock.Now(), "request-message", "owner", "request")).GetExecutionOffer()
	mustHandle(t, allocator, assignEnvelope(clock.Now(), "assign-message", "owner", "request", offer.GetOfferId(), "execution"))

	if _, err := allocator.Handle(context.Background(), cancelEnvelope(clock.Now(), "cancel-1", "owner", "execution")); !errors.Is(err, ErrRuntimeStop) {
		t.Fatalf("cancel error = %v, want ErrRuntimeStop", err)
	}
	snapshot, _ := allocator.Execution("execution")
	if snapshot.State.GetPhase() != r1sv1.ExecutionPhase_EXECUTION_PHASE_RUNNING || allocator.Available("default") != 0 {
		t.Fatalf("failed stop changed state or capacity: %+v", snapshot)
	}
	runtime.setStopError(nil)
	mustHandle(t, allocator, cancelEnvelope(clock.Now(), "cancel-2", "owner", "execution"))
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
		envelope := requestEnvelope(clock.Now(), fmt.Sprintf("message-%d", index), "owner", fmt.Sprintf("request-%d", index))
		envelope.GetExecutionRequest().ResourceClass = "unknown"
		if _, err := allocator.Handle(context.Background(), envelope); !errors.Is(err, ErrCapacityExhausted) {
			t.Fatalf("request %d error = %v", index, err)
		}
	}
	allocator.mu.Lock()
	entries := len(allocator.replay)
	allocator.mu.Unlock()
	if entries > 2 {
		t.Fatalf("replay entries = %d, want at most 2", entries)
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

func requestEnvelope(now time.Time, messageID, owner, requestID string) *r1sv1.Envelope {
	return &r1sv1.Envelope{
		MessageId: messageID,
		Sender:    []byte(owner),
		SentAt:    timestamppb.New(now),
		Payload: &r1sv1.Envelope_ExecutionRequest{ExecutionRequest: &r1sv1.ExecutionRequest{
			RequestId:     requestID,
			ResourceClass: "default",
			Workload:      &r1sv1.Workload{Image: "example.test/image:latest"},
			Policy: &r1sv1.ExecutionPolicy{
				Deadline:   timestamppb.New(now.Add(time.Hour)),
				MaxRuntime: durationpb.New(time.Minute),
			},
		}},
	}
}

func assignEnvelope(now time.Time, messageID, owner, requestID, offerID, executionID string) *r1sv1.Envelope {
	return &r1sv1.Envelope{
		MessageId: messageID,
		Sender:    []byte(owner),
		SentAt:    timestamppb.New(now),
		Payload: &r1sv1.Envelope_ExecutionAssign{ExecutionAssign: &r1sv1.ExecutionAssign{
			RequestId: requestID, OfferId: offerID, ExecutionId: executionID,
		}},
	}
}

func cancelEnvelope(now time.Time, messageID, owner, executionID string) *r1sv1.Envelope {
	return &r1sv1.Envelope{
		MessageId: messageID,
		Sender:    []byte(owner),
		SentAt:    timestamppb.New(now),
		Payload: &r1sv1.Envelope_ExecutionCancel{ExecutionCancel: &r1sv1.ExecutionCancel{
			ExecutionId: executionID, Reason: "user requested",
		}},
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
	mu         sync.Mutex
	starts     []r1sruntime.StartRequest
	stops      []string
	reporters  map[string]r1sruntime.Reporter
	startErr   error
	stopErr    error
	blockStart chan struct{}
	started    chan struct{}
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

func (r *fakeRuntime) setStopError(err error) {
	r.mu.Lock()
	r.stopErr = err
	r.mu.Unlock()
}

package allocator

import (
	"context"
	"errors"
	"testing"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
	statebolt "github.com/mytecor/r1s/internal/store/bolt"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func leaseRenewEnvelope(now time.Time, messageID, client, executionID string, lease time.Duration) *r1sv1.Envelope {
	return &r1sv1.Envelope{
		MessageId: messageID,
		Sender:    []byte(client),
		SentAt:    timestamppb.New(now),
		Payload: &r1sv1.Envelope_ExecutionLeaseRenew{ExecutionLeaseRenew: &r1sv1.ExecutionLeaseRenew{
			ExecutionId: executionID, LeaseDuration: durationpb.New(lease),
		}},
	}
}

// assignedExecution drives one request -> offer -> assign round trip and
// returns the resulting state envelope.
func assignedExecution(t *testing.T, allocator *Allocator, clock *fakeClock, messageID, client, requestID, executionID string) *r1sv1.ExecutionState {
	t.Helper()
	offer := mustHandle(t, allocator, requestEnvelope(clock.Now(), "request-"+messageID, client, requestID)).GetExecutionOffer()
	state := mustHandle(t, allocator, assignEnvelope(clock.Now(), "assign-"+messageID, client, requestID, offer.GetOfferId(), executionID))
	return state.GetExecutionState()
}

func TestLeaseRenewExtendsExpiryAndRejectsForeignCallers(t *testing.T) {
	clock := newFakeClock()
	runtime := newFakeRuntime()
	allocator := newTestAllocator(t, clock, runtime, 1)
	assignedExecution(t, allocator, clock, "one", "client", "request", "execution")

	renew := leaseRenewEnvelope(clock.Now(), "renew-message", "client", "execution", 5*time.Minute)
	ack := mustHandle(t, allocator, renew).GetExecutionLeaseRenewAck()
	if ack == nil || ack.GetExecutionId() != "execution" || !ack.GetExpiresAt().AsTime().Equal(clock.Now().Add(5*time.Minute)) {
		t.Fatalf("renew ack = %v, want expiry %s", ack, clock.Now().Add(5*time.Minute))
	}

	foreign := leaseRenewEnvelope(clock.Now(), "foreign-renew", "other-client", "execution", 5*time.Minute)
	if _, err := allocator.Handle(context.Background(), foreign); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("foreign renewal error = %v, want ErrUnauthorized", err)
	}
	if _, err := allocator.Handle(context.Background(), foreign); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("replayed foreign renewal error = %v, want ErrUnauthorized", err)
	}
	if runtime.stopCount() != 0 {
		t.Fatalf("runtime stops after foreign renewal = %d, want 0", runtime.stopCount())
	}

	if _, err := allocator.Handle(context.Background(), leaseRenewEnvelope(clock.Now(), "renew-huge", "client", "execution", 8*24*time.Hour)); !errors.Is(err, ErrLeaseTooLong) {
		t.Fatalf("oversized lease error = %v, want ErrLeaseTooLong", err)
	}
}

func TestLeaseRenewalIsReplaySafe(t *testing.T) {
	clock := newFakeClock()
	allocator := newTestAllocator(t, clock, newFakeRuntime(), 1)
	assignedExecution(t, allocator, clock, "one", "client", "request", "execution")

	renew := leaseRenewEnvelope(clock.Now(), "renew-message", "client", "execution", 5*time.Minute)
	first := mustHandle(t, allocator, renew)
	second := mustHandle(t, allocator, renew)
	if first.GetExecutionLeaseRenewAck().GetExpiresAt().AsTime().IsZero() {
		t.Fatalf("renew response = %v, want ack", first)
	}
	if !proto.Equal(first, second) {
		t.Fatal("replayed renewal changed the ack")
	}
	// A distinct message ID extends again from the current time.
	clock.Advance(time.Minute)
	third := mustHandle(t, allocator, leaseRenewEnvelope(clock.Now(), "renew-again", "client", "execution", 5*time.Minute))
	if !third.GetExecutionLeaseRenewAck().GetExpiresAt().AsTime().Equal(clock.Now().Add(5 * time.Minute)) {
		t.Fatalf("second renewal expiry = %v", third)
	}
}

func TestLeaseRenewalRejectsTerminalExecutions(t *testing.T) {
	clock := newFakeClock()
	allocator := newTestAllocator(t, clock, newFakeRuntime(), 1)
	assignedExecution(t, allocator, clock, "one", "client", "request", "execution")
	mustHandle(t, allocator, cancelEnvelope(clock.Now(), "cancel-message", "client", "execution"))

	if _, err := allocator.Handle(context.Background(), leaseRenewEnvelope(clock.Now(), "renew-message", "client", "execution", time.Minute)); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("terminal renewal error = %v, want ErrInvalidTransition", err)
	}
}

func TestExpiredLeaseIsEvictedLocally(t *testing.T) {
	clock := newFakeClock()
	runtime := newFakeRuntime()
	allocator := newTestAllocator(t, clock, runtime, 1)
	assignedExecution(t, allocator, clock, "one", "client", "request", "execution")

	clock.Advance(11 * time.Minute)
	if err := allocator.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if runtime.stopCount() != 1 {
		t.Fatalf("runtime stops = %d, want 1", runtime.stopCount())
	}
	if available := allocator.Available("default"); available != 1 {
		t.Fatalf("available after eviction = %d, want 1", available)
	}
	snapshot, ok := allocator.Execution("execution")
	if !ok || snapshot.State.GetPhase() != r1sv1.ExecutionPhase_EXECUTION_PHASE_FAILED || snapshot.State.GetDetail() != protocol.LeaseExpiredDetail {
		t.Fatalf("evicted snapshot = %+v, want failed with lease-expiry detail", snapshot)
	}
	// Terminal metadata stays retrievable inside the retention horizon.
	if _, err := allocator.Handle(context.Background(), inspectEnvelope(clock.Now(), "inspect-message", "client", "execution")); err != nil {
		t.Fatalf("inspect after eviction = %v", err)
	}
	// A repeat sweep must not re-evict or double-report.
	if err := allocator.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if runtime.stopCount() != 1 {
		t.Fatalf("runtime stops after second sweep = %d, want 1", runtime.stopCount())
	}
}

func TestRenewalBeforeEvictionKeepsExecutionAlive(t *testing.T) {
	clock := newFakeClock()
	runtime := newFakeRuntime()
	allocator := newTestAllocator(t, clock, runtime, 1)
	assignedExecution(t, allocator, clock, "one", "client", "request", "execution")

	clock.Advance(8 * time.Minute)
	mustHandle(t, allocator, leaseRenewEnvelope(clock.Now(), "renew-1", "client", "execution", 10*time.Minute))
	// Nine more minutes: still inside the renewed 10-minute lease.
	clock.Advance(9 * time.Minute)
	if err := allocator.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if runtime.stopCount() != 0 {
		t.Fatalf("runtime stops = %d, want 0 for a renewed execution", runtime.stopCount())
	}
	snapshot, ok := allocator.Execution("execution")
	if !ok || snapshot.State.GetPhase() != r1sv1.ExecutionPhase_EXECUTION_PHASE_RUNNING {
		t.Fatalf("renewed snapshot = %+v, want still running", snapshot)
	}
}

func TestRestartEvictsExpiredLeaseAndKeepsValidLease(t *testing.T) {
	clock := newFakeClock()
	path := t.TempDir() + "/allocator.db"
	store, err := statebolt.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	first, err := New(Config{
		Identity: []byte("allocator"), Capacity: map[string]uint32{"default": 2},
		OfferTTL: 30 * time.Second, Now: clock.Now, NewID: sequenceIDs(), Store: store,
	}, newFakeRuntime())
	if err != nil {
		t.Fatal(err)
	}
	offer := mustHandle(t, first, requestEnvelope(clock.Now(), "request-a", "client", "request-a")).GetExecutionOffer()
	mustHandle(t, first, assignEnvelope(clock.Now(), "assign-a", "client", "request-a", offer.GetOfferId(), "execution-a"))
	secondOffer := mustHandle(t, first, requestEnvelope(clock.Now(), "request-b", "other-client", "request-b")).GetExecutionOffer()
	mustHandle(t, first, assignEnvelope(clock.Now(), "assign-b", "other-client", "request-b", secondOffer.GetOfferId(), "execution-b"))
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// Both leases are still valid at restart time.
	clock.Advance(5 * time.Minute)
	secondStore, err := statebolt.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	secondRuntime := newFakeRuntime()
	second, err := New(Config{
		Identity: []byte("allocator"), Capacity: map[string]uint32{"default": 2},
		OfferTTL: 30 * time.Second, Now: clock.Now, NewID: sequenceIDs(), Store: secondStore,
	}, secondRuntime)
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if secondRuntime.recoverCount() != 2 || secondRuntime.startCount() != 0 {
		t.Fatalf("recovery calls=%d starts=%d, want 2 and 0", secondRuntime.recoverCount(), secondRuntime.startCount())
	}
	for _, executionID := range []string{"execution-a", "execution-b"} {
		if snapshot, ok := second.Execution(executionID); !ok || snapshot.State.GetPhase() != r1sv1.ExecutionPhase_EXECUTION_PHASE_RUNNING {
			t.Fatalf("valid-lease execution %s = %+v, want still running", executionID, snapshot)
		}
	}

	// Advance past both leases and restart again: both must be evicted
	// without reviving runtime tasks.
	if err := secondStore.Close(); err != nil {
		t.Fatal(err)
	}
	clock.Advance(6 * time.Minute)
	thirdStore, err := statebolt.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer thirdStore.Close()
	third, err := New(Config{
		Identity: []byte("allocator"), Capacity: map[string]uint32{"default": 2},
		OfferTTL: 30 * time.Second, Now: clock.Now, NewID: sequenceIDs(), Store: thirdStore,
	}, newFakeRuntime())
	if err != nil {
		t.Fatal(err)
	}
	if err := third.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, executionID := range []string{"execution-a", "execution-b"} {
		snapshot, ok := third.Execution(executionID)
		if !ok || snapshot.State.GetPhase() != r1sv1.ExecutionPhase_EXECUTION_PHASE_FAILED || snapshot.State.GetDetail() != protocol.LeaseExpiredDetail {
			t.Fatalf("expired lease after restart = %+v, want evicted", snapshot)
		}
	}
}

func TestCancellationStaysDistinguishableFromEviction(t *testing.T) {
	clock := newFakeClock()
	allocator := newTestAllocator(t, clock, newFakeRuntime(), 1)
	assignedExecution(t, allocator, clock, "one", "client", "request", "execution")

	cancelled := mustHandle(t, allocator, cancelEnvelope(clock.Now(), "cancel-message", "client", "execution")).GetExecutionState()
	if cancelled.GetPhase() != r1sv1.ExecutionPhase_EXECUTION_PHASE_CANCELLED || cancelled.GetDetail() != "user requested" {
		t.Fatalf("cancelled state = %+v", cancelled)
	}
	if cancelled.GetDetail() == protocol.LeaseExpiredDetail {
		t.Fatal("cancellation detail collides with the lease-expiry marker")
	}
}

func TestLostLeaseRenewalAfterEvictionIsRejected(t *testing.T) {
	clock := newFakeClock()
	allocator := newTestAllocator(t, clock, newFakeRuntime(), 1)
	assignedExecution(t, allocator, clock, "one", "client", "request", "execution")
	clock.Advance(11 * time.Minute)
	if err := allocator.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Renewal of an evicted execution is rejected; the client re-requests.
	renew := leaseRenewEnvelope(clock.Now(), "renew-message", "client", "execution", 5*time.Minute)
	if _, err := allocator.Handle(context.Background(), renew); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("evicted renewal error = %v, want ErrInvalidTransition", err)
	}
}

func TestFailedEvictionStopRestoresPhaseAndDetail(t *testing.T) {
	clock := newFakeClock()
	runtime := newFakeRuntime()
	allocator := newTestAllocator(t, clock, runtime, 1)
	assignedExecution(t, allocator, clock, "one", "client", "request", "execution")

	clock.Advance(11 * time.Minute)
	runtime.setStopError(errors.New("temporary stop failure"))
	if err := allocator.EvictExpiredLeases(context.Background()); !errors.Is(err, ErrRuntimeStop) {
		t.Fatalf("eviction with failing stop = %v, want ErrRuntimeStop", err)
	}
	// A failed stop restores the pre-eviction phase and detail so the next
	// sweep can retry the eviction from a clean state.
	snapshot, ok := allocator.Execution("execution")
	if !ok || snapshot.State.GetPhase() != r1sv1.ExecutionPhase_EXECUTION_PHASE_RUNNING || snapshot.State.GetDetail() != "" {
		t.Fatalf("restored snapshot = %+v, want running without detail", snapshot)
	}
	runtime.setStopError(nil)
	if err := allocator.EvictExpiredLeases(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot, ok = allocator.Execution("execution")
	if !ok || snapshot.State.GetPhase() != r1sv1.ExecutionPhase_EXECUTION_PHASE_FAILED || snapshot.State.GetDetail() != protocol.LeaseExpiredDetail {
		t.Fatalf("retried eviction snapshot = %+v, want failed with lease-expiry detail", snapshot)
	}
}

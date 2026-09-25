package client

import (
	"context"
	"errors"
	"testing"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// leasedExecution creates one request, offers, and selection so an execution
// exists with a recorded lease-holding intent.
func leasedExecution(t *testing.T, leaseDuration time.Duration) (*Client, string) {
	t.Helper()
	now := time.Unix(1_800_000_000, 0).UTC()
	core, err := New(Config{Identity: []byte("client"), Now: func() time.Time { return now }, NewID: sequenceIDs("request", "request-message", "execution", "assign-message", "renew-1", "renew-2")})
	if err != nil {
		t.Fatal(err)
	}
	registerAllocator(t, core, "allocator", "near", 1)
	requestID, request, err := core.CreateRequest(testWorkload(), testPolicy(), "default")
	if err != nil {
		t.Fatal(err)
	}
	mustObserveOffer(t, core, now, request, "allocator", "offer")
	_, assignment, err := core.Select(requestID)
	if err != nil {
		t.Fatal(err)
	}
	executionID := assignment.GetExecutionAssign().GetExecutionId()
	destination, _, lost, err := core.Maintain(executionID, leaseDuration)
	if err != nil || lost || destination != "near" {
		t.Fatalf("Maintain() = (%q, lost=%v, %v)", destination, lost, err)
	}
	return core, executionID
}

func TestMaintainRecordsDurableIntentAndRenews(t *testing.T) {
	core, executionID := leasedExecution(t, 5*time.Minute)

	// The recorded intent paces renewals: not due immediately.
	if due := core.DueLeaseRenewals(); len(due) != 0 {
		t.Fatalf("DueLeaseRenewals() = %v, want none", due)
	}
	if _, _, lost, err := core.Maintain(executionID, 0); err != nil || lost {
		t.Fatalf("second Maintain() = lost=%v err=%v, want a fresh renewal", lost, err)
	}

	// A renewal ack clears the pending command and updates the expiry.
	ack := &r1sv1.Envelope{
		MessageId: "ack-message", Sender: []byte("allocator"), CorrelationId: "renew-2", SentAt: timestamppb.Now(),
		Payload: &r1sv1.Envelope_ExecutionLeaseRenewAck{ExecutionLeaseRenewAck: &r1sv1.ExecutionLeaseRenewAck{
			ExecutionId: executionID, ExpiresAt: timestamppb.New(time.Now().Add(5 * time.Minute)),
		}},
	}
	if err := core.Handle(context.Background(), ack); err != nil {
		t.Fatal(err)
	}
	if core.LeaseIntentLost(executionID) {
		t.Fatal("acked renewal marked the intent lost")
	}
	if lost := core.LostLeaseIntents(); len(lost) != 0 {
		t.Fatalf("LostLeaseIntents() = %v, want none", lost)
	}
}

func TestLostLeaseIsDetectedFromTerminalState(t *testing.T) {
	core, executionID := leasedExecution(t, 5*time.Minute)
	now := time.Unix(1_800_000_000, 0).UTC()

	// The allocator evicted the execution for lease expiry.
	lostState := &r1sv1.Envelope{
		MessageId: "state-message", Sender: []byte("allocator"), SentAt: timestamppb.New(now.Add(time.Minute)),
		Payload: &r1sv1.Envelope_ExecutionState{ExecutionState: &r1sv1.ExecutionState{
			ExecutionId: executionID, Phase: r1sv1.ExecutionPhase_EXECUTION_PHASE_FAILED,
			OccurredAt: timestamppb.New(now.Add(time.Minute)), Detail: protocol.LeaseExpiredDetail, Revision: 3,
		}},
	}
	if err := core.Handle(context.Background(), lostState); err != nil {
		t.Fatal(err)
	}
	if !core.LeaseIntentLost(executionID) {
		t.Fatal("evicted execution was not marked as a lost lease")
	}
	if lost := core.LostLeaseIntents(); len(lost) != 1 || lost[0] != executionID {
		t.Fatalf("LostLeaseIntents() = %v, want [%s]", lost, executionID)
	}
	// The recorded request survives for a re-request.
	request, ok := core.RequestForExecution(executionID)
	if !ok || request.GetWorkload().GetImage() != testWorkload().GetImage() {
		t.Fatalf("RequestForExecution() = %v", request)
	}
}

func TestRenewalNotFoundMarksIntentLost(t *testing.T) {
	core, executionID := leasedExecution(t, 5*time.Minute)
	_, envelope, _, err := core.Maintain(executionID, 0)
	if err != nil {
		t.Fatal(err)
	}
	failure := &r1sv1.Envelope{
		MessageId: "error-message", Sender: []byte("allocator"), CorrelationId: envelope.GetMessageId(), SentAt: timestamppb.Now(),
		Payload: &r1sv1.Envelope_CommandError{CommandError: &r1sv1.CommandError{Code: "NOT_FOUND", Detail: "resource unavailable to this identity"}},
	}
	if err := core.Handle(context.Background(), failure); err != nil {
		t.Fatal(err)
	}
	if !core.LeaseIntentLost(executionID) {
		t.Fatal("authoritative renewal loss did not mark the intent lost")
	}

	// Transient failures keep the intent for retry.
	core2, executionID2 := leasedExecution(t, 5*time.Minute)
	_, envelope2, _, err := core2.Maintain(executionID2, 0)
	if err != nil {
		t.Fatal(err)
	}
	transient := &r1sv1.Envelope{
		MessageId: "error-message-2", Sender: []byte("allocator"), CorrelationId: envelope2.GetMessageId(), SentAt: timestamppb.Now(),
		Payload: &r1sv1.Envelope_CommandError{CommandError: &r1sv1.CommandError{Code: "CAPACITY", Detail: "allocator capacity exhausted", Retryable: true}},
	}
	if err := core2.Handle(context.Background(), transient); err != nil {
		t.Fatal(err)
	}
	if core2.LeaseIntentLost(executionID2) {
		t.Fatal("transient renewal failure marked the intent lost")
	}
}

func TestAuthenticatedInspectNotFoundMarksIntentLost(t *testing.T) {
	core, executionID := leasedExecution(t, 5*time.Minute)
	_, envelope, err := core.Inspect(executionID)
	if err != nil {
		t.Fatal(err)
	}
	failure := &r1sv1.Envelope{
		MessageId: "inspect-error", Sender: []byte("allocator"), CorrelationId: envelope.GetMessageId(), SentAt: timestamppb.Now(),
		Payload: &r1sv1.Envelope_CommandError{CommandError: &r1sv1.CommandError{Code: "NOT_FOUND", Detail: "resource unavailable to this identity"}},
	}
	if err := core.Handle(context.Background(), failure); err != nil {
		t.Fatal(err)
	}
	if !core.LeaseIntentLost(executionID) {
		t.Fatal("authoritative inspection loss did not mark the intent lost")
	}
}

func TestTerminalExecutionRefusesRenewal(t *testing.T) {
	core, executionID := leasedExecution(t, 5*time.Minute)
	now := time.Unix(1_800_000_000, 0).UTC()
	completed := &r1sv1.Envelope{
		MessageId: "state-message", Sender: []byte("allocator"), SentAt: timestamppb.New(now.Add(time.Minute)),
		Payload: &r1sv1.Envelope_ExecutionState{ExecutionState: &r1sv1.ExecutionState{
			ExecutionId: executionID, Phase: r1sv1.ExecutionPhase_EXECUTION_PHASE_COMPLETED,
			OccurredAt: timestamppb.New(now.Add(time.Minute)), Revision: 2,
		}},
	}
	if err := core.Handle(context.Background(), completed); err != nil {
		t.Fatal(err)
	}
	if _, _, lost, err := core.Maintain(executionID, 0); err == nil || lost {
		t.Fatalf("terminal Maintain() = lost=%v err=%v, want ErrConflict", lost, err)
	}
}

func TestRebindLeaseIntentMovesDutyToReplacement(t *testing.T) {
	core, executionID := leasedExecution(t, 5*time.Minute)
	now := time.Unix(1_800_000_000, 0).UTC()

	// Pin allocator destinations with the intent; a re-request must preserve
	// the original placement constraint.
	if err := core.RecordLeaseIntent(executionID, 5*time.Minute, []string{"pin-a", "pin-b", "pin-a"}); err != nil {
		t.Fatal(err)
	}
	pinned := core.LeaseIntentAllocators(executionID)
	if len(pinned) != 2 || pinned[0] != "pin-a" || pinned[1] != "pin-b" {
		t.Fatalf("LeaseIntentAllocators() = %v, want deduplicated [pin-a pin-b]", pinned)
	}

	// Re-request the recorded workload as a fresh request and assignment.
	replacementRequestID, replacementRequest, err := core.CreateRequest(testWorkload(), testPolicy(), "default")
	if err != nil {
		t.Fatal(err)
	}
	mustObserveOffer(t, core, now, replacementRequest, "allocator", "offer-2")
	_, assignment, err := core.Select(replacementRequestID)
	replacementID := assignment.GetExecutionAssign().GetExecutionId()

	// Mark the old lease lost, then move the duty.
	lostState := &r1sv1.Envelope{
		MessageId: "state-message", Sender: []byte("allocator"), SentAt: timestamppb.New(now.Add(time.Minute)),
		Payload: &r1sv1.Envelope_ExecutionState{ExecutionState: &r1sv1.ExecutionState{
			ExecutionId: executionID, Phase: r1sv1.ExecutionPhase_EXECUTION_PHASE_FAILED,
			OccurredAt: timestamppb.New(now.Add(time.Minute)), Detail: protocol.LeaseExpiredDetail, Revision: 3,
		}},
	}
	if err := core.Handle(context.Background(), lostState); err != nil {
		t.Fatal(err)
	}
	if err := core.RebindLeaseIntent(executionID, replacementID); err != nil {
		t.Fatal(err)
	}
	if core.LeaseIntentLost(executionID) {
		t.Fatal("rebound intent stayed on the lost execution")
	}
	due := core.DueLeaseRenewals()
	if len(due) != 1 || due[0] != replacementID {
		t.Fatalf("DueLeaseRenewals() = %v, want [%s]", due, replacementID)
	}
	destination, envelope, lost, err := core.Maintain(replacementID, 0)
	if err != nil || lost || destination != "near" || envelope.GetExecutionLeaseRenew().GetLeaseDuration().AsDuration() != 5*time.Minute {
		t.Fatalf("rebound Maintain() = (%q, lost=%v, %v, %v)", destination, lost, envelope.GetExecutionLeaseRenew().GetLeaseDuration(), err)
	}
	// The pinning moves with the intent and leaves the lost execution.
	if moved := core.LeaseIntentAllocators(replacementID); len(moved) != 2 || moved[0] != "pin-a" || moved[1] != "pin-b" {
		t.Fatalf("rebound LeaseIntentAllocators() = %v, want [pin-a pin-b]", moved)
	}
	if left := core.LeaseIntentAllocators(executionID); left != nil {
		t.Fatalf("lost LeaseIntentAllocators() = %v, want none", left)
	}
}

// TestLeaseAckWithoutPendingRenewalIsRejected ensures an ack without a
// correlation ID can never apply against an empty pending renewal.
func TestLeaseAckWithoutPendingRenewalIsRejected(t *testing.T) {
	core, executionID := leasedExecution(t, 5*time.Minute)
	strayAck := &r1sv1.Envelope{
		MessageId: "stray-ack", Sender: []byte("allocator"), CorrelationId: "", SentAt: timestamppb.Now(),
		Payload: &r1sv1.Envelope_ExecutionLeaseRenewAck{ExecutionLeaseRenewAck: &r1sv1.ExecutionLeaseRenewAck{
			ExecutionId: executionID, ExpiresAt: timestamppb.New(time.Now().Add(time.Hour)),
		}},
	}
	if err := core.Handle(context.Background(), strayAck); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("stray ack without correlation ID = %v, want ErrUnauthorized", err)
	}
}

func TestMaintainUnknownExecutionFails(t *testing.T) {
	core, err := New(Config{Identity: []byte("client")})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := core.Maintain("missing", 0); !errors.Is(err, ErrExecutionNotFound) {
		t.Fatalf("Maintain() error = %v, want ErrExecutionNotFound", err)
	}
}

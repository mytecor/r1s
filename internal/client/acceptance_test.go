package client

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// F8-02: a later monotonic revision is accepted even when occurred_at goes
// backwards (clock rollback); older and reordered states never regress the
// client's durable snapshot; revisions survive restart.
func TestClockRollbackPreservesRevisionOrdering(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	core, executionID := newClientWithExecution(t, now)

	running := revisionStateEnvelope(now.Add(time.Minute), "allocator", executionID, r1sv1.ExecutionPhase_EXECUTION_PHASE_RUNNING, 2)
	if err := core.Handle(context.Background(), running); err != nil {
		t.Fatal(err)
	}
	// FAILED revision 3 but with occurred_at earlier than the running state:
	// the allocator's clock rolled back before finishing. Revision wins.
	failed := revisionStateEnvelope(now, "allocator", executionID, r1sv1.ExecutionPhase_EXECUTION_PHASE_FAILED, 3)
	failed.GetExecutionState().ExitCode = int32Pointer(1)
	if err := core.Handle(context.Background(), failed); err != nil {
		t.Fatal(err)
	}
	// A reordered older state (revision 1) must not regress the retained result.
	older := revisionStateEnvelope(now.Add(-time.Minute), "allocator", executionID, r1sv1.ExecutionPhase_EXECUTION_PHASE_STARTING, 1)
	if err := core.Handle(context.Background(), older); err != nil {
		t.Fatal(err)
	}
	snapshot, _ := core.Execution(executionID)
	if snapshot.State.GetRevision() != 3 || snapshot.State.GetPhase() != r1sv1.ExecutionPhase_EXECUTION_PHASE_FAILED {
		t.Fatalf("state = %v, want FAILED revision 3", snapshot.State)
	}
}

// F8-02: contradictory equal revisions are rejected, and an identical equal
// revision is accepted as a duplicate.
func TestContradictoryEqualRevisionRejected(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	core, executionID := newClientWithExecution(t, now)

	first := revisionStateEnvelope(now, "allocator", executionID, r1sv1.ExecutionPhase_EXECUTION_PHASE_RUNNING, 5)
	if err := core.Handle(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	duplicate := revisionStateEnvelope(now, "allocator", executionID, r1sv1.ExecutionPhase_EXECUTION_PHASE_RUNNING, 5)
	if err := core.Handle(context.Background(), duplicate); err != nil {
		t.Fatalf("identical equal revision = %v", err)
	}
	conflicting := revisionStateEnvelope(now, "allocator", executionID, r1sv1.ExecutionPhase_EXECUTION_PHASE_FAILED, 5)
	conflicting.GetExecutionState().ExitCode = int32Pointer(1)
	if err := core.Handle(context.Background(), conflicting); !errors.Is(err, ErrConflict) {
		t.Fatalf("contradictory equal revision = %v, want ErrConflict", err)
	}
}

// F9-02: Logs creates one explicit request for an execution the client knows;
// only the authenticated owner can accept the response; a foreign allocator is
// denied without mutating state.
func TestExplicitLogsRequestAndOwnerAuthorization(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	core, executionID := newClientWithExecution(t, now)

	destination, request, err := core.Logs(executionID, "stderr", 7, 16)
	if err != nil {
		t.Fatal(err)
	}
	if destination != "allocator" {
		t.Fatalf("destination = %q", destination)
	}
	if q := request.GetExecutionLogsRequest(); q.GetExecutionId() != executionID || q.GetStream() != "stderr" || q.GetOffset() != 7 || q.GetMaxBytes() != 16 {
		t.Fatalf("logs request = %v", q)
	}
	// The owner receives a valid, matching response.
	response := logsResponseFor(t, request, "allocator", []byte("tail bytes"))
	if err := core.Handle(context.Background(), response); err != nil {
		t.Fatal(err)
	}
	// A different allocator cannot impersonate the owner's response.
	forged := logsResponseFor(t, request, "other-allocator", []byte("forged"))
	if err := core.Handle(context.Background(), forged); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("forged logs response = %v, want ErrUnauthorized", err)
	}
	// An unrelated request for an unknown execution is refused before sending.
	if _, _, err := core.Logs("unknown-execution", "stdout", 0, 8); !errors.Is(err, ErrExecutionNotFound) {
		t.Fatalf("unknown execution logs = %v", err)
	}
}

func newClientWithExecution(t *testing.T, now time.Time) (*Client, string) {
	t.Helper()
	core, err := New(Config{Identity: []byte("client"), Now: func() time.Time { return now }, NewID: sequenceIDs("request", "request-message", "execution", "assign-message")})
	if err != nil {
		t.Fatal(err)
	}
	registerAllocator(t, core, "allocator", "allocator", 1)
	requestID, request, err := core.CreateRequest(testWorkload(), testPolicy(), "default")
	if err != nil {
		t.Fatal(err)
	}
	mustObserveOffer(t, core, now, request, "allocator", "offer")
	_, assignment, err := core.Select(requestID)
	if err != nil {
		t.Fatal(err)
	}
	return core, assignment.GetExecutionAssign().GetExecutionId()
}

// revisionStateEnvelope builds a state envelope with an explicit revision.
func revisionStateEnvelope(now time.Time, allocatorID, executionID string, phase r1sv1.ExecutionPhase, revision uint64) *r1sv1.Envelope {
	state := stateEnvelope(now, allocatorID, executionID, phase, 0)
	state.GetExecutionState().Revision = revision
	return state
}

// logsResponseFor builds a valid ExecutionLogsResponse correlated to request.
func logsResponseFor(t *testing.T, request *r1sv1.Envelope, allocatorID string, data []byte) *r1sv1.Envelope {
	t.Helper()
	if len(data) > int(request.GetExecutionLogsRequest().GetMaxBytes()) {
		t.Fatal("test data exceeds requested limit")
	}
	sum := sha256.Sum256(data)
	q := request.GetExecutionLogsRequest()
	return &r1sv1.Envelope{
		MessageId: "logs-response", Sender: []byte(allocatorID), CorrelationId: request.GetMessageId(), SentAt: timestamppb.New(time.Now().UTC()),
		Payload: &r1sv1.Envelope_ExecutionLogsResponse{ExecutionLogsResponse: &r1sv1.ExecutionLogsResponse{
			ExecutionId: q.GetExecutionId(), Stream: q.GetStream(), Offset: q.GetOffset(), Data: data, NextOffset: q.GetOffset() + uint64(len(data)), Eof: true, Sha256: sum[:],
		}},
	}
}

func int32Pointer(value int32) *int32 { return &value }

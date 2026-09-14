package allocator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
	r1sruntime "github.com/mytecor/r1s/internal/runtime"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// F8-01: capacity rejection is a correlated CommandError, distinct from a
// dropped response, and stays stable across replay and restart.
func TestCapacityRejectionReturnsCorrelatedCommandError(t *testing.T) {
	clock := newFakeClock()
	runtime := newFakeRuntime()
	core := newTestAllocator(t, clock, runtime, 1)
	mustHandle(t, core, requestEnvelope(clock.Now(), "request-1", "client", "request-1"))

	rejected := requestEnvelope(clock.Now(), "request-2", "client", "request-2")
	responses, err := core.Handle(context.Background(), rejected)
	if !errors.Is(err, ErrCapacityExhausted) {
		t.Fatalf("Handle error = %v, want ErrCapacityExhausted", err)
	}
	if len(responses) != 1 {
		t.Fatalf("responses = %d, want 1", len(responses))
	}
	failure := responses[0]
	if failure.GetCorrelationId() != rejected.GetMessageId() {
		t.Fatalf("correlation = %q, want %q", failure.GetCorrelationId(), rejected.GetMessageId())
	}
	commandError := failure.GetCommandError()
	if commandError == nil || commandError.GetCode() != "CAPACITY" || !commandError.GetRetryable() {
		t.Fatalf("command error = %v, want CAPACITY retryable", commandError)
	}
	// Workload output must never appear in a rejection.
	if strings.Contains(commandError.GetDetail(), "image") {
		t.Fatalf("rejection leaked workload detail: %q", commandError.GetDetail())
	}
	// Replaying the same command returns the same rejection without mutating state.
	replayResponses, replayErr := core.Handle(context.Background(), rejected)
	if !errors.Is(replayErr, ErrCapacityExhausted) || len(replayResponses) != 1 || !proto.Equal(replayResponses[0], failure) {
		t.Fatalf("rejected replay changed: responses=%v err=%v", replayResponses, replayErr)
	}
	if available := core.Available("default"); available != 0 {
		t.Fatalf("rejected command changed capacity: available=%d", available)
	}
	if runtime.startCount() != 0 {
		t.Fatal("rejected request started work")
	}
}

// F8-01: unknown and unauthorized commands cannot disclose another client's
// execution data; the reply is an opaque NOT_FOUND, not a terminal state.
func TestUnauthorizedAndUnknownCommandsAreOpaque(t *testing.T) {
	clock := newFakeClock()
	core := newTestAllocator(t, clock, newFakeRuntime(), 1)
	offer := mustHandle(t, core, requestEnvelope(clock.Now(), "request-message", "client", "request")).GetExecutionOffer()
	mustHandle(t, core, assignEnvelope(clock.Now(), "assign-message", "client", "request", offer.GetOfferId(), "execution"))

	// A different identity must not learn the execution exists via inspect.
	spy := inspectEnvelope(clock.Now(), "spy-inspect", "other", "execution")
	responses, err := core.Handle(context.Background(), spy)
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("spy inspect error = %v", err)
	}
	if len(responses) != 1 || responses[0].GetCommandError().GetCode() != "NOT_FOUND" {
		t.Fatalf("spy inspect responses=%v, want one NOT_FOUND", responses)
	}
	if responses[0].GetExecutionState() != nil {
		t.Fatal("spy saw execution state")
	}
	// Unknown execution ID is NOT_FOUND, not a panic or a state leak.
	unknown := inspectEnvelope(clock.Now(), "unknown-inspect", "client", "does-not-exist")
	responses, err = core.Handle(context.Background(), unknown)
	if !errors.Is(err, ErrExecutionNotFound) {
		t.Fatalf("unknown inspect error = %v", err)
	}
	if len(responses) != 1 || responses[0].GetCommandError().GetCode() != "NOT_FOUND" {
		t.Fatalf("unknown inspect responses=%v", responses)
	}
}

// F10-01: an invalid resource profile fails startup before capacity is advertised.
func TestInvalidResourceProfileFailsStartup(t *testing.T) {
	clock := newFakeClock()
	_, err := New(Config{
		Identity: []byte("allocator"), Capacity: map[string]uint32{"default": 1},
		OfferTTL: 30 * time.Second, Now: clock.Now, NewID: sequenceIDs(),
		Admission: AdmissionPolicy{Profiles: map[string]r1sruntime.Resources{"default": {MemoryBytes: 16 << 20, CPUMilli: -1, Pids: 1}}},
	}, newFakeRuntime())
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("New error = %v, want ErrInvalidConfig", err)
	}
}

// F10-01: an execution keeps its admitted resource profile across restart.
func TestResourceProfileSurvivesRestart(t *testing.T) {
	clock := newFakeClock()
	store := newMemoryStateStore()
	profile := r1sruntime.Resources{MemoryBytes: 64 << 20, CPUMilli: 1000, Pids: 32}
	admission := AdmissionPolicy{Profiles: map[string]r1sruntime.Resources{"default": profile}}
	first := newStoredAllocatorWithAdmission(t, clock, newFakeRuntime(), store, admission)
	offer := mustHandle(t, first, requestEnvelope(clock.Now(), "request", "client", "request")).GetExecutionOffer()
	mustHandle(t, first, assignEnvelope(clock.Now(), "assign", "client", "request", offer.GetOfferId(), "execution"))

	restartedRuntime := newFakeRuntime()
	restarted := newStoredAllocatorWithAdmission(t, clock, restartedRuntime, store, admission)
	snapshot, ok := restarted.Execution("execution")
	if !ok || snapshot.State.GetPhase() != r1sv1.ExecutionPhase_EXECUTION_PHASE_RUNNING {
		t.Fatalf("recovered execution = %+v", snapshot)
	}
	exp, ok := restarted.Offer(offer.GetOfferId())
	if !ok || (!exp.Outstanding && !exp.Assigned) {
		t.Fatalf("offer snapshot = %+v", exp)
	}
	// The recovered running execution must still carry its profile to the runtime.
	if err := restarted.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if restartedRuntime.recoverCount() != 1 {
		t.Fatalf("recover calls = %d, want 1", restartedRuntime.recoverCount())
	}
}

// F10-02: admission policy gates reservation and execution, and one client's
// quota cannot be exhausted by another client.
func TestAdmissionAllowlistAndIdentityQuotas(t *testing.T) {
	clock := newFakeClock()
	clientA := admitClientBytes("client-a")
	clientB := admitClientBytes("client-b")
	policy := AdmissionPolicy{
		Profiles:       map[string]r1sruntime.Resources{"default": {MemoryBytes: 8 << 20, CPUMilli: 1000, Pids: 8}},
		AllowedClients: []string{hex.EncodeToString(clientA), hex.EncodeToString(clientB)},
		DefaultQuota:   Quota{Offers: 4, Executions: 2},
		ClientQuotas:   map[string]Quota{hex.EncodeToString(clientA): {Offers: 1, Executions: 1}},
	}
	core := newStoredAllocatorWithAdmissionWithCapacity(t, clock, newFakeRuntime(), newMemoryStateStore(), policy, 2)

	envelopeFor := func(message string, owner []byte, requestID string) *r1sv1.Envelope {
		request := requestEnvelope(clock.Now(), message, "placeholder", requestID)
		request.Sender = owner
		return request
	}

	// A spoofed payload identity does not bypass admission: the transport
	// authenticated sender is the authority.
	spoof := envelopeFor("spoof", admitClientBytes("attacker"), "spoof-request")
	responses, err := core.Handle(context.Background(), spoof)
	if !errors.Is(err, ErrAdmission) {
		t.Fatalf("spoof error = %v, want ErrAdmission", err)
	}
	if len(responses) != 1 || responses[0].GetCommandError().GetCode() != "DENIED" {
		t.Fatalf("spoof responses = %v, want DENIED", responses)
	}

	// client-a fills its one-offer quota; client-b is unaffected.
	mustHandle(t, core, envelopeFor("a-1", clientA, "a-request"))
	responses, err = core.Handle(context.Background(), envelopeFor("a-2", clientA, "a-request-2"))
	if !errors.Is(err, ErrCapacityExhausted) || len(responses) != 1 || responses[0].GetCommandError().GetCode() != "CAPACITY" {
		t.Fatalf("client-a quota error = %v responses=%v", err, responses)
	}
	mustHandle(t, core, envelopeFor("b-1", clientB, "b-request"))
	if core.Available("default") != 0 {
		t.Fatal("client-b did not reserve its share")
	}
}

// F11-01: after a finished execution is collected, an old assignment replayed
// after cleanup cannot restart work; the reused command returns EXPIRED.
func TestCollectedResultCannotRestartWork(t *testing.T) {
	clock := newFakeClock()
	store, runtime := newMemoryStateStore(), newFakeRuntime()
	core := newStoredTestAllocator(t, clock, runtime, store)
	offer := mustHandle(t, core, requestEnvelope(clock.Now(), "request", "client", "request")).GetExecutionOffer()
	assign := assignEnvelope(clock.Now(), "assign", "client", "request", offer.GetOfferId(), "execution")
	mustHandle(t, core, assign)
	exitCode := int32(0)
	if err := runtime.complete("execution", r1sruntime.Completion{ExecutionID: "execution", ExitCode: &exitCode}); err != nil {
		t.Fatal(err)
	}
	// Advance past the default retention (24h) so Sweep collects the result.
	clock.Advance(25 * time.Hour)
	if err := core.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	responses, err := core.Handle(context.Background(), assign)
	if !errors.Is(err, ErrResultExpired) {
		t.Fatalf("replayed assignment after cleanup = %v, want ErrResultExpired", err)
	}
	if len(responses) != 1 || responses[0].GetCommandError().GetCode() != "EXPIRED" {
		t.Fatalf("cleanup responses = %v", responses)
	}
	if runtime.startCount() != 1 {
		t.Fatalf("workload restarts = %d, want 1", runtime.startCount())
	}
	if core.Available("default") != 1 {
		t.Fatal("collected result did not release capacity")
	}
}

// F11-01: tombstones are durable across restart; the collected assignment is
// still refused after the allocator reloads its state.
func TestTombstonesSurviveRestart(t *testing.T) {
	clock := newFakeClock()
	store := newMemoryStateStore()
	runtime := newFakeRuntime()
	core := newStoredTestAllocator(t, clock, runtime, store)
	offer := mustHandle(t, core, requestEnvelope(clock.Now(), "request", "client", "request")).GetExecutionOffer()
	assign := assignEnvelope(clock.Now(), "assign", "client", "request", offer.GetOfferId(), "execution")
	mustHandle(t, core, assign)
	exitCode := int32(0)
	if err := runtime.complete("execution", r1sruntime.Completion{ExecutionID: "execution", ExitCode: &exitCode}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(25 * time.Hour)
	if err := core.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}

	restarted := newStoredTestAllocator(t, clock, newFakeRuntime(), store)
	responses, err := restarted.Handle(context.Background(), assign)
	if !errors.Is(err, ErrResultExpired) {
		t.Fatalf("restarted allocator error = %v, want ErrResultExpired", err)
	}
	if len(responses) != 1 || responses[0].GetCommandError().GetCode() != "EXPIRED" {
		t.Fatalf("restarted responses = %v", responses)
	}
	if restarted.Available("default") != 1 {
		t.Fatal("restarted allocator did not keep collected capacity free")
	}
}

func admitClientBytes(name string) []byte {
	value := make([]byte, 16)
	copy(value, name)
	return value
}

// fakeLogStore records how many times logs are read, proving that lifecycle
// traffic never touches the log store.
type fakeLogStore struct {
	data  map[string]map[string][]byte
	reads int
}

func (s *fakeLogStore) Read(_ context.Context, id, stream string, offset uint64, limit uint32) (r1sruntime.LogChunk, error) {
	s.reads++
	data := s.data[id][stream]
	if offset > uint64(len(data)) {
		return r1sruntime.LogChunk{}, r1sruntime.ErrLogOffset
	}
	end := min(uint64(len(data)), offset+uint64(limit))
	return r1sruntime.LogChunk{Data: data[offset:end], NextOffset: end, EOF: end == uint64(len(data))}, nil
}

func (s *fakeLogStore) Remove(context.Context, string) error { return nil }

// F9-01 + F9-02: container logs live behind an explicit authenticated request.
// Lifecycle traffic — request, assign, inspect, state, errors — never reads the
// log store; only an explicit owner logs request does.
func TestLogsOnlyByExplicitOwnerRequest(t *testing.T) {
	clock := newFakeClock()
	runtime := newFakeRuntime()
	logs := &fakeLogStore{data: map[string]map[string][]byte{"execution": {"stderr": []byte("boom: out of memory\n")}}}
	core := newAllocatorWithLogs(t, clock, runtime, logs, 1)

	offer := mustHandle(t, core, requestEnvelope(clock.Now(), "request", "client", "request")).GetExecutionOffer()
	responses, err := core.Handle(context.Background(), assignEnvelope(clock.Now(), "assign", "client", "request", offer.GetOfferId(), "execution"))
	if err != nil || len(responses) != 1 || responses[0].GetExecutionState().GetPhase() != r1sv1.ExecutionPhase_EXECUTION_PHASE_RUNNING {
		t.Fatalf("assign responses=%v err=%v", responses, err)
	}
	mustHandle(t, core, inspectEnvelope(clock.Now(), "inspect", "client", "execution"))
	if logs.reads != 0 {
		t.Fatalf("lifecycle traffic read logs %d times, want 0", logs.reads)
	}

	// A foreign identity is denied and does not read the store.
	before := logs.reads
	responses, err = core.Handle(context.Background(), logsRequest(clock.Now(), "spy-logs", "other", "execution", "stderr"))
	if !errors.Is(err, ErrExecutionNotFound) || len(responses) != 1 || responses[0].GetCommandError().GetCode() != "NOT_FOUND" {
		t.Fatalf("spy logs responses=%v err=%v", responses, err)
	}
	if logs.reads != before {
		t.Fatal("denied log request read the store")
	}

	// The owner retrieves a bounded, checksummed chunk.
	responses, err = core.Handle(context.Background(), logsRequest(clock.Now(), "logs", "client", "execution", "stderr"))
	if err != nil {
		t.Fatal(err)
	}
	if len(responses) != 1 {
		t.Fatalf("logs responses = %d", len(responses))
	}
	chunk := responses[0].GetExecutionLogsResponse()
	if chunk == nil || chunk.GetData() == nil {
		t.Fatalf("logs response = %v", responses[0])
	}
	if responses[0].GetExecutionState() != nil {
		t.Fatal("logs response carried execution state")
	}
	sum := sha256.Sum256(chunk.GetData())
	if !bytes.Equal(sum[:], chunk.GetSha256()) {
		t.Fatal("logs checksum mismatch")
	}
	if logs.reads != before+1 {
		t.Fatalf("owner reads = %d, want %d", logs.reads, before+1)
	}
}

func logsRequest(now time.Time, message, owner, execution, stream string) *r1sv1.Envelope {
	return &r1sv1.Envelope{
		MessageId: message, Sender: []byte(owner), SentAt: timestamppb.New(now),
		Payload: &r1sv1.Envelope_ExecutionLogsRequest{ExecutionLogsRequest: &r1sv1.ExecutionLogsRequest{
			ExecutionId: execution, Stream: stream, Offset: 0, MaxBytes: protocol.MaxLogBytes,
		}},
	}
}

func newAllocatorWithLogs(t *testing.T, clock *fakeClock, runtime *fakeRuntime, logs r1sruntime.LogStore, capacity uint32) *Allocator {
	t.Helper()
	result, err := New(Config{
		Identity: []byte("allocator"), Capacity: map[string]uint32{"default": capacity},
		OfferTTL: 30 * time.Second, Now: clock.Now, NewID: sequenceIDs(), Logs: logs,
	}, runtime)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func newStoredAllocatorWithAdmission(t *testing.T, clock *fakeClock, runtime *fakeRuntime, store StateStore, admission AdmissionPolicy) *Allocator {
	return newStoredAllocatorWithAdmissionWithCapacity(t, clock, runtime, store, admission, 1)
}

func newStoredAllocatorWithAdmissionWithCapacity(t *testing.T, clock *fakeClock, runtime *fakeRuntime, store StateStore, admission AdmissionPolicy, capacity uint32) *Allocator {
	t.Helper()
	result, err := New(Config{
		Identity: []byte("allocator"), Capacity: map[string]uint32{"default": capacity},
		OfferTTL: 30 * time.Second, Now: clock.Now, NewID: sequenceIDs(), Store: store, Admission: admission,
	}, runtime)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

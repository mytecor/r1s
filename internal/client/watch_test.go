package client

import (
	"context"
	"errors"
	"testing"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"google.golang.org/protobuf/proto"
)

// F13-01: accepted transitions are assigned monotonically increasing durable
// sequences, delivered in revision order to a live observer, and replayable
// from an arbitrary durable position after the fact.
func TestWatchJournalAssignsOrderedDurableSequences(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	core, executionID := newClientWithExecution(t, now)

	var observed []WatchEvent
	cancel := core.SubscribeWatch(func(event WatchEvent) { observed = append(observed, event) })
	defer cancel()

	for index, revision := range []uint64{1, 2, 3} {
		phase := r1sv1.ExecutionPhase_EXECUTION_PHASE_STARTING + r1sv1.ExecutionPhase(index)
		state := revisionStateEnvelope(now.Add(time.Duration(index)*time.Minute), "allocator", executionID, phase, revision)
		if err := core.Handle(context.Background(), state); err != nil {
			t.Fatal(err)
		}
	}

	if len(observed) != 3 {
		t.Fatalf("observed %d events, want 3", len(observed))
	}
	for index, event := range observed {
		if want := uint64(index + 1); event.Sequence != want {
			t.Fatalf("event %d sequence = %d, want %d", index, event.Sequence, want)
		}
		if event.ExecutionID != executionID {
			t.Fatalf("event %d execution = %q", index, event.ExecutionID)
		}
	}

	// Replay from the middle reproduces only later revisions, in order.
	events, contiguous := core.WatchAfter(1)
	if !contiguous || len(events) != 2 {
		t.Fatalf("WatchAfter(1) = %d events contiguous=%v, want 2 true", len(events), contiguous)
	}
	if events[0].Sequence != 2 || events[1].Sequence != 3 || events[0].State.GetRevision() != 2 || events[1].State.GetRevision() != 3 {
		t.Fatalf("replay revisions out of order: %v %v", events[0], events[1])
	}
}

// F13-01: sequences survive service restarts and are never reused; a watcher
// resuming from its durable position observes exactly the revisions that
// happened after the restart.
func TestWatchSequencesSurviveRestart(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	store := newMemoryStore()
	core, err := New(Config{Identity: []byte("client"), Store: store, Now: func() time.Time { return now }, NewID: sequenceIDs("r", "m", "e", "am")})
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
	executionID := assignment.GetExecutionAssign().GetExecutionId()

	// One transition with sequence 1 is durably committed.
	if err := core.Handle(context.Background(), revisionStateEnvelope(now, "allocator", executionID, r1sv1.ExecutionPhase_EXECUTION_PHASE_RUNNING, 1)); err != nil {
		t.Fatal(err)
	}
	seqBeforeRestart := core.WatchSeq()

	// Restart the service on the same store.
	restarted, err := New(Config{Identity: []byte("client"), Store: store, Now: func() time.Time { return now }, NewID: sequenceIDs("r2", "m2", "e2", "am2")})
	if err != nil {
		t.Fatal(err)
	}
	if got := restarted.WatchSeq(); got != seqBeforeRestart {
		t.Fatalf("WatchSeq after restart = %d, want %d", got, seqBeforeRestart)
	}

	// Resuming from the last awarded sequence yields the follow-up transition.
	events, contiguous := restarted.WatchAfter(seqBeforeRestart)
	if !contiguous || len(events) != 0 {
		t.Fatalf("WatchAfter(restart) = %d events contiguous=%v, want 0 true", len(events), contiguous)
	}
	if err := restarted.Handle(context.Background(), revisionStateEnvelope(now.Add(time.Minute), "allocator", executionID, r1sv1.ExecutionPhase_EXECUTION_PHASE_COMPLETED, 2)); err != nil {
		t.Fatal(err)
	}
	events, contiguous = restarted.WatchAfter(seqBeforeRestart)
	if !contiguous || len(events) != 1 {
		t.Fatalf("WatchAfter(%d after restart) = %d events contiguous=%v", seqBeforeRestart, len(events), contiguous)
	}
	if events[0].Sequence != seqBeforeRestart+1 || events[0].State.GetRevision() != 2 {
		t.Fatalf("post-restart event = %+v", events[0])
	}
}

// F13-01: once the retained journal horizon passes a watcher's position, the
// watcher is told it lost contiguity instead of silently receiving a gap.
func TestWatchJournalReportsGapAfterHorizon(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	core, executionID := newClientWithExecution(t, now)

	// Overflow the journal well past the retained horizon so a watcher still
	// positioned at the first events needs an event that fell off.
	for revision := uint64(1); revision <= 2*watchJournalCapacity+2; revision++ {
		state := revisionStateEnvelope(now.Add(time.Duration(revision)*time.Minute), "allocator", executionID, r1sv1.ExecutionPhase_EXECUTION_PHASE_STARTING, revision)
		if err := core.Handle(context.Background(), state); err != nil {
			t.Fatal(err)
		}
	}

	// A watcher still positioned before the retained horizon must re-sync.
	if _, contiguous := core.WatchAfter(1); contiguous {
		t.Fatal("WatchAfter(1) stayed contiguous after the journal overflowed")
	}
	// A position at the retained edge stays contiguous.
	last := core.watchJournal.events[len(core.watchJournal.events)-1].seq
	if events, contiguous := core.WatchAfter(last); !contiguous || len(events) != 0 {
		t.Fatalf("WatchAfter(last=%d) = %d events contiguous=%v", last, len(events), contiguous)
	}
}

// F13-01: a failed persist never assigns a sequence or notifies observers, so a
// crash cannot surface a transition that was never durably committed.
func TestWatchFailedPersistNeverEmits(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	core, executionID := newClientWithExecution(t, now)

	notified := 0
	cancel := core.SubscribeWatch(func(WatchEvent) { notified++ })
	defer cancel()

	core.store = failingStore{}
	seqBefore := core.WatchSeq()
	err := core.Handle(context.Background(), revisionStateEnvelope(now, "allocator", executionID, r1sv1.ExecutionPhase_EXECUTION_PHASE_RUNNING, 1))
	if err == nil {
		t.Fatal("Handle() succeeded against a failing store")
	}
	if core.WatchSeq() != seqBefore {
		t.Fatalf("WatchSeq advanced on failed persist: %d -> %d", seqBefore, core.WatchSeq())
	}
	if notified != 0 {
		t.Fatalf("observer notified %d times on failed persist", notified)
	}
}

func newMemoryStore() *memoryStore { return &memoryStore{} }

type memoryStore struct {
	data []byte
}

func (m *memoryStore) Load(context.Context) ([]byte, error) {
	return append([]byte(nil), m.data...), nil
}

func (m *memoryStore) Save(_ context.Context, data []byte) error {
	m.data = append([]byte(nil), data...)
	return nil
}

type failingStore struct{}

func (failingStore) Load(context.Context) ([]byte, error) { return nil, nil }
func (failingStore) Save(context.Context, []byte) error   { return errors.New("disk full") }

var _ = proto.Equal

package client

import (
	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"google.golang.org/protobuf/proto"
)

// WatchEvent is one durable, monotonically ordered execution-state transition.
// Sequence is assigned only after the transition is durably persisted, so a
// watcher can resume from Sequence without inventing or reordering states and
// service restarts never reuse a sequence.
type WatchEvent struct {
	Sequence    uint64
	ExecutionID string
	State       *r1sv1.ExecutionState
}

// watchJournalCapacity bounds retained transitions. A slow watcher that lets
// its position fall behind the retained horizon must re-synchronize from a
// durable position; it never stalls the shared journal.
const watchJournalCapacity = 1024

// watchJournal is a contiguous window of accepted transitions ordered by seq.
// Events older than the window are dropped, but watchSequence never decreases,
// so assigned sequences are never reused across the service lifetime.
type watchJournal struct {
	firstSeq uint64
	events   []watchEvent
}

type watchEvent struct {
	seq         uint64
	executionID string
	state       *r1sv1.ExecutionState
}

// WatchSeq returns the highest assigned durable sequence. A watcher that wants
// to observe only future transitions starts after this position.
func (o *Client) WatchSeq() uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.watchSequence
}

// WatchAfter returns accepted transitions with Sequence > previouslyAssigned,
// in order. previouslyAssigned == 0 replays the whole retained journal.
// contiguous=false signals that the position fell off the retained horizon and
// the watcher must re-synchronize from a fresh durable position instead of
// guessing at missing revisions.
func (o *Client) WatchAfter(previouslyAssigned uint64) (events []WatchEvent, contiguous bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.watchAfterLocked(previouslyAssigned)
}

func (o *Client) watchAfterLocked(previouslyAssigned uint64) ([]WatchEvent, bool) {
	if previouslyAssigned == 0 {
		replay := make([]WatchEvent, 0, len(o.watchJournal.events))
		for _, event := range o.watchJournal.events {
			replay = append(replay, event.copy())
		}
		return replay, true
	}
	if len(o.watchJournal.events) == 0 {
		// No retained transitions. The position is contiguous only if it is still
		// within the assigned sequence space.
		return nil, previouslyAssigned <= o.watchSequence
	}
	last := o.watchJournal.events[len(o.watchJournal.events)-1].seq
	if previouslyAssigned >= last {
		return nil, true
	}
	if previouslyAssigned+1 < o.watchJournal.firstSeq {
		return nil, false
	}
	start := int(previouslyAssigned + 1 - o.watchJournal.firstSeq)
	replay := make([]WatchEvent, 0, len(o.watchJournal.events)-start)
	for _, event := range o.watchJournal.events[start:] {
		replay = append(replay, event.copy())
	}
	return replay, true
}

// watchSubscription is one registered observer. The pointer is the unique,
// comparable identity used to remove it safely.
type watchSubscription struct {
	callback func(WatchEvent)
}

// SubscribeWatch registers an observer that is notified of every accepted
// transition after its position. Observers MUST NOT block: notify runs with the
// client lock held, so an observer that blocks stalls the whole client. The
// returned cancel removes the observer exactly once.
func (o *Client) SubscribeWatch(observer func(WatchEvent)) (cancel func()) {
	subscription := &watchSubscription{callback: observer}
	o.mu.Lock()
	o.watchObservers = append(o.watchObservers, subscription)
	o.mu.Unlock()
	canceled := false
	return func() {
		o.mu.Lock()
		defer o.mu.Unlock()
		if canceled {
			return
		}
		canceled = true
		for index, candidate := range o.watchObservers {
			if candidate == subscription {
				o.watchObservers = append(o.watchObservers[:index], o.watchObservers[index+1:]...)
				break
			}
		}
	}
}

// watchNotifyLocked delivers an already-durable event to live observers without
// running under a write lock held during persist. Callers must hold o.mu.
func (o *Client) watchNotifyLocked(event WatchEvent) {
	for _, subscription := range o.watchObservers {
		subscription.callback(event)
	}
}

// watchAppendLocked assigns the next durable sequence and retains the transition
// within the bounded journal. It returns the assigned event but does not notify
// observers; the caller persists first and only then wakes observers via
// watchNotifyLocked so a crash cannot surface a sequence to a watcher that was
// never durably committed. Callers must hold o.mu.
func (o *Client) watchAppendLocked(executionID string, state *r1sv1.ExecutionState) WatchEvent {
	o.watchSequence++
	event := watchEvent{seq: o.watchSequence, executionID: executionID, state: proto.Clone(state).(*r1sv1.ExecutionState)}
	if o.watchJournal.events == nil {
		o.watchJournal.firstSeq = o.watchSequence
	}
	if len(o.watchJournal.events) == watchJournalCapacity {
		o.watchJournal.events = o.watchJournal.events[1:]
		o.watchJournal.firstSeq++
	}
	o.watchJournal.events = append(o.watchJournal.events, event)
	return WatchEvent{Sequence: event.seq, ExecutionID: executionID, State: proto.Clone(state).(*r1sv1.ExecutionState)}
}

func (e watchEvent) copy() WatchEvent {
	return WatchEvent{Sequence: e.seq, ExecutionID: e.executionID, State: proto.Clone(e.state).(*r1sv1.ExecutionState)}
}

// watchJournalLocked returns the retained journal for durable storage, so a
// watcher resuming from its prior position after a service restart observes the
// exact same revisions with the exact same sequences. Callers must hold o.mu.
func (o *Client) watchJournalLocked() []persistedWatchEvent {
	result := make([]persistedWatchEvent, 0, len(o.watchJournal.events))
	for _, event := range o.watchJournal.events {
		encoded, _ := proto.Marshal(event.state)
		result = append(result, persistedWatchEvent{Sequence: event.seq, ExecutionID: event.executionID, State: encoded})
	}
	return result
}

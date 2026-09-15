package allocator

import (
	"context"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"google.golang.org/protobuf/proto"
)

func (a *Allocator) beginReplay(key string, envelope *r1sv1.Envelope, now time.Time) (*replayEntry, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.expireOffersLocked(now)
	entry, duplicate, err := a.replay.begin(key, envelope, now)
	if err != nil || duplicate {
		return entry, duplicate, err
	}
	if err := a.persistLocked(context.Background()); err != nil {
		delete(a.replay.entries, key)
		return nil, false, err
	}
	return entry, false, nil
}

func (a *Allocator) finishReplay(entry *replayEntry, responses []*r1sv1.Envelope, err error) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.replay.finish(entry, responses, err)
	return a.persistLocked(context.Background())
}

type replayEntry struct {
	seenAt    time.Time
	done      chan struct{}
	envelope  *r1sv1.Envelope
	responses []*r1sv1.Envelope
	err       error
}

// replayCache owns duplicate-command coalescing and bounded replay retention.
// Persistence remains the allocator's responsibility; callers must hold
// Allocator.mu while mutating the cache.
type replayCache struct {
	ttl      time.Duration
	capacity int
	entries  map[string]*replayEntry
}

func newReplayCache(ttl time.Duration, capacity int) replayCache {
	return replayCache{ttl: ttl, capacity: capacity, entries: make(map[string]*replayEntry)}
}

func (c *replayCache) begin(key string, envelope *r1sv1.Envelope, now time.Time) (*replayEntry, bool, error) {
	c.prune(now)
	if entry, exists := c.entries[key]; exists {
		if !proto.Equal(entry.envelope, envelope) {
			return nil, false, ErrReplayConflict
		}
		return entry, true, nil
	}
	c.evict(c.capacity - 1)
	if len(c.entries) >= c.capacity {
		return nil, false, ErrReplayCapacity
	}
	entry := &replayEntry{seenAt: now, done: make(chan struct{}), envelope: proto.Clone(envelope).(*r1sv1.Envelope)}
	c.entries[key] = entry
	return entry, false, nil
}

func (c *replayCache) finish(entry *replayEntry, responses []*r1sv1.Envelope, err error) {
	entry.responses = cloneEnvelopes(responses)
	entry.err = err
	close(entry.done)
	c.evict(c.capacity)
}

func (c *replayCache) prune(now time.Time) {
	cutoff := now.Add(-c.ttl)
	for key, entry := range c.entries {
		if !entry.seenAt.After(cutoff) && replayDone(entry) {
			delete(c.entries, key)
		}
	}
}

func (c *replayCache) evict(limit int) {
	for len(c.entries) > limit {
		var oldestKey string
		var oldest time.Time
		for candidate, entry := range c.entries {
			if replayDone(entry) && (oldestKey == "" || entry.seenAt.Before(oldest)) {
				oldestKey, oldest = candidate, entry.seenAt
			}
		}
		if oldestKey == "" {
			return
		}
		delete(c.entries, oldestKey)
	}
}

func (c replayCache) clone() replayCache {
	cloned := newReplayCache(c.ttl, c.capacity)
	for key, entry := range c.entries {
		cloned.entries[key] = entry
	}
	return cloned
}

func replayDone(entry *replayEntry) bool {
	select {
	case <-entry.done:
		return true
	default:
		return false
	}
}

func cloneEnvelopes(envelopes []*r1sv1.Envelope) []*r1sv1.Envelope {
	if envelopes == nil {
		return nil
	}
	cloned := make([]*r1sv1.Envelope, len(envelopes))
	for index, envelope := range envelopes {
		cloned[index] = proto.Clone(envelope).(*r1sv1.Envelope)
	}
	return cloned
}

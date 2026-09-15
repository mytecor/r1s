package allocator

import (
	"bytes"
	"context"
	"errors"
	"maps"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	r1sruntime "github.com/mytecor/r1s/internal/runtime"
)

const CommandHorizon = 7 * 24 * time.Hour
const DefaultRetention = 24 * time.Hour
const DefaultMaxRecords = 10000

var ErrResultExpired = errors.New("retained result expired")
var ErrCommandExpired = errors.New("command outside replay horizon")

type tombstone struct {
	Client         []byte    `json:"client"`
	RequestID      string    `json:"request_id"`
	OfferID        string    `json:"offer_id"`
	ExecutionID    string    `json:"execution_id,omitempty"`
	ExpiresAt      time.Time `json:"expires_at"`
	CleanupPending bool      `json:"cleanup_pending,omitempty"`
}

func retention(policy *r1sv1.ExecutionPolicy) time.Duration {
	if policy.GetResultRetention() == nil {
		return DefaultRetention
	}
	return min(policy.GetResultRetention().AsDuration(), CommandHorizon)
}

func (a *Allocator) effectiveNowLocked() time.Time {
	now := a.now().UTC()
	if now.After(a.highWater) {
		a.highWater = now
	}
	return a.highWater
}

// Freshness is checked before replay lookup. A replay horizon bounds tombstones;
// messages outside it cannot resurrect work even after history has been collected.
func (a *Allocator) checkFreshness(e *r1sv1.Envelope) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := a.effectiveNowLocked()
	sent := e.GetSentAt().AsTime()
	if sent.Before(now.Add(-CommandHorizon)) || sent.After(now.Add(2*time.Minute)) {
		return ErrCommandExpired
	}
	id := ""
	if q := e.GetExecutionInspect(); q != nil {
		id = q.GetExecutionId()
	}
	if q := e.GetExecutionCancel(); q != nil {
		id = q.GetExecutionId()
	}
	if q := e.GetExecutionAssign(); q != nil {
		id = q.GetExecutionId()
	}
	if q := e.GetExecutionLogsRequest(); q != nil {
		id = q.GetExecutionId()
	}
	if record := a.executions[id]; record != nil && bytes.Equal(record.client, e.GetSender()) && terminal(record.phase) && !record.retainUntil.After(now) {
		return ErrResultExpired
	}
	for _, dead := range a.tombstones {
		if dead.ExecutionID != "" && dead.ExecutionID == id && bytes.Equal(dead.Client, e.GetSender()) {
			return ErrResultExpired
		}
	}
	return nil
}

// Sweep commits metadata collection before deleting local log files. Pending
// cleanup survives failures and restart. In-flight transitions are never collected.
func (a *Allocator) Sweep(ctx context.Context) error {
	a.mu.Lock()
	oldOffers, oldExecutions, oldRequests, oldReplay, oldDead, oldUsed, oldClock := a.offers, a.executions, a.requests, a.replay, a.tombstones, a.capacity.snapshot(), a.highWater
	a.offers = maps.Clone(a.offers)
	for id, record := range a.offers {
		copy := *record
		a.offers[id] = &copy
	}
	a.executions = maps.Clone(a.executions)
	a.requests = maps.Clone(a.requests)
	a.replay = a.replay.clone()
	a.tombstones = maps.Clone(a.tombstones)
	now := a.effectiveNowLocked()
	a.expireOffersLocked(now)
	a.replay.prune(now)
	busy := make(map[string]bool)
	for _, entry := range a.replay.entries {
		if !replayDone(entry) {
			e := entry.envelope
			if q := e.GetExecutionRequest(); q != nil {
				busy[authorityKey(e.GetSender(), q.GetRequestId())] = true
			}
			if q := e.GetExecutionAssign(); q != nil {
				busy[authorityKey(e.GetSender(), q.GetRequestId())] = true
			}
			if q := e.GetExecutionCancel(); q != nil {
				if r := a.executions[q.GetExecutionId()]; r != nil {
					busy[authorityKey(r.client, r.request.GetRequestId())] = true
				}
			}
		}
	}
	for id, offer := range a.offers {
		key := authorityKey(offer.client, offer.offer.GetRequestId())
		if busy[key] {
			continue
		}
		eligible := offer.status == offerExpired || offer.status == offerReleased
		if offer.status == offerAssigned {
			r := a.executions[offer.execution]
			eligible = r != nil && terminal(r.phase) && !r.retainUntil.After(now)
		}
		if !eligible {
			continue
		}
		a.tombstones[id] = tombstone{Client: cloneBytes(offer.client), RequestID: offer.offer.GetRequestId(), OfferID: id, ExecutionID: offer.execution, ExpiresAt: now.Add(CommandHorizon), CleanupPending: offer.execution != ""}
		delete(a.offers, id)
		delete(a.requests, key)
		delete(a.executions, offer.execution)
	}
	retired := make(map[string]bool)
	for id, dead := range a.tombstones {
		retired[authorityKey(dead.Client, dead.RequestID)] = true
		if !dead.CleanupPending && !dead.ExpiresAt.After(now) {
			delete(a.tombstones, id)
		}
	}
	for key, entry := range a.replay.entries {
		if replayDone(entry) {
			e := entry.envelope
			drop := false
			if q := e.GetExecutionRequest(); q != nil {
				drop = retired[authorityKey(e.GetSender(), q.GetRequestId())]
			}
			if q := e.GetExecutionAssign(); q != nil {
				drop = retired[authorityKey(e.GetSender(), q.GetRequestId())]
			}
			if q := e.GetExecutionCancel(); q != nil {
				drop = a.executions[q.GetExecutionId()] == nil
			}
			if q := e.GetExecutionInspect(); q != nil {
				drop = a.executions[q.GetExecutionId()] == nil
			}
			if drop {
				delete(a.replay.entries, key)
			}
		}
	}
	if err := a.persistLocked(ctx); err != nil {
		a.offers, a.executions, a.requests, a.replay, a.tombstones, a.highWater = oldOffers, oldExecutions, oldRequests, oldReplay, oldDead, oldClock
		a.capacity.restore(oldUsed)
		a.mu.Unlock()
		return err
	}
	var pending []tombstone
	for _, dead := range a.tombstones {
		if dead.CleanupPending {
			pending = append(pending, dead)
		}
	}
	a.mu.Unlock()
	var allErr error
	for _, dead := range pending {
		var err error
		if forgetter, ok := a.runtime.(r1sruntime.Forgetter); ok {
			err = forgetter.Forget(ctx, dead.ExecutionID)
		}
		if err == nil && a.logs != nil {
			err = a.logs.Remove(ctx, dead.ExecutionID)
		}
		if err != nil {
			allErr = errors.Join(allErr, err)
			continue
		}
		a.mu.Lock()
		current := a.tombstones[dead.OfferID]
		current.CleanupPending = false
		a.tombstones[dead.OfferID] = current
		if err := a.persistLocked(ctx); err != nil {
			a.tombstones[dead.OfferID] = dead
			allErr = errors.Join(allErr, err)
		}
		a.mu.Unlock()
	}
	return allErr
}

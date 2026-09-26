package allocator

import (
	"bytes"
	"errors"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
)

// Retention policy: the time bounds that decide how long allocator offers,
// executions, and commands stay alive and how long terminal metadata is
// retrievable. The Sweep collector that applies this policy lives in
// retention.go.

// CommandHorizon bounds how far back an incoming command may be sent and how
// long a tombstone guards its resources. It matches maxLeaseTTL so a durable
// lease never outlives the tombstones that guard it.
const CommandHorizon = 7 * 24 * time.Hour

// DefaultRetention is the allocator's operator-configured terminal-record
// retention horizon applied when no explicit --retention is set. Since the F22
// cutover, retention is allocator-local policy — a workload cannot choose it —
// and every configured value is bounded below by CommandHorizon.
const DefaultRetention = 24 * time.Hour

// DefaultMaxRecords bounds the allocator's retained terminal records.
const DefaultMaxRecords = 10000

var ErrResultExpired = errors.New("retained result expired")
var ErrCommandExpired = errors.New("command outside replay horizon")

// tombstone records a collected offer/execution so a late command for it is
// rejected as expired instead of silently resurrecting retired work.
type tombstone struct {
	Client         []byte    `json:"client"`
	RequestID      string    `json:"request_id"`
	OfferID        string    `json:"offer_id"`
	ExecutionID    string    `json:"execution_id,omitempty"`
	ExpiresAt      time.Time `json:"expires_at"`
	CleanupPending bool      `json:"cleanup_pending,omitempty"`
}

// retention bounds an operator-configured retention horizon by the command
// replay horizon, so no terminal record outlives the tombstones that guard it
// and a configured value never exceeds the replay-safe ceiling.
func retention(configured time.Duration) time.Duration {
	return min(configured, CommandHorizon)
}

// effectiveNowLocked returns the allocator's monotonic now: it never moves
// backwards across restart or recovery, so a freshly persisted high-water mark
// keeps late commands rejected even after a clock regression.
func (a *Allocator) effectiveNowLocked() time.Time {
	now := a.now().UTC()
	if now.After(a.highWater) {
		a.highWater = now
	}
	return a.highWater
}

// checkFreshness validates that an inbound envelope is within the command
// replay horizon and does not name an execution whose retained result has
// expired. It runs before replay lookup so a message outside the horizon can
// never resurrect work even after history has been collected.
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
	if q := e.GetExecutionLeaseRenew(); q != nil {
		id = q.GetExecutionId()
	}
	if q := e.GetExecutionTunnelOpen(); q != nil {
		id = q.GetExecutionId()
	}
	if record := a.executions[id]; record != nil && bytes.Equal(record.client, e.GetSender()) && protocol.Terminal(record.phase) && !record.retainUntil.After(now) {
		return ErrResultExpired
	}
	for _, dead := range a.tombstones {
		if dead.ExecutionID != "" && dead.ExecutionID == id && bytes.Equal(dead.Client, e.GetSender()) {
			return ErrResultExpired
		}
	}
	return nil
}

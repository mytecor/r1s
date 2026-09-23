package allocator

import (
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	r1sruntime "github.com/mytecor/r1s/internal/runtime"
)

// stateVersion is the versioned on-disk allocator snapshot schema. It is
// incremented (never reused) on breaking format changes; loadLocked accepts
// the current version and the legacy v1.
const stateVersion = 2

// persistedOffer is the durable form of one offerRecord.
type persistedOffer struct {
	Resources    r1sruntime.Resources `json:"resources,omitzero"`
	Offer        []byte               `json:"offer"`
	Request      []byte               `json:"request"`
	Client       []byte               `json:"client,omitempty"`
	LegacySender []byte               `json:"owner,omitempty"`
	Status       offerStatus          `json:"status"`
	Execution    string               `json:"execution,omitempty"`
}

// persistedExecution is the durable form of one executionRecord.
type persistedExecution struct {
	RetainUntil   time.Time            `json:"retain_until,omitempty"`
	LeaseUntil    time.Time            `json:"lease_until,omitempty"`
	Resources     r1sruntime.Resources `json:"resources,omitzero"`
	ID            string               `json:"id"`
	OfferID       string               `json:"offer_id"`
	Client        []byte               `json:"client,omitempty"`
	LegacySender  []byte               `json:"owner,omitempty"`
	ResourceClass string               `json:"resource_class"`
	Request       []byte               `json:"request"`
	Phase         r1sv1.ExecutionPhase `json:"phase"`
	Detail        string               `json:"detail,omitempty"`
	ExitCode      *int32               `json:"exit_code,omitempty"`
	OccurredAt    time.Time            `json:"occurred_at"`
	StartedAt     time.Time            `json:"started_at"`
	Released      bool                 `json:"released"`
	Revision      uint64               `json:"revision,omitempty"`
}

// persistedReplay is the durable form of one replayCache entry.
type persistedReplay struct {
	Key       string    `json:"key"`
	SeenAt    time.Time `json:"seen_at"`
	Envelope  []byte    `json:"envelope"`
	Responses [][]byte  `json:"responses,omitempty"`
	Error     string    `json:"error,omitempty"`
	ErrorCode string    `json:"error_code,omitempty"`
	Complete  bool      `json:"complete"`
}

// persistedState is the full allocator snapshot persisted atomically after
// every accepted transition.
type persistedState struct {
	HighWater  time.Time            `json:"high_water,omitempty"`
	Tombstones map[string]tombstone `json:"tombstones,omitempty"`
	Version    int                  `json:"version"`
	Identity   []byte               `json:"identity"`
	Offers     []persistedOffer     `json:"offers,omitempty"`
	Executions []persistedExecution `json:"executions,omitempty"`
	Replay     []persistedReplay    `json:"replay,omitempty"`
}

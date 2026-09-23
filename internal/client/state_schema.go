package client

import "time"

// stateVersion is the versioned on-disk client snapshot schema. It is
// incremented (never reused) on breaking format changes; loadLocked accepts
// only the current version.
const stateVersion = 1

// persistedState is the full client snapshot persisted atomically after every
// accepted transition. The durable schema lives here; the load/persist behavior
// that maps it onto Client lives in state.go.
type persistedState struct {
	Version      int                   `json:"version"`
	Identity     []byte                `json:"identity"`
	WatchSeq     uint64                `json:"watch_seq,omitempty"`
	WatchJournal []persistedWatchEvent `json:"watch_journal,omitempty"`
	Allocators   []persistedAllocator  `json:"allocators,omitempty"`
	Requests     []persistedRequest    `json:"requests,omitempty"`
	Executions   []persistedExecution  `json:"executions,omitempty"`
}

// persistedWatchEvent is the durable form of one watch-journal event.
type persistedWatchEvent struct {
	Sequence    uint64 `json:"sequence"`
	ExecutionID string `json:"execution_id"`
	State       []byte `json:"state"`
}

// persistedAllocator is the durable form of one known allocator route.
type persistedAllocator struct {
	Identity    []byte            `json:"identity"`
	Destination string            `json:"destination"`
	Hops        uint8             `json:"hops"`
	Capacity    map[string]uint32 `json:"capacity,omitempty"`
	Node        []byte            `json:"node,omitempty"`
	// Tunnel advertisement (F21-02), advisory like Node/Capacity.
	TunnelHost        string `json:"tunnel_host,omitempty"`
	TunnelPort        int    `json:"tunnel_port,omitempty"`
	TunnelDestination string `json:"tunnel_destination,omitempty"`
}

// persistedRequest is the durable form of one client request.
type persistedRequest struct {
	Request     []byte           `json:"request"`
	MessageID   string           `json:"message_id"`
	CreatedAt   time.Time        `json:"created_at"`
	Offers      []persistedOffer `json:"offers,omitempty"`
	ExecutionID string           `json:"execution_id,omitempty"`
}

// persistedOffer is the durable form of one received offer.
type persistedOffer struct {
	Offer       []byte         `json:"offer"`
	AllocatorID []byte         `json:"allocator_id"`
	ReceivedAt  time.Time      `json:"received_at"`
	Release     *releaseIntent `json:"release,omitempty"`
}

// persistedExecution is the durable form of one execution record, including
// the durable lease-holding intent that keeps it alive.
type persistedExecution struct {
	ID                  string    `json:"id"`
	RequestID           string    `json:"request_id"`
	OfferID             string    `json:"offer_id"`
	AllocatorID         []byte    `json:"allocator_id"`
	Destination         string    `json:"destination"`
	AssignmentMessageID string    `json:"assignment_message_id"`
	AssignmentSentAt    time.Time `json:"assignment_sent_at"`
	InspectMessageID    string    `json:"inspect_message_id,omitempty"`
	InspectSentAt       time.Time `json:"inspect_sent_at,omitempty"`
	CancelMessageID     string    `json:"cancel_message_id,omitempty"`
	CancelSentAt        time.Time `json:"cancel_sent_at,omitempty"`
	CancelReason        string    `json:"cancel_reason,omitempty"`
	State               []byte    `json:"state,omitempty"`
	LeaseDurationNanos  int64     `json:"lease_duration,omitempty"`
	LeaseAllocators     []string  `json:"lease_allocators,omitempty"`
	LeaseLost           bool      `json:"lease_lost,omitempty"`
	LeaseRenewedAt      time.Time `json:"lease_renewed_at,omitempty"`
	LeaseRenewMessageID string    `json:"lease_renew_message_id,omitempty"`
	LeaseExpiresAt      time.Time `json:"lease_expires_at,omitempty"`
}

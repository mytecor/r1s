package allocator

import (
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
	r1sruntime "github.com/mytecor/r1s/internal/runtime"
)

// Configuration defaults shared by Config construction. Consumers may override
// any of them through Config.
const (
	defaultOfferTTL       = 30 * time.Second
	defaultReplayTTL      = 10 * time.Minute
	defaultReplayCapacity = 4096
	// defaultLeaseTTL is the initial lease granted at assignment. A client
	// extends it with authenticated ExecutionLeaseRenew messages.
	defaultLeaseTTL = protocol.DefaultLease
)

// Config defines local allocator authority and fixed class capacities.
type Config struct {
	// MaxRecords bounds the durable offer/execution/tombstone budget.
	MaxRecords int
	// Retention is the terminal-record retention the allocator applies to
	// every completed execution. It is operator configuration, never workload
	// input (F22-07): after the policy's result_retention was retired, a
	// workload cannot choose bookkeeping retention. Zero means
	// DefaultRetention; any value is bounded by CommandHorizon, which also
	// bounds replay-safe tombstones.
	Retention      time.Duration
	Identity       []byte
	Capacity       map[string]uint32
	OfferTTL       time.Duration
	LeaseTTL       time.Duration
	ReplayTTL      time.Duration
	ReplayCapacity int
	Now            func() time.Time
	NewID          func() string
	Store          StateStore
	Admission      AdmissionPolicy
	Logs           r1sruntime.LogStore
	// Tunnel is the allocator-local F14 direct-access tunnel authorization
	// surface. It is transport-neutral (opaque endpoint bytes, no network
	// library) and in-memory: grants are never persisted and a restart
	// invalidates them by construction.
	Tunnel TunnelConfig
	// Node is the allocator's bounded local capability metadata. When nil, the
	// allocator advertises no placement details and only matches empty
	// constraints (every node accepts every request); an explicit placement
	// constraint is then rejected so the client is never told a node matches
	// without evidence.
	Node *r1sv1.NodeCapabilities
}

// offerStatus is the lifecycle phase of one offer record: outstanding while
// it reserves capacity, then assigned, expired, or released.
type offerStatus uint8

const (
	offerOutstanding offerStatus = iota + 1
	offerAssigned
	offerExpired
	offerReleased
)

// offerRecord tracks one published offer together with the request it backs
// and the local reservation it holds.
type offerRecord struct {
	offer     *r1sv1.ExecutionOffer
	request   *r1sv1.ExecutionRequest
	client    []byte
	status    offerStatus
	execution string
	resources r1sruntime.Resources
}

// executionRecord tracks one locally running or terminal workload execution
// and its durable client-held lease.
type executionRecord struct {
	id            string
	offerID       string
	client        []byte
	resourceClass string
	request       *r1sv1.ExecutionRequest
	phase         r1sv1.ExecutionPhase
	detail        string
	exitCode      *int32
	occurredAt    time.Time
	startedAt     time.Time
	released      bool
	retainUntil   time.Time
	// leaseUntil is the durable client-held lease expiry. It is zero only for
	// terminal executions, whose lifetime no longer needs a lease.
	leaseUntil time.Time
	revision   uint64
	resources  r1sruntime.Resources
}

// OfferSnapshot is a read-only view of allocator offer state.
type OfferSnapshot struct {
	Offer       *r1sv1.ExecutionOffer
	Client      []byte
	Outstanding bool
	Assigned    bool
	Expired     bool
	Released    bool
	ExecutionID string
}

// ExecutionSnapshot is a read-only view of allocator execution state.
type ExecutionSnapshot struct {
	ExecutionID   string
	OfferID       string
	Client        []byte
	ResourceClass string
	Request       *r1sv1.ExecutionRequest
	State         *r1sv1.ExecutionState
}

// Package client implements transport-independent request, selection, and execution tracking.
package client

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
)

// Config defines local client authority. Since F22-07 the client is an
// ephemeral, in-memory run process: there is no persistent store, durable
// identity, or durable lease intent, so Config carries only the authenticated
// authority and its time/ID sources.
type Config struct {
	Identity []byte
	Now      func() time.Time
	NewID    func() string
}

// Allocator describes a discovered allocator and the route used to reach it.
// Node is the allocator's advertised capability metadata (nil when the source
// did not advertise any); it is advisory and revalidated by the allocator at
// assignment, so a stale client-side copy never grants placement that local
// policy denies.
type Allocator struct {
	Identity    []byte
	Destination string
	Hops        uint8
	Capacity    map[string]uint32
	Node        *r1sv1.NodeCapabilities
	// Tunnel endpoint advertisement (F21-02): the allocator's private tunnel
	// RNS Backbone/TCP listener as host:port plus the tunnel RNS destination
	// hash the client dials to open a tunnel. Populated from the authenticated
	// announce descriptor; advisory, like Node, and re-discovered on the next
	// announce. Holds empty values when the allocator runs no tunnel edge.
	TunnelHost        string
	TunnelPort        int
	TunnelDestination string
}

type offerRecord struct {
	offer       *r1sv1.ExecutionOffer
	allocatorID []byte
	receivedAt  time.Time
	release     *releaseIntent
}

type requestRecord struct {
	request     *r1sv1.ExecutionRequest
	messageID   string
	createdAt   time.Time
	offers      map[string]*offerRecord
	executionID string
}

type executionRecord struct {
	id                  string
	requestID           string
	offerID             string
	allocatorID         []byte
	destination         string
	assignmentMessageID string
	assignmentSentAt    time.Time
	inspectMessageID    string
	inspectSentAt       time.Time
	cancelMessageID     string
	cancelSentAt        time.Time
	cancelReason        string
	state               *r1sv1.ExecutionState
	// Lease intent: the client-held duty to keep this execution leased. The
	// public run controller replays this in-memory state on every tick.
	// leaseLost marks a lease that expired
	// before a renewal landed: the recorded workload must be re-requested.
	// leaseAllocators preserves the explicit allocator pinning recorded with
	// the intent so a re-request reuses the original placement constraint.
	leaseDuration       time.Duration
	leaseAllocators     []string
	leaseLost           bool
	leaseRenewedAt      time.Time
	leaseRenewMessageID string
	leaseExpiresAt      time.Time
}

// RequestSnapshot is a read-only view of a client request.
type RequestSnapshot struct {
	Request     *r1sv1.ExecutionRequest
	CreatedAt   time.Time
	OfferCount  int
	ExecutionID string
}

// ExecutionSnapshot is a read-only view of a client execution.
type ExecutionSnapshot struct {
	ExecutionID string
	RequestID   string
	OfferID     string
	Allocator   []byte
	Destination string
	State       *r1sv1.ExecutionState
}

// Client owns request selection and observed execution state in memory for the
// lifetime of one run process (F22). There is no durable client snapshot: a
// restart of the client never resumes a run, and the allocator retains the
// execution database.
type Client struct {
	logMessageID string
	logRequest   *r1sv1.ExecutionLogsRequest
	mu           sync.Mutex

	identity []byte
	now      func() time.Time
	newID    func() string

	allocators allocatorCatalog
	requests   map[string]*requestRecord
	executions map[string]*executionRecord
}

// New constructs client state. There is no restoration from durable storage.
func New(config Config) (*Client, error) {
	if len(config.Identity) == 0 {
		return nil, fmt.Errorf("%w: identity is required", ErrInvalidConfig)
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.NewID == nil {
		config.NewID = randomID
	}
	return &Client{
		identity: bytes.Clone(config.Identity), now: config.Now, newID: config.NewID,
		allocators: newAllocatorCatalog(), requests: make(map[string]*requestRecord), executions: make(map[string]*executionRecord),
	}, nil
}

func randomID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		panic(fmt.Sprintf("read crypto random ID: %v", err))
	}
	return hex.EncodeToString(value[:])
}

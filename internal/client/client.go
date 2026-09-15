// Package client implements transport-independent request, selection, and execution tracking.
package client

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
)

// StateStore atomically loads and saves one opaque client snapshot.
type StateStore interface {
	Load(context.Context) ([]byte, error)
	Save(context.Context, []byte) error
}

// Config defines local client authority and persistence.
type Config struct {
	Identity []byte
	Store    StateStore
	Now      func() time.Time
	NewID    func() string
}

// Allocator describes a discovered allocator and the route used to reach it.
type Allocator struct {
	Identity    []byte
	Destination string
	Hops        uint8
	Capacity    map[string]uint32
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

// Client owns durable request selection and observed execution state.
type Client struct {
	logMessageID string
	logRequest   *r1sv1.ExecutionLogsRequest
	mu           sync.Mutex

	identity []byte
	store    StateStore
	now      func() time.Time
	newID    func() string

	allocators allocatorCatalog
	requests   map[string]*requestRecord
	executions map[string]*executionRecord
}

// New restores or constructs client state.
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
	result := &Client{
		identity: bytes.Clone(config.Identity), store: config.Store, now: config.Now, newID: config.NewID,
		allocators: newAllocatorCatalog(), requests: make(map[string]*requestRecord), executions: make(map[string]*executionRecord),
	}
	result.mu.Lock()
	err := result.loadLocked(context.Background())
	result.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return result, nil
}

func randomID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		panic(fmt.Sprintf("read crypto random ID: %v", err))
	}
	return hex.EncodeToString(value[:])
}

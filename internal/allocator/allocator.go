// Package allocator implements transport-independent allocation and execution transitions.
package allocator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
	r1sruntime "github.com/mytecor/r1s/internal/runtime"
	"github.com/mytecor/r1s/internal/tunnel"
	"google.golang.org/protobuf/proto"
)

const (
	defaultOfferTTL       = 30 * time.Second
	defaultReplayTTL      = 10 * time.Minute
	defaultReplayCapacity = 4096
	// defaultLeaseTTL is the initial lease granted at assignment. A client
	// extends it with authenticated ExecutionLeaseRenew messages.
	defaultLeaseTTL = protocol.DefaultLease
	// maxLeaseTTL bounds a single renewal. It matches the command replay
	// horizon so a durable lease never outlives the tombstones that guard it.
	maxLeaseTTL = CommandHorizon
)

// Config defines local allocator authority and fixed class capacities.
type Config struct {
	MaxRecords     int
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

type offerStatus uint8

const (
	offerOutstanding offerStatus = iota + 1
	offerAssigned
	offerExpired
	offerReleased
)

type offerRecord struct {
	offer     *r1sv1.ExecutionOffer
	request   *r1sv1.ExecutionRequest
	client    []byte
	status    offerStatus
	execution string
	resources r1sruntime.Resources
}

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

// Allocator coordinates command handling around local policy, durable state,
// capacity accounting, replay protection, and the runtime boundary.
type Allocator struct {
	maxRecords int
	highWater  time.Time
	tombstones map[string]tombstone
	mu         sync.Mutex

	identity  []byte
	capacity  *capacityLedger
	offerTTL  time.Duration
	leaseTTL  time.Duration
	now       func() time.Time
	newID     func() string
	runtime   r1sruntime.Runtime
	store     StateStore
	admission AdmissionPolicy
	logs      r1sruntime.LogStore
	node      *r1sv1.NodeCapabilities

	offers     map[string]*offerRecord
	requests   map[string]string
	executions map[string]*executionRecord
	replay     replayCache

	tunnels      *tunnel.Registry
	tunnelConfig TunnelConfig
}

// New restores or constructs an allocator.
func New(config Config, runtime r1sruntime.Runtime) (*Allocator, error) {
	if len(config.Identity) == 0 {
		return nil, fmt.Errorf("%w: identity is required", ErrInvalidConfig)
	}
	if runtime == nil {
		return nil, fmt.Errorf("%w: runtime is required", ErrInvalidConfig)
	}
	capacity, err := newCapacityLedger(config.Capacity)
	if err != nil {
		return nil, err
	}
	if config.OfferTTL == 0 {
		config.OfferTTL = defaultOfferTTL
	}
	if config.ReplayTTL == 0 {
		config.ReplayTTL = defaultReplayTTL
	}
	if config.ReplayCapacity == 0 {
		config.ReplayCapacity = defaultReplayCapacity
	}
	if config.LeaseTTL == 0 {
		config.LeaseTTL = defaultLeaseTTL
	}
	if config.OfferTTL < 0 || config.LeaseTTL < 0 || config.ReplayTTL < 0 || config.ReplayCapacity < 0 {
		return nil, fmt.Errorf("%w: TTLs and replay capacity must be positive", ErrInvalidConfig)
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.NewID == nil {
		config.NewID = randomID
	}
	if err := config.Admission.validate(capacity.limits); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	if err := protocol.ValidateCapabilities(config.Node); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	// Copy the node advertisement so callers cannot mutate it concurrently.
	var node *r1sv1.NodeCapabilities
	if config.Node != nil {
		node = proto.Clone(config.Node).(*r1sv1.NodeCapabilities)
	}
	// Copy policy maps and slices so callers cannot mutate admission concurrently.
	policyData, _ := json.Marshal(config.Admission)
	var admission AdmissionPolicy
	_ = json.Unmarshal(policyData, &admission)
	if config.MaxRecords == 0 {
		config.MaxRecords = DefaultMaxRecords
	}
	if config.MaxRecords < 2 {
		return nil, ErrInvalidConfig
	}
	result := &Allocator{
		maxRecords: config.MaxRecords, tombstones: make(map[string]tombstone),
		identity:   bytes.Clone(config.Identity),
		capacity:   capacity,
		offerTTL:   config.OfferTTL,
		leaseTTL:   config.LeaseTTL,
		now:        config.Now,
		newID:      config.NewID,
		runtime:    runtime,
		store:      config.Store,
		admission:  admission,
		logs:       config.Logs,
		node:       node,
		offers:     make(map[string]*offerRecord),
		requests:   make(map[string]string),
		executions: make(map[string]*executionRecord),
		replay:     newReplayCache(config.ReplayTTL, config.ReplayCapacity),
		tunnelConfig: TunnelConfig{
			Enabled:       config.Tunnel.Enabled,
			DefaultTarget: config.Tunnel.DefaultTarget,
			GrantTTL:      config.Tunnel.GrantTTL,
			Endpoint:      config.Tunnel.Endpoint,
		},
	}
	// Copy the target map so callers cannot mutate tunnel targets concurrently.
	result.tunnelConfig.TargetByClass = make(map[string]tunnel.Target, len(config.Tunnel.TargetByClass))
	for class, target := range config.Tunnel.TargetByClass {
		result.tunnelConfig.TargetByClass[class] = target
	}
	result.tunnels, err = tunnel.NewRegistry(tunnel.RegistryConfig{NewID: config.NewID})
	if err != nil {
		return nil, err
	}
	result.mu.Lock()
	err = result.loadLocked(context.Background())
	result.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Handle validates an authenticated envelope and applies one allocator command.
// Duplicate message IDs from the same sender are acknowledged without mutation.
func (a *Allocator) Handle(ctx context.Context, envelope *r1sv1.Envelope) ([]*r1sv1.Envelope, error) {
	if err := protocol.ValidateEnvelope(envelope); err != nil {
		return nil, err
	}
	if err := a.checkFreshness(envelope); err != nil {
		return a.errorResponse(envelope, err), err
	}
	if envelope.GetExecutionLogsRequest() != nil {
		return a.handleLogs(ctx, envelope)
	}
	switch envelope.GetPayload().(type) {
	case *r1sv1.Envelope_ExecutionRequest, *r1sv1.Envelope_ExecutionAssign, *r1sv1.Envelope_ExecutionCancel, *r1sv1.Envelope_ExecutionInspect, *r1sv1.Envelope_ExecutionOfferRelease, *r1sv1.Envelope_ExecutionLeaseRenew, *r1sv1.Envelope_ExecutionTunnelGrant:
	default:
		return nil, ErrUnsupportedMessage
	}

	replayKey := authorityKey(envelope.GetSender(), envelope.GetMessageId())
	entry, duplicate, replayErr := a.beginReplay(replayKey, envelope, a.now().UTC())
	if replayErr != nil {
		return a.errorResponse(envelope, replayErr), replayErr
	}
	if duplicate {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-entry.done:
			return cloneEnvelopes(entry.responses), entry.err
		}
	}

	var responses []*r1sv1.Envelope
	var err error
	switch payload := envelope.GetPayload().(type) {
	case *r1sv1.Envelope_ExecutionRequest:
		responses, err = a.handleRequest(envelope, payload.ExecutionRequest)
	case *r1sv1.Envelope_ExecutionAssign:
		responses, err = a.handleAssign(ctx, envelope, payload.ExecutionAssign)
	case *r1sv1.Envelope_ExecutionCancel:
		responses, err = a.handleCancel(ctx, envelope, payload.ExecutionCancel)
	case *r1sv1.Envelope_ExecutionInspect:
		responses, err = a.handleInspect(envelope, payload.ExecutionInspect)
	case *r1sv1.Envelope_ExecutionOfferRelease:
		responses, err = a.handleOfferRelease(envelope, payload.ExecutionOfferRelease)
	case *r1sv1.Envelope_ExecutionLeaseRenew:
		responses, err = a.handleLeaseRenew(envelope, payload.ExecutionLeaseRenew)
	case *r1sv1.Envelope_ExecutionTunnelGrant:
		responses, err = a.handleTunnelGrant(envelope, payload.ExecutionTunnelGrant)
	default:
		panic("payload type checked above")
	}
	if err != nil && len(responses) == 0 {
		responses = a.errorResponse(envelope, err)
	}
	if persistErr := a.finishReplay(entry, responses, err); persistErr != nil {
		err = errors.Join(err, persistErr)
	}
	return responses, err
}

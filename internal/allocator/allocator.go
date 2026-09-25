package allocator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
	r1sruntime "github.com/mytecor/r1s/internal/runtime"
	"github.com/mytecor/r1s/internal/tunnel"
	"google.golang.org/protobuf/proto"
)

// maxLeaseTTL bounds a single renewal. It matches the command replay horizon
// so a durable lease never outlives the tombstones that guard it.
const maxLeaseTTL = CommandHorizon

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
//
// New is the allocator's composition root: it validates Config, builds the
// capacity ledger, replay cache, and tunnel registry, and loads durable state
// for the allocator's own identity. Command routing happens in Handle
// (dispatch.go), state transitions in the per-command files, and durable
// load/persist in state.go.
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
			Enabled:  config.Tunnel.Enabled,
			Endpoint: config.Tunnel.Endpoint,
		},
	}
	result.tunnels = tunnel.NewRegistry()
	result.mu.Lock()
	err = result.loadLocked(context.Background())
	result.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return result, nil
}

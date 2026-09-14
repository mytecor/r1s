// Package allocator implements transport-independent allocation and execution transitions.
package allocator

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
	r1sruntime "github.com/mytecor/r1s/internal/runtime"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultOfferTTL       = 30 * time.Second
	defaultReplayTTL      = 10 * time.Minute
	defaultReplayCapacity = 4096
)

// Config defines local allocator authority and fixed class capacities.
type Config struct {
	MaxRecords     int
	Identity       []byte
	Capacity       map[string]uint32
	OfferTTL       time.Duration
	ReplayTTL      time.Duration
	ReplayCapacity int
	Now            func() time.Time
	NewID          func() string
	Store          StateStore
	Admission      AdmissionPolicy
	Logs           r1sruntime.LogStore
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
	revision      uint64
	resources     r1sruntime.Resources
}

type replayEntry struct {
	seenAt    time.Time
	done      chan struct{}
	envelope  *r1sv1.Envelope
	responses []*r1sv1.Envelope
	err       error
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

// Allocator owns local offers, executions, replay state, and capacity accounting.
type Allocator struct {
	maxRecords int
	highWater  time.Time
	tombstones map[string]tombstone
	mu         sync.Mutex

	identity       []byte
	capacity       map[string]uint32
	used           map[string]uint32
	offerTTL       time.Duration
	replayTTL      time.Duration
	replayCapacity int
	now            func() time.Time
	newID          func() string
	runtime        r1sruntime.Runtime
	store          StateStore
	admission      AdmissionPolicy
	logs           r1sruntime.LogStore

	offers     map[string]*offerRecord
	requests   map[string]string
	executions map[string]*executionRecord
	replay     map[string]*replayEntry
}

// New constructs an allocator with no durable state.
func New(config Config, runtime r1sruntime.Runtime) (*Allocator, error) {
	if len(config.Identity) == 0 {
		return nil, fmt.Errorf("%w: identity is required", ErrInvalidConfig)
	}
	if runtime == nil {
		return nil, fmt.Errorf("%w: runtime is required", ErrInvalidConfig)
	}
	if len(config.Capacity) == 0 {
		return nil, fmt.Errorf("%w: capacity is required", ErrInvalidConfig)
	}
	capacity := make(map[string]uint32, len(config.Capacity))
	for class, slots := range config.Capacity {
		if strings.TrimSpace(class) == "" || slots == 0 {
			return nil, fmt.Errorf("%w: capacity classes and slots must be non-zero", ErrInvalidConfig)
		}
		capacity[class] = slots
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
	if config.OfferTTL < 0 || config.ReplayTTL < 0 || config.ReplayCapacity < 0 {
		return nil, fmt.Errorf("%w: TTLs and replay capacity must be positive", ErrInvalidConfig)
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.NewID == nil {
		config.NewID = randomID
	}

	if err := config.Admission.validate(capacity); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
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
		identity:       bytes.Clone(config.Identity),
		capacity:       capacity,
		used:           make(map[string]uint32, len(capacity)),
		offerTTL:       config.OfferTTL,
		replayTTL:      config.ReplayTTL,
		replayCapacity: config.ReplayCapacity,
		now:            config.Now,
		newID:          config.NewID,
		runtime:        runtime,
		store:          config.Store,
		admission:      admission,
		logs:           config.Logs,
		offers:         make(map[string]*offerRecord),
		requests:       make(map[string]string),
		executions:     make(map[string]*executionRecord),
		replay:         make(map[string]*replayEntry),
	}
	result.mu.Lock()
	err := result.loadLocked(context.Background())
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
	case *r1sv1.Envelope_ExecutionRequest, *r1sv1.Envelope_ExecutionAssign, *r1sv1.Envelope_ExecutionCancel, *r1sv1.Envelope_ExecutionInspect, *r1sv1.Envelope_ExecutionOfferRelease:
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

func (a *Allocator) handleInspect(envelope *r1sv1.Envelope, inspect *r1sv1.ExecutionInspect) ([]*r1sv1.Envelope, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	record, ok := a.executions[inspect.GetExecutionId()]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrExecutionNotFound, inspect.GetExecutionId())
	}
	if !bytes.Equal(record.client, envelope.GetSender()) {
		return nil, ErrUnauthorized
	}
	response, err := a.stateEnvelopeLocked(record, envelope.GetMessageId(), a.now().UTC())
	if err != nil {
		return nil, err
	}
	return []*r1sv1.Envelope{response}, nil
}

func (a *Allocator) handleRequest(envelope *r1sv1.Envelope, request *r1sv1.ExecutionRequest) ([]*r1sv1.Envelope, error) {
	now := a.now().UTC()
	a.mu.Lock()
	defer a.mu.Unlock()
	a.expireOffersLocked(now)

	requestKey := authorityKey(envelope.GetSender(), request.GetRequestId())
	if offerID, ok := a.requests[requestKey]; ok {
		record := a.offers[offerID]
		if record != nil {
			if record.status == offerExpired {
				return nil, ErrOfferExpired
			}
			if !proto.Equal(record.request, request) {
				return nil, fmt.Errorf("%w: request ID %q was reused with different content", ErrExecutionConflict, request.GetRequestId())
			}
			if record.status == offerReleased {
				return nil, ErrOfferReleased
			}
			response, err := a.offerEnvelopeLocked(record.offer, envelope.GetMessageId(), now)
			if err != nil {
				return nil, err
			}
			return []*r1sv1.Envelope{response}, nil
		}
	}

	for _, dead := range a.tombstones {
		if bytes.Equal(dead.Client, envelope.GetSender()) && dead.RequestID == request.GetRequestId() {
			return nil, ErrResultExpired
		}
	}
	if len(a.offers)*2+len(a.tombstones)+2 > a.maxRecords {
		return nil, ErrCapacityExhausted
	}
	if request.GetPolicy().GetResultRetention().AsDuration() > CommandHorizon {
		return nil, fmt.Errorf("%w: retention exceeds seven days", protocol.ErrInvalidEnvelope)
	}
	if err := a.admitLocked(envelope.GetSender(), false); err != nil {
		return nil, err
	}
	class := request.GetResourceClass()
	limit, ok := a.capacity[class]
	if !ok || a.used[class] >= limit {
		return nil, fmt.Errorf("%w: resource class %q", ErrCapacityExhausted, class)
	}
	offerID := a.newID()
	if offerID == "" {
		return nil, fmt.Errorf("%w: empty offer ID", ErrInvalidConfig)
	}
	if _, exists := a.offers[offerID]; exists {
		return nil, fmt.Errorf("%w: offer %q", ErrIDCollision, offerID)
	}
	offer := &r1sv1.ExecutionOffer{
		OfferId:       offerID,
		RequestId:     request.GetRequestId(),
		ResourceClass: class,
		ExpiresAt:     timestamppb.New(now.Add(a.offerTTL)),
	}
	response, err := a.offerEnvelopeLocked(offer, envelope.GetMessageId(), now)
	if err != nil {
		return nil, err
	}
	a.offers[offerID] = &offerRecord{
		offer:     proto.Clone(offer).(*r1sv1.ExecutionOffer),
		request:   proto.Clone(request).(*r1sv1.ExecutionRequest),
		client:    bytes.Clone(envelope.GetSender()),
		status:    offerOutstanding,
		resources: a.admission.Profiles[class],
	}
	a.requests[requestKey] = offerID
	a.used[class]++
	if err := a.persistLocked(context.Background()); err != nil {
		delete(a.requests, requestKey)
		delete(a.offers, offerID)
		a.releaseClassLocked(class)
		return nil, err
	}
	return []*r1sv1.Envelope{response}, nil
}

func (a *Allocator) handleAssign(ctx context.Context, envelope *r1sv1.Envelope, assign *r1sv1.ExecutionAssign) ([]*r1sv1.Envelope, error) {
	now := a.now().UTC()
	a.mu.Lock()
	a.expireOffersLocked(now)
	offer, ok := a.offers[assign.GetOfferId()]
	if !ok {
		a.mu.Unlock()
		return nil, fmt.Errorf("%w: %q", ErrOfferNotFound, assign.GetOfferId())
	}
	if !bytes.Equal(offer.client, envelope.GetSender()) {
		a.mu.Unlock()
		return nil, ErrUnauthorized
	}
	if offer.offer.GetRequestId() != assign.GetRequestId() {
		a.mu.Unlock()
		return nil, fmt.Errorf("%w: assignment request does not match offer", ErrExecutionConflict)
	}
	switch offer.status {
	case offerReleased:
		a.mu.Unlock()
		return nil, ErrOfferReleased
	case offerExpired:
		a.mu.Unlock()
		return nil, ErrOfferExpired
	case offerAssigned:
		if offer.execution != assign.GetExecutionId() {
			a.mu.Unlock()
			return nil, ErrOfferAlreadyAssigned
		}
		response, err := a.stateEnvelopeLocked(a.executions[offer.execution], envelope.GetMessageId(), now)
		a.mu.Unlock()
		if err != nil {
			return nil, err
		}
		return []*r1sv1.Envelope{response}, nil
	}
	if existing, exists := a.executions[assign.GetExecutionId()]; exists {
		a.mu.Unlock()
		if existing.offerID == offer.offer.GetOfferId() && bytes.Equal(existing.client, envelope.GetSender()) {
			return nil, nil
		}
		return nil, fmt.Errorf("%w: %q", ErrExecutionConflict, assign.GetExecutionId())
	}

	if err := a.admitLocked(envelope.GetSender(), true); err != nil {
		a.mu.Unlock()
		return nil, err
	}
	record := &executionRecord{
		id:            assign.GetExecutionId(),
		offerID:       offer.offer.GetOfferId(),
		client:        bytes.Clone(offer.client),
		resourceClass: offer.offer.GetResourceClass(),
		request:       proto.Clone(offer.request).(*r1sv1.ExecutionRequest),
		phase:         r1sv1.ExecutionPhase_EXECUTION_PHASE_STARTING,
		occurredAt:    now,
		revision:      1,
		resources:     offer.resources,
		startedAt:     now,
	}
	offer.status = offerAssigned
	offer.execution = record.id
	a.executions[record.id] = record
	if err := a.persistLocked(context.Background()); err != nil {
		delete(a.executions, record.id)
		offer.status = offerOutstanding
		offer.execution = ""
		a.mu.Unlock()
		return nil, err
	}
	startRequest := r1sruntime.StartRequest{
		ExecutionID: record.id,
		Client:      bytes.Clone(record.client),
		Workload:    proto.Clone(record.request.GetWorkload()).(*r1sv1.Workload),
		Policy:      proto.Clone(record.request.GetPolicy()).(*r1sv1.ExecutionPolicy),
		StartedAt:   record.startedAt,
		Resources:   record.resources,
	}
	a.mu.Unlock()

	reporter := func(completion r1sruntime.Completion) error {
		if completion.ExecutionID == "" {
			completion.ExecutionID = record.id
		}
		if completion.ExecutionID != record.id {
			return fmt.Errorf("%w: runtime reported %q for %q", ErrExecutionConflict, completion.ExecutionID, record.id)
		}
		return a.RuntimeCompleted(completion)
	}
	startErr := a.runtime.Start(ctx, startRequest, reporter)

	a.mu.Lock()
	current := a.executions[record.id]
	if startErr != nil && current.phase == r1sv1.ExecutionPhase_EXECUTION_PHASE_STARTING {
		a.finishLocked(current, r1sv1.ExecutionPhase_EXECUTION_PHASE_FAILED, startErr.Error(), nil, a.now().UTC())
	} else if startErr == nil && current.phase == r1sv1.ExecutionPhase_EXECUTION_PHASE_STARTING {
		current.phase = r1sv1.ExecutionPhase_EXECUTION_PHASE_RUNNING
		current.occurredAt = a.now().UTC()
		current.revision++
	}
	response, responseErr := a.stateEnvelopeLocked(current, envelope.GetMessageId(), a.now().UTC())
	persistErr := a.persistLocked(context.Background())
	a.mu.Unlock()
	if responseErr != nil {
		return nil, responseErr
	}
	if persistErr != nil {
		return []*r1sv1.Envelope{response}, errors.Join(startErr, persistErr)
	}
	if startErr != nil {
		return []*r1sv1.Envelope{response}, errors.Join(ErrRuntimeStart, startErr)
	}
	return []*r1sv1.Envelope{response}, nil
}

func (a *Allocator) handleCancel(ctx context.Context, envelope *r1sv1.Envelope, cancel *r1sv1.ExecutionCancel) ([]*r1sv1.Envelope, error) {
	now := a.now().UTC()
	a.mu.Lock()
	record, ok := a.executions[cancel.GetExecutionId()]
	if !ok {
		a.mu.Unlock()
		return nil, fmt.Errorf("%w: %q", ErrExecutionNotFound, cancel.GetExecutionId())
	}
	if !bytes.Equal(record.client, envelope.GetSender()) {
		a.mu.Unlock()
		return nil, ErrUnauthorized
	}
	if terminal(record.phase) || record.phase == r1sv1.ExecutionPhase_EXECUTION_PHASE_CANCELLING {
		response, err := a.stateEnvelopeLocked(record, envelope.GetMessageId(), now)
		a.mu.Unlock()
		if err != nil {
			return nil, err
		}
		return []*r1sv1.Envelope{response}, nil
	}
	previousPhase := record.phase
	previousDetail := record.detail
	previousTime := record.occurredAt
	previousRevision := record.revision
	record.phase = r1sv1.ExecutionPhase_EXECUTION_PHASE_CANCELLING
	record.detail = cancel.GetReason()
	record.occurredAt = now
	record.revision++
	if err := a.persistLocked(context.Background()); err != nil {
		record.phase = previousPhase
		record.detail = previousDetail
		record.occurredAt = previousTime
		record.revision = previousRevision
		a.mu.Unlock()
		return nil, err
	}
	a.mu.Unlock()

	stopErr := a.runtime.Stop(ctx, record.id)

	a.mu.Lock()
	current := a.executions[record.id]
	if stopErr != nil {
		if current.phase == r1sv1.ExecutionPhase_EXECUTION_PHASE_CANCELLING {
			current.phase = previousPhase
			current.detail = previousDetail
			current.occurredAt = a.now().UTC()
			current.revision++
		}
		persistErr := a.persistLocked(context.Background())
		a.mu.Unlock()
		return nil, errors.Join(ErrRuntimeStop, stopErr, persistErr)
	}
	if !terminal(current.phase) {
		a.finishLocked(current, r1sv1.ExecutionPhase_EXECUTION_PHASE_CANCELLED, cancel.GetReason(), nil, a.now().UTC())
	}
	response, responseErr := a.stateEnvelopeLocked(current, envelope.GetMessageId(), a.now().UTC())
	persistErr := a.persistLocked(context.Background())
	a.mu.Unlock()
	if responseErr != nil {
		return nil, responseErr
	}
	if persistErr != nil {
		return []*r1sv1.Envelope{response}, persistErr
	}
	return []*r1sv1.Envelope{response}, nil
}

// RuntimeCompleted applies an asynchronous completion or failure callback.
func (a *Allocator) RuntimeCompleted(completion r1sruntime.Completion) error {
	if completion.ExecutionID == "" {
		return fmt.Errorf("%w: execution ID is required", ErrInvalidTransition)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	record, ok := a.executions[completion.ExecutionID]
	if !ok {
		return fmt.Errorf("%w: %q", ErrExecutionNotFound, completion.ExecutionID)
	}
	phase := r1sv1.ExecutionPhase_EXECUTION_PHASE_COMPLETED
	detail := completion.Detail
	if completion.Err != nil {
		phase = r1sv1.ExecutionPhase_EXECUTION_PHASE_FAILED
		if detail == "" {
			detail = completion.Err.Error()
		}
	}
	if terminal(record.phase) {
		if record.phase == phase {
			return nil
		}
		return fmt.Errorf("%w: execution %q is already %s", ErrInvalidTransition, record.id, record.phase)
	}
	previous := *record
	used := a.used[record.resourceClass]
	a.finishLocked(record, phase, detail, completion.ExitCode, a.now().UTC())
	if err := a.persistLocked(context.Background()); err != nil {
		*record = previous
		a.used[record.resourceClass] = used
		return err
	}
	return nil
}

// Recover reconciles durable non-terminal executions with the runtime. It
// never creates a missing workload: missing or conflicting runtime metadata is
// recorded as a terminal failure, while running tasks regain completion and
// deadline monitoring.
func (a *Allocator) Recover(ctx context.Context) error {
	recoverer, ok := a.runtime.(r1sruntime.Recoverer)
	if !ok {
		a.mu.Lock()
		hasActive := false
		for _, record := range a.executions {
			hasActive = hasActive || !terminal(record.phase)
		}
		a.mu.Unlock()
		if hasActive {
			return ErrRecoveryUnsupported
		}
		return nil
	}

	a.mu.Lock()
	ids := make([]string, 0, len(a.executions))
	for id, record := range a.executions {
		if !terminal(record.phase) {
			ids = append(ids, id)
		}
	}
	a.mu.Unlock()
	sort.Strings(ids)

	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return err
		}
		a.mu.Lock()
		record := a.executions[id]
		if record == nil || terminal(record.phase) {
			a.mu.Unlock()
			continue
		}
		if record.phase == r1sv1.ExecutionPhase_EXECUTION_PHASE_CANCELLING {
			reason := record.detail
			a.mu.Unlock()
			if err := a.runtime.Stop(ctx, id); err != nil {
				return errors.Join(ErrRuntimeStop, err)
			}
			a.mu.Lock()
			if current := a.executions[id]; current != nil && !terminal(current.phase) {
				a.finishLocked(current, r1sv1.ExecutionPhase_EXECUTION_PHASE_CANCELLED, reason, nil, a.now().UTC())
			}
			err := a.persistLocked(context.Background())
			a.mu.Unlock()
			if err != nil {
				return err
			}
			continue
		}

		request := r1sruntime.StartRequest{
			ExecutionID: record.id,
			Client:      bytes.Clone(record.client),
			Workload:    proto.Clone(record.request.GetWorkload()).(*r1sv1.Workload),
			Policy:      proto.Clone(record.request.GetPolicy()).(*r1sv1.ExecutionPolicy),
			StartedAt:   record.startedAt,
			Resources:   record.resources,
		}
		a.mu.Unlock()
		reporter := func(completion r1sruntime.Completion) error {
			if completion.ExecutionID == "" {
				completion.ExecutionID = id
			}
			if completion.ExecutionID != id {
				return fmt.Errorf("%w: runtime reported %q for %q", ErrExecutionConflict, completion.ExecutionID, id)
			}
			return a.RuntimeCompleted(completion)
		}
		recoverErr := recoverer.Recover(ctx, request, reporter)

		a.mu.Lock()
		current := a.executions[id]
		if recoverErr == nil {
			if current != nil && current.phase == r1sv1.ExecutionPhase_EXECUTION_PHASE_STARTING {
				current.phase = r1sv1.ExecutionPhase_EXECUTION_PHASE_RUNNING
				current.occurredAt = a.now().UTC()
				current.revision++
			}
		} else if errors.Is(recoverErr, r1sruntime.ErrExecutionMissing) || errors.Is(recoverErr, r1sruntime.ErrExecutionConflict) {
			if current != nil && !terminal(current.phase) {
				a.finishLocked(current, r1sv1.ExecutionPhase_EXECUTION_PHASE_FAILED, recoverErr.Error(), nil, a.now().UTC())
			}
		} else {
			a.mu.Unlock()
			return errors.Join(ErrRuntimeStart, recoverErr)
		}
		persistErr := a.persistLocked(context.Background())
		a.mu.Unlock()
		if persistErr != nil {
			return persistErr
		}
	}
	return nil
}

// SweepExpired releases capacity held by offers whose TTL elapsed.
func (a *Allocator) SweepExpired() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	expired := a.expireOffersLocked(a.now().UTC())
	if expired > 0 {
		_ = a.persistLocked(context.Background())
	}
	return expired
}

// Available reports currently unreserved slots for a resource class.
func (a *Allocator) Available(class string) uint32 {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.expireOffersLocked(a.now().UTC())
	limit := a.capacity[class]
	if a.used[class] >= limit {
		return 0
	}
	return limit - a.used[class]
}

// Offer returns a cloned offer snapshot.
func (a *Allocator) Offer(id string) (OfferSnapshot, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.expireOffersLocked(a.now().UTC())
	record, ok := a.offers[id]
	if !ok {
		return OfferSnapshot{}, false
	}
	return OfferSnapshot{
		Offer:       proto.Clone(record.offer).(*r1sv1.ExecutionOffer),
		Client:      bytes.Clone(record.client),
		Outstanding: record.status == offerOutstanding,
		Assigned:    record.status == offerAssigned,
		Expired:     record.status == offerExpired,
		Released:    record.status == offerReleased,
		ExecutionID: record.execution,
	}, true
}

// Execution returns a cloned execution snapshot.
func (a *Allocator) Execution(id string) (ExecutionSnapshot, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	record, ok := a.executions[id]
	if !ok {
		return ExecutionSnapshot{}, false
	}
	return ExecutionSnapshot{
		ExecutionID:   record.id,
		OfferID:       record.offerID,
		Client:        bytes.Clone(record.client),
		ResourceClass: record.resourceClass,
		Request:       proto.Clone(record.request).(*r1sv1.ExecutionRequest),
		State:         stateFromRecord(record),
	}, true
}

func (a *Allocator) expireOffersLocked(now time.Time) int {
	expired := 0
	for _, offer := range a.offers {
		if offer.status == offerOutstanding && !offer.offer.GetExpiresAt().AsTime().After(now) {
			offer.status = offerExpired
			a.releaseClassLocked(offer.offer.GetResourceClass())
			expired++
		}
	}
	return expired
}

func (a *Allocator) beginReplay(key string, envelope *r1sv1.Envelope, now time.Time) (*replayEntry, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.expireOffersLocked(now)
	a.pruneReplayLocked(now)
	if entry, exists := a.replay[key]; exists {
		if !proto.Equal(entry.envelope, envelope) {
			return nil, false, ErrReplayConflict
		}
		return entry, true, nil
	}
	a.evictReplayLocked(a.replayCapacity - 1)
	if len(a.replay) >= a.replayCapacity {
		return nil, false, ErrReplayCapacity
	}
	entry := &replayEntry{seenAt: now, done: make(chan struct{}), envelope: proto.Clone(envelope).(*r1sv1.Envelope)}
	a.replay[key] = entry
	if err := a.persistLocked(context.Background()); err != nil {
		delete(a.replay, key)
		return nil, false, err
	}
	return entry, false, nil
}

func (a *Allocator) finishReplay(entry *replayEntry, responses []*r1sv1.Envelope, err error) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	entry.responses = cloneEnvelopes(responses)
	entry.err = err
	close(entry.done)
	a.evictReplayLocked(a.replayCapacity)
	return a.persistLocked(context.Background())
}

func (a *Allocator) pruneReplayLocked(now time.Time) {
	cutoff := now.Add(-a.replayTTL)
	for key, entry := range a.replay {
		if !entry.seenAt.After(cutoff) && replayDone(entry) {
			delete(a.replay, key)
		}
	}
}

func (a *Allocator) evictReplayLocked(limit int) {
	for len(a.replay) > limit {
		var oldestKey string
		var oldest time.Time
		for candidate, entry := range a.replay {
			if replayDone(entry) && (oldestKey == "" || entry.seenAt.Before(oldest)) {
				oldestKey, oldest = candidate, entry.seenAt
			}
		}
		if oldestKey == "" {
			return
		}
		delete(a.replay, oldestKey)
	}
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

func (a *Allocator) offerEnvelopeLocked(offer *r1sv1.ExecutionOffer, correlationID string, now time.Time) (*r1sv1.Envelope, error) {
	messageID := a.newID()
	if messageID == "" {
		return nil, fmt.Errorf("%w: empty message ID", ErrInvalidConfig)
	}
	return &r1sv1.Envelope{
		MessageId:     messageID,
		Sender:        bytes.Clone(a.identity),
		CorrelationId: correlationID,
		SentAt:        timestamppb.New(now),
		Payload: &r1sv1.Envelope_ExecutionOffer{
			ExecutionOffer: proto.Clone(offer).(*r1sv1.ExecutionOffer),
		},
	}, nil
}

func (a *Allocator) stateEnvelopeLocked(record *executionRecord, correlationID string, now time.Time) (*r1sv1.Envelope, error) {
	if record == nil {
		return nil, fmt.Errorf("%w: missing execution record", ErrExecutionNotFound)
	}
	messageID := a.newID()
	if messageID == "" {
		return nil, fmt.Errorf("%w: empty message ID", ErrInvalidConfig)
	}
	return &r1sv1.Envelope{
		MessageId:     messageID,
		Sender:        bytes.Clone(a.identity),
		CorrelationId: correlationID,
		SentAt:        timestamppb.New(now),
		Payload: &r1sv1.Envelope_ExecutionState{
			ExecutionState: stateFromRecord(record),
		},
	}, nil
}

func (a *Allocator) finishLocked(record *executionRecord, phase r1sv1.ExecutionPhase, detail string, exitCode *int32, now time.Time) {
	record.phase = phase
	record.detail = detail
	record.exitCode = cloneInt32(exitCode)
	record.occurredAt = now
	record.revision++
	end := now
	if a.highWater.After(end) {
		end = a.highWater
	}
	record.retainUntil = end.Add(retention(record.request.GetPolicy()))
	if !record.released {
		a.releaseClassLocked(record.resourceClass)
		record.released = true
	}
}

func (a *Allocator) releaseClassLocked(class string) {
	if a.used[class] > 0 {
		a.used[class]--
	}
}

func stateFromRecord(record *executionRecord) *r1sv1.ExecutionState {
	return &r1sv1.ExecutionState{
		ExecutionId: record.id,
		Phase:       record.phase,
		OccurredAt:  timestamppb.New(record.occurredAt),
		Detail:      record.detail,
		ExitCode:    cloneInt32(record.exitCode),
		Revision:    record.revision,
	}
}

func terminal(phase r1sv1.ExecutionPhase) bool {
	return phase == r1sv1.ExecutionPhase_EXECUTION_PHASE_CANCELLED ||
		phase == r1sv1.ExecutionPhase_EXECUTION_PHASE_COMPLETED ||
		phase == r1sv1.ExecutionPhase_EXECUTION_PHASE_FAILED
}

func authorityKey(identity []byte, id string) string {
	return string(identity) + "\x00" + id
}

func cloneInt32(value *int32) *int32 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func randomID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		panic(fmt.Sprintf("generate random ID: %v", err))
	}
	return hex.EncodeToString(value[:])
}

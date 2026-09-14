// Package client implements transport-independent request, selection, and execution tracking.
package client

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
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
	mu sync.Mutex

	identity []byte
	store    StateStore
	now      func() time.Time
	newID    func() string

	allocators map[string]Allocator
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
		allocators: make(map[string]Allocator), requests: make(map[string]*requestRecord), executions: make(map[string]*executionRecord),
	}
	result.mu.Lock()
	err := result.loadLocked(context.Background())
	result.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return result, nil
}

// RegisterAllocator records a verified discovery or authenticated session route.
func (o *Client) RegisterAllocator(candidate Allocator) error {
	if len(candidate.Identity) == 0 || strings.TrimSpace(candidate.Destination) == "" {
		return ErrInvalidAllocator
	}
	key := hex.EncodeToString(candidate.Identity)
	o.mu.Lock()
	defer o.mu.Unlock()
	previous, existed := o.allocators[key]
	if existed {
		if candidate.Capacity == nil {
			candidate.Capacity = previous.Capacity
			candidate.Hops = previous.Hops
		}
	}
	cloned := cloneAllocator(candidate)
	o.allocators[key] = cloned
	if err := o.persistLocked(context.Background()); err != nil {
		if existed {
			o.allocators[key] = previous
		} else {
			delete(o.allocators, key)
		}
		return err
	}
	return nil
}

// CreateRequest durably creates a request before it is sent to any allocator.
func (o *Client) CreateRequest(workload *r1sv1.Workload, policy *r1sv1.ExecutionPolicy, resourceClass string) (string, *r1sv1.Envelope, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	requestID := o.uniqueIDLocked(o.requests)
	messageID := o.newID()
	if requestID == "" || messageID == "" {
		return "", nil, fmt.Errorf("%w: ID generator returned an empty or duplicate ID", ErrInvalidConfig)
	}
	now := o.now().UTC()
	request := &r1sv1.ExecutionRequest{
		RequestId: requestID, Workload: cloneWorkload(workload), Policy: clonePolicy(policy), ResourceClass: resourceClass,
	}
	envelope := o.requestEnvelopeLocked(request, messageID, now)
	if err := protocol.ValidateEnvelope(envelope); err != nil {
		return "", nil, err
	}
	o.requests[requestID] = &requestRecord{request: request, messageID: messageID, createdAt: now, offers: make(map[string]*offerRecord)}
	if err := o.persistLocked(context.Background()); err != nil {
		delete(o.requests, requestID)
		return "", nil, err
	}
	return requestID, proto.Clone(envelope).(*r1sv1.Envelope), nil
}

// RequestEnvelope returns the stable request envelope used for all allocators.
func (o *Client) RequestEnvelope(requestID string) (*r1sv1.Envelope, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	record := o.requests[requestID]
	if record == nil {
		return nil, false
	}
	return o.requestEnvelopeLocked(record.request, record.messageID, record.createdAt), true
}

// Handle accepts an offer or state from an authenticated allocator.
func (o *Client) Handle(_ context.Context, envelope *r1sv1.Envelope) error {
	if err := protocol.ValidateEnvelope(envelope); err != nil {
		return err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	switch payload := envelope.GetPayload().(type) {
	case *r1sv1.Envelope_ExecutionOffer:
		return o.handleOfferLocked(envelope, payload.ExecutionOffer)
	case *r1sv1.Envelope_ExecutionState:
		return o.handleStateLocked(envelope, payload.ExecutionState)
	default:
		return ErrUnsupportedMessage
	}
}

func (o *Client) handleOfferLocked(envelope *r1sv1.Envelope, offer *r1sv1.ExecutionOffer) error {
	record := o.requests[offer.GetRequestId()]
	if record == nil {
		return fmt.Errorf("%w: %q", ErrRequestNotFound, offer.GetRequestId())
	}
	if record.messageID != envelope.GetCorrelationId() || offer.GetResourceClass() != record.request.GetResourceClass() {
		return ErrConflict
	}
	allocatorKey := hex.EncodeToString(envelope.GetSender())
	if _, ok := o.allocators[allocatorKey]; !ok {
		return ErrUnauthorized
	}
	if existing := record.offers[offer.GetOfferId()]; existing != nil {
		if !bytes.Equal(existing.allocatorID, envelope.GetSender()) || !proto.Equal(existing.offer, offer) {
			return ErrConflict
		}
		return nil
	}
	if record.executionID != "" {
		return nil
	}
	record.offers[offer.GetOfferId()] = &offerRecord{offer: proto.Clone(offer).(*r1sv1.ExecutionOffer), allocatorID: bytes.Clone(envelope.GetSender()), receivedAt: o.now().UTC()}
	if err := o.persistLocked(context.Background()); err != nil {
		delete(record.offers, offer.GetOfferId())
		return err
	}
	return nil
}

func (o *Client) handleStateLocked(envelope *r1sv1.Envelope, state *r1sv1.ExecutionState) error {
	record := o.executions[state.GetExecutionId()]
	if record == nil {
		return fmt.Errorf("%w: %q", ErrExecutionNotFound, state.GetExecutionId())
	}
	if !bytes.Equal(record.allocatorID, envelope.GetSender()) {
		return ErrUnauthorized
	}
	if record.state != nil {
		oldTime := record.state.GetOccurredAt().AsTime()
		newTime := state.GetOccurredAt().AsTime()
		if newTime.Before(oldTime) {
			return nil
		}
		if newTime.Equal(oldTime) {
			if proto.Equal(record.state, state) {
				return nil
			}
			return ErrConflict
		}
		if terminal(record.state.GetPhase()) && !proto.Equal(record.state, state) {
			return ErrConflict
		}
	}
	previous := record.state
	record.state = proto.Clone(state).(*r1sv1.ExecutionState)
	if err := o.persistLocked(context.Background()); err != nil {
		record.state = previous
		return err
	}
	return nil
}

// Select deterministically chooses one non-expired offer and durably records its assignment.
func (o *Client) Select(requestID string) (string, *r1sv1.Envelope, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	record := o.requests[requestID]
	if record == nil {
		return "", nil, fmt.Errorf("%w: %q", ErrRequestNotFound, requestID)
	}
	if record.executionID != "" {
		execution := o.executions[record.executionID]
		return execution.destination, o.assignmentEnvelopeLocked(execution), nil
	}
	now := o.now().UTC()
	candidates := make([]*offerRecord, 0, len(record.offers))
	for _, offer := range record.offers {
		allocator, ok := o.allocators[hex.EncodeToString(offer.allocatorID)]
		if ok && strings.TrimSpace(allocator.Destination) != "" && offer.offer.GetExpiresAt().AsTime().After(now) {
			candidates = append(candidates, offer)
		}
	}
	if len(candidates) == 0 {
		return "", nil, ErrNoOffer
	}
	sort.Slice(candidates, func(left, right int) bool {
		leftAllocator := o.allocators[hex.EncodeToString(candidates[left].allocatorID)]
		rightAllocator := o.allocators[hex.EncodeToString(candidates[right].allocatorID)]
		if leftAllocator.Hops != rightAllocator.Hops {
			return leftAllocator.Hops < rightAllocator.Hops
		}
		if compared := bytes.Compare(candidates[left].allocatorID, candidates[right].allocatorID); compared != 0 {
			return compared < 0
		}
		return candidates[left].offer.GetOfferId() < candidates[right].offer.GetOfferId()
	})
	selected := candidates[0]
	allocator := o.allocators[hex.EncodeToString(selected.allocatorID)]
	executionID := o.uniqueExecutionIDLocked()
	messageID := o.newID()
	if executionID == "" || messageID == "" {
		return "", nil, fmt.Errorf("%w: ID generator returned an empty or duplicate ID", ErrInvalidConfig)
	}
	execution := &executionRecord{
		id: executionID, requestID: requestID, offerID: selected.offer.GetOfferId(), allocatorID: bytes.Clone(selected.allocatorID),
		destination: allocator.Destination, assignmentMessageID: messageID, assignmentSentAt: now,
	}
	record.executionID = executionID
	o.executions[executionID] = execution
	if err := o.persistLocked(context.Background()); err != nil {
		record.executionID = ""
		delete(o.executions, executionID)
		return "", nil, err
	}
	return execution.destination, o.assignmentEnvelopeLocked(execution), nil
}

// Inspect creates a fresh query so allocator replay caching cannot return stale state.
func (o *Client) Inspect(executionID string) (string, *r1sv1.Envelope, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	record := o.executions[executionID]
	if record == nil {
		return "", nil, fmt.Errorf("%w: %q", ErrExecutionNotFound, executionID)
	}
	previous := record.inspectMessageID
	previousSentAt := record.inspectSentAt
	record.inspectMessageID = o.newID()
	record.inspectSentAt = o.now().UTC()
	if record.inspectMessageID == "" {
		record.inspectMessageID = previous
		record.inspectSentAt = previousSentAt
		return "", nil, fmt.Errorf("%w: ID generator returned an empty ID", ErrInvalidConfig)
	}
	if err := o.persistLocked(context.Background()); err != nil {
		record.inspectMessageID = previous
		record.inspectSentAt = previousSentAt
		return "", nil, err
	}
	return record.destination, &r1sv1.Envelope{
		MessageId: record.inspectMessageID, Sender: bytes.Clone(o.identity), SentAt: timestamppb.New(record.inspectSentAt),
		Payload: &r1sv1.Envelope_ExecutionInspect{ExecutionInspect: &r1sv1.ExecutionInspect{ExecutionId: executionID}},
	}, nil
}

// Cancel durably records a stable cancellation before it is sent.
func (o *Client) Cancel(executionID, reason string) (string, *r1sv1.Envelope, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	record := o.executions[executionID]
	if record == nil {
		return "", nil, fmt.Errorf("%w: %q", ErrExecutionNotFound, executionID)
	}
	if record.cancelMessageID == "" {
		record.cancelMessageID = o.newID()
		record.cancelSentAt = o.now().UTC()
		record.cancelReason = reason
		if record.cancelMessageID == "" {
			return "", nil, fmt.Errorf("%w: ID generator returned an empty ID", ErrInvalidConfig)
		}
		if err := o.persistLocked(context.Background()); err != nil {
			record.cancelMessageID = ""
			record.cancelSentAt = time.Time{}
			record.cancelReason = ""
			return "", nil, err
		}
	} else if record.cancelReason != reason {
		return "", nil, ErrConflict
	}
	return record.destination, &r1sv1.Envelope{
		MessageId: record.cancelMessageID, Sender: bytes.Clone(o.identity), SentAt: timestamppb.New(record.cancelSentAt),
		Payload: &r1sv1.Envelope_ExecutionCancel{ExecutionCancel: &r1sv1.ExecutionCancel{ExecutionId: executionID, Reason: reason}},
	}, nil
}

// Requests returns stable, sorted snapshots.
func (o *Client) Requests() []RequestSnapshot {
	o.mu.Lock()
	defer o.mu.Unlock()
	result := make([]RequestSnapshot, 0, len(o.requests))
	for _, record := range o.requests {
		result = append(result, RequestSnapshot{Request: proto.Clone(record.request).(*r1sv1.ExecutionRequest), CreatedAt: record.createdAt, OfferCount: len(record.offers), ExecutionID: record.executionID})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.Before(result[j].CreatedAt) })
	return result
}

// Execution returns a cloned execution snapshot.
func (o *Client) Execution(executionID string) (ExecutionSnapshot, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	record := o.executions[executionID]
	if record == nil {
		return ExecutionSnapshot{}, false
	}
	return snapshotExecution(record), true
}

// Executions returns stable, sorted execution snapshots.
func (o *Client) Executions() []ExecutionSnapshot {
	o.mu.Lock()
	defer o.mu.Unlock()
	result := make([]ExecutionSnapshot, 0, len(o.executions))
	for _, record := range o.executions {
		result = append(result, snapshotExecution(record))
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ExecutionID < result[j].ExecutionID })
	return result
}

func (o *Client) requestEnvelopeLocked(request *r1sv1.ExecutionRequest, messageID string, sentAt time.Time) *r1sv1.Envelope {
	return &r1sv1.Envelope{
		MessageId: messageID, Sender: bytes.Clone(o.identity), SentAt: timestamppb.New(sentAt),
		Payload: &r1sv1.Envelope_ExecutionRequest{ExecutionRequest: proto.Clone(request).(*r1sv1.ExecutionRequest)},
	}
}

func (o *Client) assignmentEnvelopeLocked(record *executionRecord) *r1sv1.Envelope {
	return &r1sv1.Envelope{
		MessageId: record.assignmentMessageID, Sender: bytes.Clone(o.identity), SentAt: timestamppb.New(record.assignmentSentAt),
		Payload: &r1sv1.Envelope_ExecutionAssign{ExecutionAssign: &r1sv1.ExecutionAssign{
			RequestId: record.requestID, OfferId: record.offerID, ExecutionId: record.id,
		}},
	}
}

func (o *Client) uniqueIDLocked(existing map[string]*requestRecord) string {
	value := o.newID()
	if value == "" || existing[value] != nil {
		return ""
	}
	return value
}

func (o *Client) uniqueExecutionIDLocked() string {
	value := o.newID()
	if value == "" || o.executions[value] != nil {
		return ""
	}
	return value
}

func snapshotExecution(record *executionRecord) ExecutionSnapshot {
	result := ExecutionSnapshot{
		ExecutionID: record.id, RequestID: record.requestID, OfferID: record.offerID,
		Allocator: bytes.Clone(record.allocatorID), Destination: record.destination,
	}
	if record.state != nil {
		result.State = proto.Clone(record.state).(*r1sv1.ExecutionState)
	}
	return result
}

func cloneAllocator(value Allocator) Allocator {
	result := Allocator{Identity: bytes.Clone(value.Identity), Destination: value.Destination, Hops: value.Hops}
	if value.Capacity != nil {
		result.Capacity = make(map[string]uint32, len(value.Capacity))
		for class, slots := range value.Capacity {
			result.Capacity[class] = slots
		}
	}
	return result
}

func cloneWorkload(value *r1sv1.Workload) *r1sv1.Workload {
	if value == nil {
		return nil
	}
	return proto.Clone(value).(*r1sv1.Workload)
}

func clonePolicy(value *r1sv1.ExecutionPolicy) *r1sv1.ExecutionPolicy {
	if value == nil {
		return nil
	}
	return proto.Clone(value).(*r1sv1.ExecutionPolicy)
}

func terminal(phase r1sv1.ExecutionPhase) bool {
	return phase == r1sv1.ExecutionPhase_EXECUTION_PHASE_CANCELLED || phase == r1sv1.ExecutionPhase_EXECUTION_PHASE_COMPLETED || phase == r1sv1.ExecutionPhase_EXECUTION_PHASE_FAILED
}

func randomID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		panic(fmt.Sprintf("read crypto random ID: %v", err))
	}
	return hex.EncodeToString(value[:])
}

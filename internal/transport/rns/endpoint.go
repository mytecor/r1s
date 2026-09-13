package rns

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
	coretransport "github.com/mytecor/r1s/internal/transport"
	"google.golang.org/protobuf/proto"
	"quad4/reticulum-go/pkg/channel"
	"quad4/reticulum-go/pkg/common"
	"quad4/reticulum-go/pkg/destination"
	"quad4/reticulum-go/pkg/identity"
	"quad4/reticulum-go/pkg/link"
)

const (
	defaultAppName          = "r1s"
	defaultAspect           = "allocator"
	defaultAnnounceInterval = 5 * time.Minute
	defaultNetworkWait      = 30 * time.Second
)

var (
	ErrInvalidConfig      = errors.New("invalid RNS transport configuration")
	ErrNotStarted         = errors.New("RNS endpoint is not started")
	ErrInvalidDestination = errors.New("invalid RNS destination")
)

// Config defines one embedded Reticulum endpoint.
type Config struct {
	Reticulum        *common.ReticulumConfig
	IdentityPath     string
	Capacity         map[string]uint32
	AppName          string
	Aspect           string
	AnnounceInterval time.Duration
	NetworkWait      time.Duration
}

// Service describes an allocator learned from an authenticated announce.
type Service struct {
	Destination string
	Identity    string
	Descriptor  Descriptor
	Hops        uint8
}

// Endpoint is one persistent RNS identity, destination, and set of authenticated links.
type Endpoint struct {
	mu          sync.Mutex
	stack       *stack
	identity    *identity.Identity
	destination *destination.Destination
	handler     coretransport.Handler
	interval    time.Duration
	networkWait time.Duration
	name        string

	started      bool
	closed       bool
	sessions     map[string]*session
	destinations map[string][]byte
	dials        map[string]*dialAttempt
	waiters      map[string][]chan struct{}
	discovered   chan Service
	stopAnnounce context.CancelFunc
}

type session struct {
	mu      sync.RWMutex
	link    *link.Link
	channel *channel.Channel
	sender  []byte
	pending [][]byte
}

type dialAttempt struct {
	done    chan struct{}
	session *session
	err     error
}

var _ coretransport.Endpoint = (*Endpoint)(nil)

// New constructs an endpoint without starting network interfaces.
func New(config Config, handler coretransport.Handler) (*Endpoint, error) {
	if config.Reticulum == nil || strings.TrimSpace(config.IdentityPath) == "" || handler == nil {
		return nil, fmt.Errorf("%w: Reticulum config, identity path, and handler are required", ErrInvalidConfig)
	}
	descriptor, err := newDescriptor(config.Capacity)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	descriptorData, err := descriptor.marshal()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	if config.AppName == "" {
		config.AppName = defaultAppName
	}
	if config.Aspect == "" {
		config.Aspect = defaultAspect
	}
	if config.AnnounceInterval == 0 {
		config.AnnounceInterval = defaultAnnounceInterval
	}
	if config.AnnounceInterval < 0 {
		return nil, fmt.Errorf("%w: announce interval must be positive", ErrInvalidConfig)
	}
	if config.NetworkWait == 0 {
		config.NetworkWait = defaultNetworkWait
	}
	if config.NetworkWait < 0 {
		return nil, fmt.Errorf("%w: network wait must be positive", ErrInvalidConfig)
	}

	localIdentity, err := loadOrCreateIdentity(config.IdentityPath)
	if err != nil {
		return nil, fmt.Errorf("load identity: %w", err)
	}
	rnsStack, err := newStack(config.Reticulum)
	if err != nil {
		return nil, fmt.Errorf("construct Reticulum stack: %w", err)
	}
	localDestination, err := destination.New(localIdentity, destination.In, destination.Single, config.AppName, rnsStack.transport, config.Aspect)
	if err != nil {
		return nil, fmt.Errorf("construct RNS destination: %w", err)
	}
	localDestination.AcceptsLinks(true)
	localDestination.SetDefaultAppData(descriptorData)

	endpoint := &Endpoint{
		stack: rnsStack, identity: localIdentity, destination: localDestination,
		handler: handler, interval: config.AnnounceInterval, networkWait: config.NetworkWait,
		name:     hex.EncodeToString(localIdentity.Hash()),
		sessions: make(map[string]*session), dials: make(map[string]*dialAttempt),
		destinations: make(map[string][]byte), waiters: make(map[string][]chan struct{}), discovered: make(chan Service, 32),
	}
	localDestination.SetLinkEstablishedCallback(endpoint.acceptLink)
	rnsStack.transport.RegisterAnnounceHandler(&announceHandler{endpoint: endpoint, aspect: config.AppName + "." + config.Aspect})
	return endpoint, nil
}

// Start starts Reticulum interfaces, publishes the service descriptor, and refreshes it periodically.
func (e *Endpoint) Start(ctx context.Context) error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return coretransport.ErrEndpointClosed
	}
	if e.started {
		e.mu.Unlock()
		return nil
	}
	e.started = true
	e.mu.Unlock()
	if err := e.stack.Start(); err != nil {
		e.mu.Lock()
		e.started = false
		e.mu.Unlock()
		return fmt.Errorf("start Reticulum stack: %w", err)
	}
	if err := e.destination.Announce(false, nil, nil); err != nil {
		_ = e.stack.Close()
		e.mu.Lock()
		e.started = false
		e.mu.Unlock()
		return fmt.Errorf("announce r1s service: %w", err)
	}
	announceContext, cancel := context.WithCancel(ctx)
	e.mu.Lock()
	e.stopAnnounce = cancel
	e.mu.Unlock()
	go e.announceLoop(announceContext)
	return nil
}

// Name is the hex-encoded hash of the persistent RNS identity.
func (e *Endpoint) Name() string { return e.name }

// Destination is the hex-encoded destination hash owners use for their first connection.
func (e *Endpoint) Destination() string { return hex.EncodeToString(e.destination.GetHash()) }

// Discoveries reports validated allocator announces. Delivery is best-effort and bounded.
func (e *Endpoint) Discoveries() <-chan Service { return e.discovered }

// Send serializes an envelope onto an RNS Channel.
func (e *Endpoint) Send(ctx context.Context, target string, envelope *r1sv1.Envelope) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if envelope == nil {
		return fmt.Errorf("%w: envelope is required", coretransport.ErrInvalidEndpoint)
	}
	destinationHash, key, err := parseDestination(target)
	if err != nil {
		return err
	}

	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return coretransport.ErrEndpointClosed
	}
	if !e.started {
		e.mu.Unlock()
		return ErrNotStarted
	}
	active := e.sessions[key]
	if mapped := e.destinations[key]; len(mapped) == 16 {
		destinationHash = bytes.Clone(mapped)
		key = hex.EncodeToString(destinationHash)
	}
	e.mu.Unlock()
	if active == nil || active.link.GetStatus() != link.StatusActive {
		active, err = e.connect(ctx, destinationHash, key)
		if err != nil {
			return err
		}
	}

	cloned := proto.Clone(envelope).(*r1sv1.Envelope)
	cloned.Sender = bytes.Clone(e.identity.Hash())
	data, err := proto.Marshal(cloned)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}
	if len(data) > active.channel.MDU() {
		return fmt.Errorf("marshal envelope: %d bytes exceed RNS Channel MDU %d", len(data), active.channel.MDU())
	}
	if err := active.channel.Send(&envelopeMessage{data: data}); err != nil {
		return fmt.Errorf("send RNS Channel message: %w", err)
	}
	return nil
}

func (e *Endpoint) connect(ctx context.Context, destinationHash []byte, key string) (*session, error) {
	e.mu.Lock()
	if active := e.sessions[key]; active != nil && active.link.GetStatus() == link.StatusActive {
		e.mu.Unlock()
		return active, nil
	}
	if attempt := e.dials[key]; attempt != nil {
		e.mu.Unlock()
		select {
		case <-attempt.done:
			return attempt.session, attempt.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	attempt := &dialAttempt{done: make(chan struct{})}
	e.dials[key] = attempt
	e.mu.Unlock()

	attempt.session, attempt.err = e.dial(ctx, destinationHash)
	e.mu.Lock()
	delete(e.dials, key)
	close(attempt.done)
	e.mu.Unlock()
	return attempt.session, attempt.err
}

func (e *Endpoint) dial(ctx context.Context, destinationHash []byte) (*session, error) {
	if err := e.awaitPath(ctx, destinationHash); err != nil {
		return nil, fmt.Errorf("discover RNS path: %w", err)
	}
	remoteIdentity, err := identity.Recall(destinationHash)
	if err != nil {
		return nil, fmt.Errorf("recall announced identity: %w", err)
	}
	outbound, err := destination.FromHash(destinationHash, remoteIdentity, destination.Single, e.stack.transport)
	if err != nil {
		return nil, fmt.Errorf("construct outbound destination: %w", err)
	}
	established := make(chan *session, 1)
	failed := make(chan struct{}, 1)
	var outboundLink *link.Link
	outboundLink = link.NewLink(outbound, e.stack.transport, nil, func(value *link.Link) {
		active, setupErr := e.newSession(value, remoteIdentity.Hash(), destinationHash)
		if setupErr == nil {
			setupErr = value.Identify(e.identity)
		}
		if setupErr != nil {
			select {
			case failed <- struct{}{}:
			default:
			}
			return
		}
		select {
		case established <- active:
		default:
		}
	}, func(*link.Link) {
		select {
		case failed <- struct{}{}:
		default:
		}
	})
	if err := outboundLink.Establish(); err != nil {
		return nil, fmt.Errorf("establish RNS link: %w", err)
	}
	wait, cancel := boundedContext(ctx, e.networkWait)
	defer cancel()
	select {
	case active := <-established:
		return active, nil
	case <-failed:
		return nil, errors.New("RNS link closed before establishment")
	case <-wait.Done():
		outboundLink.Teardown()
		return nil, wait.Err()
	}
}

func (e *Endpoint) awaitPath(ctx context.Context, destinationHash []byte) error {
	if e.stack.transport.HasPath(destinationHash) {
		return nil
	}
	key := hex.EncodeToString(destinationHash)
	waiter := make(chan struct{})
	e.mu.Lock()
	e.waiters[key] = append(e.waiters[key], waiter)
	e.mu.Unlock()
	defer e.removeWaiter(key, waiter)
	if e.stack.transport.HasPath(destinationHash) {
		return nil
	}
	if err := e.stack.transport.RequestPath(destinationHash, "", nil, false); err != nil {
		return err
	}
	wait, cancel := boundedContext(ctx, e.networkWait)
	defer cancel()
	select {
	case <-waiter:
		if e.stack.transport.HasPath(destinationHash) {
			return nil
		}
		return errors.New("RNS announce did not install a path")
	case <-wait.Done():
		return wait.Err()
	}
}

func (e *Endpoint) acceptLink(value any) {
	inbound, ok := value.(*link.Link)
	if !ok || inbound == nil {
		return
	}
	active, err := e.newSession(inbound, nil, nil)
	if err != nil {
		inbound.Teardown()
		return
	}
	authenticate := func(remote *identity.Identity) {
		if remote == nil {
			return
		}
		active.mu.Lock()
		active.sender = bytes.Clone(remote.Hash())
		pending := active.pending
		active.pending = nil
		active.mu.Unlock()
		e.cacheSession(active, remote.Hash(), nil)
		for _, data := range pending {
			go e.deliver(active, data)
		}
	}
	inbound.SetRemoteIdentifiedCallback(func(_ *link.Link, remote *identity.Identity) { authenticate(remote) })
	authenticate(inbound.GetRemoteIdentity())
}

func (e *Endpoint) newSession(rnsLink *link.Link, sender, destinationHash []byte) (*session, error) {
	rnsChannel := rnsLink.GetChannel()
	if err := rnsChannel.RegisterMessageType(envelopeMessageType, func() channel.MessageBase { return &envelopeMessage{} }); err != nil {
		return nil, err
	}
	active := &session{
		link: rnsLink, channel: rnsChannel,
		sender: bytes.Clone(sender),
	}
	rnsChannel.AddMessageHandler(func(message channel.MessageBase) bool {
		wire, ok := message.(*envelopeMessage)
		if !ok {
			return false
		}
		go e.deliver(active, wire.data)
		return true
	})
	e.cacheSession(active, sender, destinationHash)
	return active, nil
}

func (e *Endpoint) deliver(active *session, data []byte) {
	active.mu.Lock()
	sender := bytes.Clone(active.sender)
	if len(sender) == 0 {
		if len(active.pending) < 8 {
			active.pending = append(active.pending, bytes.Clone(data))
		}
		active.mu.Unlock()
		return
	}
	active.mu.Unlock()
	var envelope r1sv1.Envelope
	if err := proto.Unmarshal(data, &envelope); err != nil {
		return
	}
	envelope.Sender = sender
	if err := protocol.ValidateEnvelope(&envelope); err != nil {
		return
	}
	_ = e.handler(context.Background(), &envelope)
}

func (e *Endpoint) cacheSession(active *session, identities ...[]byte) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, value := range identities {
		if len(value) == 16 {
			e.sessions[hex.EncodeToString(value)] = active
		}
	}
}

func (e *Endpoint) removeWaiter(key string, waiter chan struct{}) {
	e.mu.Lock()
	defer e.mu.Unlock()
	entries := e.waiters[key]
	for index, entry := range entries {
		if entry == waiter {
			entries = append(entries[:index], entries[index+1:]...)
			break
		}
	}
	if len(entries) == 0 {
		delete(e.waiters, key)
	} else {
		e.waiters[key] = entries
	}
}

func (e *Endpoint) announceLoop(ctx context.Context) {
	ticker := time.NewTicker(e.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = e.destination.Announce(false, nil, nil)
		}
	}
}

// Close shuts down links and the embedded Reticulum node. It is idempotent.
func (e *Endpoint) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	if e.stopAnnounce != nil {
		e.stopAnnounce()
	}
	seen := make(map[*session]struct{}, len(e.sessions))
	for _, active := range e.sessions {
		seen[active] = struct{}{}
	}
	e.mu.Unlock()
	for active := range seen {
		active.link.Teardown()
	}
	return e.stack.Close()
}

type announceHandler struct {
	endpoint *Endpoint
	aspect   string
}

func (h *announceHandler) AspectFilter() []string     { return []string{h.aspect} }
func (h *announceHandler) ReceivePathResponses() bool { return true }
func (h *announceHandler) ReceivedAnnounce(destinationHash []byte, announced any, appData []byte, hops uint8) error {
	key := hex.EncodeToString(destinationHash)
	h.endpoint.mu.Lock()
	waiters := h.endpoint.waiters[key]
	delete(h.endpoint.waiters, key)
	h.endpoint.mu.Unlock()
	for _, waiter := range waiters {
		close(waiter)
	}
	descriptor, err := parseDescriptor(appData)
	if err != nil {
		return nil
	}
	announcedIdentity, ok := announced.(*identity.Identity)
	if !ok || announcedIdentity == nil {
		return nil
	}
	service := Service{
		Destination: key,
		Identity:    hex.EncodeToString(announcedIdentity.Hash()),
		Descriptor:  descriptor,
		Hops:        hops,
	}
	h.endpoint.mu.Lock()
	h.endpoint.destinations[service.Identity] = bytes.Clone(destinationHash)
	h.endpoint.mu.Unlock()
	select {
	case h.endpoint.discovered <- service:
	default:
	}
	return nil
}

func parseDestination(value string) ([]byte, string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != 16 {
		return nil, "", fmt.Errorf("%w: expected a 32-character hex hash", ErrInvalidDestination)
	}
	return decoded, value, nil
}

func boundedContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, timeout)
}

func loadOrCreateIdentity(path string) (*identity.Identity, error) {
	loaded, err := identity.FromFile(path)
	if err == nil {
		return loaded, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	created, err := identity.New()
	if err != nil {
		return nil, err
	}
	if err := created.ToFile(path); err != nil {
		return nil, err
	}
	return created, nil
}

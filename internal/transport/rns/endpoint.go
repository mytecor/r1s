package rns

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Quad4-Software/Reticulum-Go/pkg/common"
	"github.com/Quad4-Software/Reticulum-Go/pkg/destination"
	"github.com/Quad4-Software/Reticulum-Go/pkg/identity"
	"github.com/Quad4-Software/Reticulum-Go/pkg/interfaces"
	"github.com/Quad4-Software/Reticulum-Go/pkg/link"
	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/cluster"
	coretransport "github.com/mytecor/r1s/internal/transport"
	"google.golang.org/protobuf/proto"
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
	Reticulum *common.ReticulumConfig
	// IdentitySource is an existing or new identity file path, or a private
	// RNS identity encoded in hex, Base32, or URL-safe Base64.
	IdentitySource string
	// ClusterKey is the shared 256-bit membership secret. It is used only for
	// local ID derivation and link challenge-response, and is never announced.
	ClusterKey []byte
	// Capacity advertises this endpoint as an allocator. An empty map creates a
	// passive client endpoint that discovers allocators but does not announce one.
	Capacity         map[string]uint32
	AppName          string
	Aspect           string
	AnnounceInterval time.Duration
	NetworkWait      time.Duration
	// Interfaces, when non-empty, replaces config-driven interface
	// construction. Tests use this to inject wrapped interfaces (for example
	// packet-loss capture) while keeping the transport machinery intact.
	Interfaces []interfaces.Interface
}

// Service describes an allocator learned from an authenticated announce.
type Service struct {
	Destination string
	Identity    string
	Descriptor  Descriptor
	Hops        uint8
}

// Endpoint is the public transport facade. Link establishment and Channel
// delivery live behind it so callers only see discovery and envelope exchange.
type Endpoint struct {
	mu          sync.Mutex
	stack       *stack
	identity    *identity.Identity
	destination *destination.Destination
	handler     coretransport.Handler
	interval    time.Duration
	networkWait time.Duration
	name        string
	advertises  bool
	clusterKey  []byte
	clusterID   []byte

	started      bool
	closed       bool
	connections  *connectionRegistry
	discovered   chan Service
	stopAnnounce context.CancelFunc
}

var _ coretransport.Endpoint = (*Endpoint)(nil)

// New constructs an endpoint without starting network interfaces.
func New(config Config, handler coretransport.Handler) (*Endpoint, error) {
	if config.Reticulum == nil || strings.TrimSpace(config.IdentitySource) == "" || handler == nil {
		return nil, fmt.Errorf("%w: Reticulum config, identity source, and handler are required", ErrInvalidConfig)
	}
	clusterID, err := cluster.ID(config.ClusterKey)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	var descriptorData []byte
	if len(config.Capacity) > 0 {
		descriptor, err := newDescriptor(clusterID, config.Capacity)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
		}
		descriptorData, err = descriptor.marshal()
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
		}
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

	localIdentity, err := loadOrCreateIdentity(config.IdentitySource)
	if err != nil {
		return nil, fmt.Errorf("load identity: %w", err)
	}
	rnsStack, err := newStack(config.Reticulum, config.Interfaces...)
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
		name:       hex.EncodeToString(localIdentity.Hash()),
		clusterKey: bytes.Clone(config.ClusterKey), clusterID: clusterID,
		advertises:  len(descriptorData) > 0,
		connections: newConnectionRegistry(), discovered: make(chan Service, 32),
	}
	localDestination.SetLinkEstablishedCallback(endpoint.acceptLink)
	rnsStack.transport.RegisterAnnounceHandler(&announceHandler{endpoint: endpoint, aspect: config.AppName + "." + config.Aspect})
	return endpoint, nil
}

// Name is the hex-encoded hash of the persistent RNS identity.
func (e *Endpoint) Name() string { return e.name }

// Destination is the hex-encoded destination hash clients use for their first connection.
func (e *Endpoint) Destination() string { return hex.EncodeToString(e.destination.GetHash()) }

// Discoveries reports validated allocator announces. Delivery is best-effort and bounded.
func (e *Endpoint) Discoveries() <-chan Service { return e.discovered }

// DestinationForIdentity returns the last authenticated allocator destination
// learned through an announce or an outbound session.
func (e *Endpoint) DestinationForIdentity(identityHash string) (string, bool) {
	key := strings.ToLower(strings.TrimSpace(identityHash))
	destinationHash, ok := e.connections.destination(key)
	if !ok {
		return "", false
	}
	return hex.EncodeToString(destinationHash), true
}

// Send serializes an envelope onto an authenticated RNS Channel.
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
	e.mu.Unlock()
	destinationHash, key, active := e.connections.resolve(destinationHash, key)
	if active == nil || active.link.GetStatus() != link.StatusActive {
		active, err = e.connect(ctx, destinationHash, key)
		if err != nil {
			return err
		}
	}
	if err := e.waitAuthenticated(ctx, active); err != nil {
		return fmt.Errorf("authenticate RNS session: %w", err)
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
	if err := e.sendChannel(ctx, active, &envelopeMessage{data: data}); err != nil {
		return fmt.Errorf("send RNS Channel message: %w", err)
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

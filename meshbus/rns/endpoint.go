// Package rns implements a reusable, authenticated Reticulum adapter for
// meshbus. It owns RNS identity handling, destinations, announces, links,
// mutual realm authentication, Channels, authenticated direct-message
// delivery, connection/session reuse, peer route lookup, bounded
// pre-authentication buffering, and generic peer discovery integration.
//
// The adapter is application-agnostic: it exposes opaque direct delivery and
// reports advisory discovery separately from completed realm authentication.
// Node owns authoritative peer state and pub/sub fan-out. Announces always use
// the bounded meshbus.v1 presence format.
package rns

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Quad4-Software/Reticulum-Go/pkg/common"
	"github.com/Quad4-Software/Reticulum-Go/pkg/destination"
	"github.com/Quad4-Software/Reticulum-Go/pkg/identity"
	"github.com/Quad4-Software/Reticulum-Go/pkg/interfaces"
	"github.com/Quad4-Software/Reticulum-Go/pkg/link"
	"github.com/mytecor/r1s/meshbus"
	"github.com/mytecor/r1s/meshbus/realm"
)

const (
	defaultRealmAppName  = "meshbus"
	defaultRealmAspect   = "peer"
	defaultAnnounce      = 5 * time.Minute
	defaultNetworkWait   = 30 * time.Second
	defaultDirectorySize = 1024
)

// Config defines one generic Reticulum adapter endpoint. Production endpoints
// leave Reticulum nil and require the platform-default shared instance. A
// non-nil Reticulum config is reserved for deterministic and live standalone
// test harnesses.
type Config struct {
	Reticulum *common.ReticulumConfig
	// connectShared is a test-only override for attaching production-mode
	// endpoints to an isolated shared-instance listener.
	connectShared sharedConnector
	// IdentitySource is an existing or new identity file path, or a private
	// RNS identity encoded in hex, Base32, or URL-safe Base64.
	IdentitySource string
	// EphemeralIdentity creates a fresh identity in memory. It is intended for
	// one run-oriented client process and is mutually exclusive with
	// IdentitySource; the private identity is never written to disk.
	EphemeralIdentity bool
	// RealmKey is the shared 256-bit membership secret. It is used only for
	// realm ID derivation and link challenge-response, and is never announced.
	RealmKey         []byte
	AppName          string
	Aspect           string
	AnnounceInterval time.Duration
	NetworkWait      time.Duration
	// Interfaces, when non-empty, replaces config-driven interface
	// construction. Tests use this to inject wrapped interfaces (for example
	// packet-loss capture) while keeping the transport machinery intact.
	Interfaces []interfaces.Interface
	// PresenceMetadata is bounded advisory data carried by meshbus.v1 presence.
	PresenceMetadata map[string]string
	// Passive suppresses local announces while retaining peer discovery.
	Passive bool
	// DirectoryConfig bounds the peer directory. Zero uses the package default.
	DirectoryConfig DirectoryConfig
	// Handler receives authenticated direct messages as meshbus primitives.
	Handler meshbus.Handler
}

// DirectoryConfig bounds the adapter's peer directory.
type DirectoryConfig struct {
	MaxPeers         int
	MaxMetadataBytes int
}

// Endpoint is the reusable meshbus transport facade over authenticated RNS
// direct messages. Link establishment, realm authentication, Channel delivery,
// discovery and session reuse remain behind it.
type Endpoint struct {
	mu          sync.Mutex
	stack       *stack
	identity    *identity.Identity
	destination *destination.Destination
	handler     meshbus.Handler
	interval    time.Duration
	networkWait time.Duration
	name        string
	advertises  bool
	realm       *realm.Realm
	realmID     []byte
	codec       genericPresenceCodec
	directory   *meshbus.PeerDirectory
	observer    meshbus.PeerObserver
	runContext  context.Context

	started      bool
	closed       bool
	connections  *connectionRegistry
	stopAnnounce context.CancelFunc
}

// New constructs an adapter endpoint without starting network interfaces. It
// validates realm membership and bounded presence metadata before returning.
func New(config Config) (*Endpoint, error) {
	if config.Handler == nil {
		return nil, fmt.Errorf("%w: handler is required", ErrInvalidConfig)
	}
	if config.EphemeralIdentity == (strings.TrimSpace(config.IdentitySource) != "") {
		return nil, fmt.Errorf("%w: exactly one of identity source or ephemeral identity is required", ErrInvalidConfig)
	}
	openedRealm, err := realm.Open(realm.Config{Key: config.RealmKey})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	realmID := openedRealm.ID()

	codec := genericPresenceCodec{metadata: config.PresenceMetadata, passive: config.Passive}
	if config.AppName == "" {
		config.AppName = defaultRealmAppName
	}
	if config.Aspect == "" {
		config.Aspect = defaultRealmAspect
	}
	if config.AnnounceInterval == 0 {
		config.AnnounceInterval = defaultAnnounce
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

	var localIdentity *identity.Identity
	var loadErr error
	if config.EphemeralIdentity {
		localIdentity, loadErr = identity.New()
		if loadErr != nil {
			return nil, fmt.Errorf("generate ephemeral identity: %w", loadErr)
		}
	} else {
		localIdentity, loadErr = loadOrCreateIdentity(config.IdentitySource)
		if loadErr != nil {
			return nil, fmt.Errorf("load identity: %w", loadErr)
		}
	}
	rnsStack, err := newStack(config.Reticulum, config.Interfaces...)
	if err != nil {
		return nil, fmt.Errorf("construct Reticulum stack: %w", err)
	}
	if config.connectShared != nil {
		if !rnsStack.required {
			return nil, fmt.Errorf("%w: shared-instance connector cannot be combined with a standalone Reticulum config", ErrInvalidConfig)
		}
		rnsStack.connect = config.connectShared
	}
	localDestination, err := destination.New(localIdentity, destination.In, destination.Single, config.AppName, rnsStack.transport, config.Aspect)
	if err != nil {
		return nil, fmt.Errorf("construct RNS destination: %w", err)
	}
	localDestination.AcceptsLinks(true)

	// Build announcement presence once. Passive endpoints produce no app_data
	// and do not advertise.
	advertiseData, err := codec.Build(realmID, localIdentity.Hash())
	if err != nil {
		return nil, fmt.Errorf("%w: build presence: %v", ErrInvalidConfig, err)
	}
	localDestination.SetDefaultAppData(advertiseData)

	endpoint := &Endpoint{
		stack: rnsStack, identity: localIdentity, destination: localDestination,
		handler: config.Handler, interval: config.AnnounceInterval, networkWait: config.NetworkWait,
		name: hex.EncodeToString(localIdentity.Hash()), realm: openedRealm, realmID: realmID,
		codec: codec, advertises: len(advertiseData) > 0,
		connections: newConnectionRegistry(),
		directory: meshbus.NewPeerDirectory(meshbus.DirectoryConfig{
			MaxPeers:         config.DirectoryConfig.MaxPeers,
			MaxMetadataBytes: config.DirectoryConfig.MaxMetadataBytes,
		}),
	}
	localDestination.SetLinkEstablishedCallback(endpoint.acceptLink)
	aspect := config.AppName + "." + config.Aspect
	rnsStack.transport.RegisterAnnounceHandler(&announceHandler{endpoint: endpoint, aspect: aspect})
	return endpoint, nil
}

// Name is the hex-encoded hash of the RNS identity.
func (e *Endpoint) Name() string { return e.name }

// Destination is the hex-encoded destination hash clients use for their first connection.
func (e *Endpoint) Destination() string { return hex.EncodeToString(e.destination.GetHash()) }

// Identity returns the local transport-authenticated peer identity.
func (e *Endpoint) Identity() meshbus.PeerID {
	peer, _ := meshbus.NewPeerID(e.identity.Hash())
	return peer
}

// RealmID returns a copy of the adapter's public realm identifier.
func (e *Endpoint) RealmID() []byte { return bytes.Clone(e.realmID) }

// DiscoveredPeers returns advisory announce candidates. Realm authentication
// has not necessarily completed; Node promotes candidates after proof.
func (e *Endpoint) DiscoveredPeers() []meshbus.Peer { return e.directory.Peers() }

// DiscoveredRoutes returns advisory routes learned from presence.
func (e *Endpoint) DiscoveredRoutes() []string { return e.directory.Routes() }

// DiscoveryDirectory exposes the bounded candidate directory.
func (e *Endpoint) DiscoveryDirectory() *meshbus.PeerDirectory { return e.directory }

// SetPeerObserver binds discovery and realm-authentication events before Start.
func (e *Endpoint) SetPeerObserver(observer meshbus.PeerObserver) error {
	if observer == nil {
		return fmt.Errorf("%w: peer observer is required", ErrInvalidConfig)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrClosed
	}
	if e.started {
		return fmt.Errorf("%w: peer observer must be set before Start", ErrInvalidConfig)
	}
	e.observer = observer
	return nil
}

func (e *Endpoint) handlerContext() context.Context {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.runContext != nil {
		return e.runContext
	}
	return context.Background()
}

// DestinationForIdentity returns the last authenticated destination learned
// through an announce or an outbound session.
func (e *Endpoint) DestinationForIdentity(identityHash string) (string, bool) {
	key := strings.ToLower(strings.TrimSpace(identityHash))
	destinationHash, ok := e.connections.destination(key)
	if !ok {
		return "", false
	}
	return hex.EncodeToString(destinationHash), true
}

// SendMessage sends opaque application bytes over an authenticated realm
// session. It does not inspect the payload or accept a payload-provided sender.
func (e *Endpoint) SendMessage(ctx context.Context, target string, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(payload) == 0 {
		return fmt.Errorf("%w: payload is required", meshbus.ErrInvalidMessage)
	}
	destinationHash, key, err := parseDestination(target)
	if err != nil {
		return err
	}

	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return ErrClosed
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
	if len(payload) > active.channel.MDU() {
		return fmt.Errorf("direct message: %d bytes exceed RNS Channel MDU %d", len(payload), active.channel.MDU())
	}
	if err := e.sendChannel(ctx, active, &directMessage{data: bytes.Clone(payload)}); err != nil {
		return fmt.Errorf("send RNS Channel message: %w", err)
	}
	return nil
}

var _ meshbus.Sender = (*Endpoint)(nil)
var _ meshbus.NodeTransport = (*Endpoint)(nil)

func parseDestination(value string) ([]byte, string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != 16 {
		return nil, "", fmt.Errorf("%w: expected a 32-character hex hash", ErrInvalidDestination)
	}
	return decoded, value, nil
}

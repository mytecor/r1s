package rns

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"

	"github.com/Quad4-Software/Reticulum-Go/pkg/destination"
	"github.com/Quad4-Software/Reticulum-Go/pkg/identity"
	"github.com/Quad4-Software/Reticulum-Go/pkg/link"
)

// ListenerConfig tunes the allocator-side tunnel edge.
type ListenerConfig struct {
	// IdentitySource is the persistent RNS identity file path (or encoded
	// private identity) shared with the control plane. The tunnel Link uses
	// this same identity; no separate edge key is minted.
	IdentitySource string
	// ClusterKey is the shared 256-bit cluster secret. The Backbone/TCP
	// interface access code is derived from it, so only cluster members can
	// form the private tunnel underlay.
	ClusterKey []byte
	// BindAddress is the allocator's Ygg IPv6 address to bind the
	// Backbone/TCP listener to, and ListenPort is the tunnel listener port.
	// Together they form the advertised Endpoint host:port.
	BindAddress string
	ListenPort  int
	// ConfigPath is the ephemeral storage path for the tunnel stack's
	// Reticulum config; empty uses an in-memory default.
	ConfigPath string
	LogLevel   int
}

// Listener is the allocator-side tunnel edge. It runs the private tunnel RNS
// stack with exactly one Backbone/TCP server interface and Accepts inbound
// tunnel Links, awaiting Link.Identify() before a Conn is returned.
type Listener struct {
	config      ListenerConfig
	stack       *stack
	identity    *identity.Identity
	destination *destination.Destination

	mu      sync.Mutex
	closed  bool
	links   chan *link.Link
	closeCh chan struct{}
}

// NewListener constructs the allocator tunnel edge without starting it. It
// derives the Backbone IFAC from the cluster secret and binds the
// Backbone/TCP server on BindAddress:ListenPort.
func NewListener(config ListenerConfig) (*Listener, error) {
	if config.IdentitySource == "" {
		return nil, errors.New("tunnel listener: identity source is required")
	}
	if len(config.ClusterKey) == 0 {
		return nil, errors.New("tunnel listener: cluster key is required")
	}
	if config.ListenPort <= 0 {
		return nil, errors.New("tunnel listener: listen port is required")
	}
	passphrase, err := deriveIFACPassphrase(config.ClusterKey)
	if err != nil {
		return nil, err
	}

	localIdentity, err := loadOrCreateIdentity(config.IdentitySource)
	if err != nil {
		return nil, fmt.Errorf("load tunnel identity: %w", err)
	}
	stack, err := newStack(baseConfig(config.ConfigPath, config.LogLevel))
	if err != nil {
		localIdentity.Close()
		return nil, fmt.Errorf("construct tunnel RNS stack: %w", err)
	}
	localDestination, err := destination.New(localIdentity, destination.In, destination.Single, tunnelAppName, stack.transport, tunnelAspect)
	if err != nil {
		localIdentity.Close()
		return nil, fmt.Errorf("construct tunnel destination: %w", err)
	}
	localDestination.AcceptsLinks(true)

	listener := &Listener{
		config: config, stack: stack, identity: localIdentity, destination: localDestination,
		links: make(chan *link.Link, 16), closeCh: make(chan struct{}),
	}
	localDestination.SetLinkEstablishedCallback(listener.onLinkEstablished)

	if err := ensureBackbone(); err != nil {
		localIdentity.Close()
		return nil, err
	}
	// The cluster wire cipher is applied by underlay wrappers around the
	// concrete Backbone interfaces. It is deliberately kept out of
	// Reticulum's IFAC machinery: its transport re-applies ApplyIFACInbound on
	// every inbound packet and filters multi-hop PLAIN/GROUP before unmasking,
	// which makes a masked single-hop private underlay impossible to use
	// directly (see underlay.go). Reticulum's newSpawnedBackboneClient also
	// never carries any wire protection to the client-half interface it spawns
	// for an accepted connection, so the SpawnBackbone hook re-wraps and binds
	// every spawned client here.
	cipher, err := newWireCipher(passphrase)
	if err != nil {
		localIdentity.Close()
		return nil, fmt.Errorf("derive tunnel wire cipher: %w", err)
	}
	server, err := newBackboneServer("tunnel-backbone", config.BindAddress, config.ListenPort, cipher, stack)
	if err != nil {
		localIdentity.Close()
		return nil, fmt.Errorf("construct tunnel Backbone interface: %w", err)
	}
	// The parent underlay is registered so the transport can fan announce and
	// link packets that are not directed at a single client (e.g. the
	// allocator's initial self-announce); each spawned client is bound by the
	// SpawnBackbone hook above.
	if err := stack.attach(server.underlay()); err != nil {
		localIdentity.Close()
		return nil, err
	}
	return listener, nil
}

// Start starts the tunnel stack and the Backbone listener, and announces the
// allocator's tunnel destination so identified client links can find it.
func (l *Listener) Start(ctx context.Context) error {
	if err := l.stack.Start(); err != nil {
		return fmt.Errorf("start tunnel RNS stack: %w", err)
	}
	// Announce the allocator tunnel destination over the tunnel interface so a
	// client that knows only the destination hash can recall the identity and
	// establish the Link. The announcement is confined to the tunnel underlay
	// (a private Backbone/TCP pair), never the control RNS.
	if err := l.destination.Announce(false, nil, nil); err != nil {
		_ = l.stack.Close()
		return fmt.Errorf("announce tunnel destination: %w", err)
	}
	return nil
}

// onLinkEstablished is invoked by the destination when an inbound Link is
// established. The Link is handed to Accept; identification is awaited there.
func (l *Listener) onLinkEstablished(value any) {
	inbound, ok := value.(*link.Link)
	if !ok || inbound == nil {
		return
	}
	select {
	case l.links <- inbound:
	case <-l.closeCh:
		inbound.Teardown()
	}
}

// Accept blocks until an inbound, identified tunnel Link is ready. The
// returned Conn carries the verified remote identity hash. Accept returns an
// error once the listener is closed.
func (l *Listener) Accept(ctx context.Context) (Conn, error) {
	for {
		select {
		case <-l.closeCh:
			return nil, errors.New("tunnel listener closed")
		case <-ctx.Done():
			return nil, ctx.Err()
		case inbound := <-l.links:
			stream, err := newConn(l.stack.transport, inbound, nil)
			if err != nil {
				inbound.Teardown()
				return nil, err
			}
			// Introduce the allocator's own persistent identity to the peer so
			// the dialer can verify us the same way we verify it. Both sides
			// call Link.Identify: only the identity verified over the Link is
			// trusted, never anything the peer asserts.
			if err := inbound.Identify(l.identity); err != nil {
				inbound.Teardown()
				return nil, fmt.Errorf("identify tunnel Link: %w", err)
			}
			if err := stream.awaitIdentification(ctx); err != nil {
				inbound.Teardown()
				return nil, err
			}
			return stream, nil
		}
	}
}

// Endpoint returns the allocator tunnel advertisement: the Backbone/TCP
// listener address and the tunnel RNS destination hash.
func (l *Listener) Endpoint() Endpoint {
	return Endpoint{
		Address:         fmt.Sprintf("%s:%d", l.config.BindAddress, l.config.ListenPort),
		DestinationHash: hex.EncodeToString(l.destination.GetHash()),
	}
}

// Close stops the tunnel stack and releases blocked Accept calls.
func (l *Listener) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	close(l.closeCh)
	l.mu.Unlock()
	l.identity.Close()
	return l.stack.Close()
}

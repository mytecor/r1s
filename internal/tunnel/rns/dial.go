package rns

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Quad4-Software/Reticulum-Go/pkg/destination"
	"github.com/Quad4-Software/Reticulum-Go/pkg/identity"
	"github.com/Quad4-Software/Reticulum-Go/pkg/interfaces"
	"github.com/Quad4-Software/Reticulum-Go/pkg/link"
)

// DialerConfig tunes the client-side tunnel edge.
type DialerConfig struct {
	// IdentitySource is the persistent RNS identity file path (or encoded
	// private identity) shared with the control plane. The tunnel Link uses
	// this same identity; no separate edge key is minted.
	IdentitySource string
	// ClusterKey is the shared 256-bit cluster secret used to derive the
	// Backbone/TCP interface access code.
	ClusterKey []byte
	// ConfigPath is the ephemeral storage path for the tunnel stack's
	// Reticulum config; empty uses an in-memory default.
	ConfigPath string
	LogLevel   int
}

// Dialer is the client-side tunnel edge. It runs the private tunnel RNS stack
// and Dial opens one tunnel Link to the allocator per call, mapping each Link
// to exactly one stream (the one-Link-per-stream model).
type Dialer struct {
	config   DialerConfig
	stack    *stack
	identity *identity.Identity

	mu          sync.Mutex
	closed      bool
	attached    bool
	attachReady chan struct{}
	attachErr   error
}

// NewDialer constructs the client tunnel edge without starting it.
func NewDialer(config DialerConfig) (*Dialer, error) {
	if config.IdentitySource == "" {
		return nil, errors.New("tunnel dialer: identity source is required")
	}
	if len(config.ClusterKey) == 0 {
		return nil, errors.New("tunnel dialer: cluster key is required")
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
	return &Dialer{
		config: config, stack: stack, identity: localIdentity,
		attachReady: make(chan struct{}),
	}, nil
}

// Start starts the tunnel transport. The outbound Backbone client interface is
// attached lazily by Dial (the target is learned from the control plane).
func (d *Dialer) Start(ctx context.Context) error {
	return d.stack.Start()
}

// ensureAttached attaches the outbound Backbone/TCP client interface to the
// allocator's advertised address, once. It is derived from the caller-supplied
// endpoint: one private tunnel underlay connection per Dial target. Redialing
// reuses the attached interface; the target address is stable per allocator.
func (d *Dialer) ensureAttached(endpoint Endpoint) error {
	host, port, err := endpoint.hostAndPort()
	if err != nil {
		return err
	}
	passphrase, err := deriveIFACPassphrase(d.config.ClusterKey)
	if err != nil {
		return err
	}
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return errors.New("tunnel dialer closed")
	}
	if d.attached {
		d.mu.Unlock()
		return nil
	}
	d.mu.Unlock()

	if err := ensureBackbone(); err != nil {
		return err
	}

	cfg := backboneConfig("tunnel-backbone-client", host, port)
	cfg.Type = "BackboneClientInterface"
	cfg.TargetHost = host
	cfg.TargetPort = port
	clientIface, err := interfaces.NewFromConfig("tunnel-backbone-client", cfg)
	if err != nil {
		return fmt.Errorf("construct tunnel Backbone client interface: %w", err)
	}
	cipher, err := newWireCipher(passphrase)
	if err != nil {
		return fmt.Errorf("derive tunnel wire cipher: %w", err)
	}
	under := newUnderlay(clientIface, cipher)
	// The dialer client is a concrete BackboneClientInterface; bind its
	// inbound path so cluster frames are unmasked before the transport. The
	// underlay reports GetIFAC()==nil so the transport never drops on its own
	// (broken) IFAC pipeline. This must run after the underlay is registered
	// so its transport callback is installed.
	if err := d.stack.attach(under); err != nil {
		return err
	}
	bindClientInbound(clientIface, under)
	d.mu.Lock()
	d.attached = true
	d.attachErr = nil
	close(d.attachReady)
	d.mu.Unlock()
	return nil
}

// Dial opens a tunnel Link to the allocator endpoint, identifies to the peer,
// waits for the peer's identification, and returns the one-stream Conn.
func (d *Dialer) Dial(ctx context.Context, endpoint Endpoint) (Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(endpoint.DestinationHash) == 0 {
		return nil, errors.New("tunnel dial: destination hash is required")
	}
	destinationHash, err := hex.DecodeString(endpoint.DestinationHash)
	if err != nil || len(destinationHash) != 16 {
		return nil, errors.New("tunnel dial: invalid destination hash")
	}

	if err := d.ensureAttached(endpoint); err != nil {
		return nil, err
	}
	if err := d.awaitPath(ctx, destinationHash); err != nil {
		return nil, fmt.Errorf("discover tunnel path: %w", err)
	}
	remoteIdentity, err := identity.Recall(destinationHash)
	if err != nil {
		return nil, fmt.Errorf("recall announced tunnel identity: %w", err)
	}
	outbound, err := destination.FromHash(destinationHash, remoteIdentity, destination.Single, d.stack.transport)
	if err != nil {
		return nil, fmt.Errorf("construct tunnel destination: %w", err)
	}

	established := make(chan *link.Link, 1)
	failed := make(chan struct{}, 1)
	var outboundLink *link.Link
	outboundLink = link.NewLink(outbound, d.stack.transport, nil, func(value *link.Link) {
		select {
		case established <- value:
		default:
		}
	}, func(*link.Link) {
		select {
		case failed <- struct{}{}:
		default:
		}
	})
	if err := outboundLink.Establish(); err != nil {
		return nil, fmt.Errorf("establish tunnel RNS link: %w", err)
	}
	select {
	case <-failed:
		outboundLink.Teardown()
		return nil, errors.New("tunnel RNS link closed before establishment")
	case <-ctx.Done():
		outboundLink.Teardown()
		return nil, ctx.Err()
	case outboundLink = <-established:
	}

	stream, err := newConn(d.stack.transport, outboundLink, nil)
	if err != nil {
		outboundLink.Teardown()
		return nil, err
	}
	// Introduce ourselves to the allocator; the allocator's authorization uses
	// the identity it verifies (Link.Identify), not this outbound copy.
	if err := outboundLink.Identify(d.identity); err != nil {
		outboundLink.Teardown()
		return nil, fmt.Errorf("identify tunnel Link: %w", err)
	}
	if err := stream.awaitIdentification(ctx); err != nil {
		outboundLink.Teardown()
		return nil, err
	}
	return stream, nil
}

// awaitPath waits until the Backbone underlay is online and the transport has
// a path to the allocator's tunnel destination, issuing a path request when
// needed. The allocator answers the path request with its tunnel destination
// announce, which also carries the identity the dialer recalls.
func (d *Dialer) awaitPath(ctx context.Context, destinationHash []byte) error {
	if err := d.stack.waitOnline(ctx); err != nil {
		return err
	}
	if d.stack.transport.HasPath(destinationHash) {
		return nil
	}
	if err := d.stack.transport.RequestPath(destinationHash, "", nil, false); err != nil {
		return err
	}
	// Poll for the path until it lands or the context is done. The allocator's
	// tunnel destination announce (installed over the Backbone underlay) is
	// what answers the path request.
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		if d.stack.transport.HasPath(destinationHash) {
			return nil
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Close stops the tunnel stack.
func (d *Dialer) Close() error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	d.mu.Unlock()
	d.identity.Close()
	return d.stack.Close()
}

// Package client provides the public, run-oriented r1s client API.
//
// A Client is an ephemeral RNS participant and controls one logical run in its
// lifetime. It does not start a local API server or persist requests, identities,
// or lease intent. The authenticated RNS identity owned by the Client is the
// authority for every execution attempt it creates.
package client

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	coreclient "github.com/mytecor/r1s/internal/client"
	"github.com/mytecor/r1s/internal/cluster"
	"github.com/mytecor/r1s/internal/transport/rns"
)

const defaultNetworkWait = 30 * time.Second

var (
	// ErrClosed is returned after the client has been closed.
	ErrClosed = errors.New("r1s client is closed")
	// ErrNotStarted is returned when Run is called before Start.
	ErrNotStarted = errors.New("r1s client is not started")
	// ErrRunActive is returned when the same ephemeral client is asked to own a
	// second logical run. Construct a fresh Client to get fresh run authority.
	ErrRunActive = errors.New("r1s client already controls or controlled a run")
)

// Config configures an ephemeral run controller.
type Config struct {
	// ClusterKey is the 32-byte cluster membership secret. It is used for RNS
	// link authentication and is never included in workload or protocol data.
	ClusterKey []byte
	// NetworkWait bounds RNS path discovery, link establishment, and sends.
	// Zero uses 30 seconds.
	NetworkWait time.Duration
}

// Open resolves a locally joined cluster ID (or unique prefix) and constructs
// an ephemeral Client. The returned Client still needs Start before Run.
func Open(selector string, config Config) (*Client, error) {
	directory, err := cluster.DefaultDirectory()
	if err != nil {
		return nil, err
	}
	key, _, err := cluster.Resolve(directory, selector)
	if err != nil {
		return nil, fmt.Errorf("select cluster: %w", err)
	}
	config.ClusterKey = key
	return New(config)
}

// Client is an ephemeral RNS run controller. It is safe for concurrent log
// reads and event callbacks while Run is active, but intentionally accepts only
// one Run call in its lifetime so one instance has the same authority boundary
// as one r1s run process.
type Client struct {
	mu       sync.Mutex
	endpoint controllerEndpoint
	core     *coreclient.Client
	identity []byte

	started       bool
	closed        bool
	runActive     bool
	runUsed       bool
	offerReleases func() []coreclient.PendingRelease
	waitMu        sync.Mutex
	waiters       map[string][]chan *r1sv1.Envelope
	stateChanged  chan struct{}
	logMu         sync.Mutex
}

// controllerEndpoint is the authenticated transport surface needed by the
// run controller. Keeping it narrow preserves deterministic contract tests and
// prevents the public lifecycle from depending on Reticulum implementation
// details.
type controllerEndpoint interface {
	Start(context.Context) error
	Send(context.Context, string, *r1sv1.Envelope) error
	Close() error
	Discoveries() <-chan rns.Service
	DestinationForIdentity(string) (string, bool)
}

// New constructs an ephemeral client without starting its RNS endpoint.
func New(config Config) (*Client, error) {
	if config.NetworkWait == 0 {
		config.NetworkWait = defaultNetworkWait
	}
	if config.NetworkWait < 0 {
		return nil, errors.New("client: network wait must be positive")
	}
	c := &Client{waiters: make(map[string][]chan *r1sv1.Envelope), stateChanged: make(chan struct{}, 1)}
	endpoint, err := rns.New(rns.Config{
		EphemeralIdentity: true,
		ClusterKey:        bytes.Clone(config.ClusterKey),
		NetworkWait:       config.NetworkWait,
	}, c.handleEnvelope)
	if err != nil {
		return nil, err
	}
	c.endpoint = endpoint
	identityHash, err := hex.DecodeString(endpoint.Name())
	if err != nil {
		_ = endpoint.Close()
		return nil, fmt.Errorf("decode ephemeral identity: %w", err)
	}
	c.identity = identityHash
	c.core, err = coreclient.New(coreclient.Config{Identity: identityHash})
	if err != nil {
		_ = endpoint.Close()
		return nil, err
	}
	return c, nil
}

// Identity returns the authenticated RNS identity hash used as execution
// authority for this client's lifetime.
func (c *Client) Identity() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return bytes.Clone(c.identity)
}

// Start attaches the ephemeral endpoint to the platform RNS shared instance.
// The supplied context bounds the complete lifetime of the client.
func (c *Client) Start(ctx context.Context) error {
	if ctx == nil {
		return errors.New("client: context is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	if c.started {
		return nil
	}
	if err := c.endpoint.Start(ctx); err != nil {
		return err
	}
	c.started = true
	c.offerReleases = c.core.StartOfferReleases(ctx, c.endpoint.Send)
	return nil
}

// Close stops the endpoint. Any execution continues until its allocator-side
// lease expires; transport connection state never determines its lifetime.
// The returned slice reports offer releases that could not be delivered before
// shutdown and will therefore fall back to allocator-side offer expiry.
func (c *Client) Close() []PendingRelease {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	stopReleases := c.offerReleases
	c.offerReleases = nil
	c.mu.Unlock()

	var pending []PendingRelease
	if stopReleases != nil {
		for _, release := range stopReleases() {
			pending = append(pending, PendingRelease{
				Destination: release.Destination,
				OfferID:     release.Envelope.GetExecutionOfferRelease().GetOfferId(),
			})
		}
	}
	_ = c.endpoint.Close()
	return pending
}

// PendingRelease describes an offer release that will instead be reclaimed by
// allocator-side expiry after the client shuts down.
type PendingRelease struct {
	Destination string
	OfferID     string
}

func (c *Client) beginRun() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	if !c.started {
		return ErrNotStarted
	}
	if c.runActive || c.runUsed {
		return ErrRunActive
	}
	c.runActive = true
	c.runUsed = true
	return nil
}

func (c *Client) endRun() {
	c.mu.Lock()
	c.runActive = false
	c.mu.Unlock()
}

func (c *Client) registerWaiter(correlationID string) (chan *r1sv1.Envelope, func()) {
	ch := make(chan *r1sv1.Envelope, 1)
	c.waitMu.Lock()
	c.waiters[correlationID] = append(c.waiters[correlationID], ch)
	c.waitMu.Unlock()
	var once sync.Once
	return ch, func() {
		once.Do(func() {
			c.waitMu.Lock()
			defer c.waitMu.Unlock()
			channels := c.waiters[correlationID]
			for i, candidate := range channels {
				if candidate == ch {
					c.waiters[correlationID] = append(channels[:i], channels[i+1:]...)
					break
				}
			}
			if len(c.waiters[correlationID]) == 0 {
				delete(c.waiters, correlationID)
			}
		})
	}
}

func (c *Client) dispatchEnvelope(envelope *r1sv1.Envelope) {
	correlationID := envelope.GetCorrelationId()
	if correlationID == "" {
		return
	}
	c.waitMu.Lock()
	channels := c.waiters[correlationID]
	delete(c.waiters, correlationID)
	c.waitMu.Unlock()
	for _, ch := range channels {
		select {
		case ch <- envelope:
		default:
		}
	}
}

func (c *Client) handleEnvelope(ctx context.Context, envelope *r1sv1.Envelope) error {
	identityKey := hex.EncodeToString(envelope.GetSender())
	if destination, ok := c.endpoint.DestinationForIdentity(identityKey); ok {
		if err := c.core.RegisterAllocator(coreclient.Allocator{Identity: envelope.GetSender(), Destination: destination}); err != nil {
			return err
		}
	}
	if err := c.core.Handle(ctx, envelope); err != nil {
		return err
	}
	if envelope.GetExecutionState() != nil && c.stateChanged != nil {
		select {
		case c.stateChanged <- struct{}{}:
		default:
		}
	}
	c.dispatchEnvelope(envelope)
	return nil
}

func (c *Client) send(ctx context.Context, destination string, envelope *r1sv1.Envelope) error {
	sendCtx, cancel := context.WithTimeout(ctx, defaultNetworkWait)
	defer cancel()
	return c.endpoint.Send(sendCtx, destination, envelope)
}

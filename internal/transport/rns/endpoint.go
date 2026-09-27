// Package rns adapts r1s discovery descriptors and Protobuf envelopes to the
// reusable authenticated transport implemented by meshbus/rns.
package rns

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/Quad4-Software/Reticulum-Go/pkg/common"
	"github.com/Quad4-Software/Reticulum-Go/pkg/interfaces"
	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	coretransport "github.com/mytecor/r1s/internal/transport"
	"github.com/mytecor/r1s/meshbus"
	meshrns "github.com/mytecor/r1s/meshbus/rns"
)

const (
	defaultAppName = "r1s"
	defaultAspect  = "allocator"
)

var (
	ErrInvalidConfig         = meshrns.ErrInvalidConfig
	ErrNotStarted            = meshrns.ErrNotStarted
	ErrInvalidDestination    = meshrns.ErrInvalidDestination
	ErrClusterAuthentication = meshrns.ErrRealmAuthentication
)

// Config defines the r1s-specific projection onto a reusable meshbus RNS
// endpoint. Reticulum and Interfaces are test-harness overrides; production
// endpoints attach to the platform-default shared instance.
type Config struct {
	Reticulum         *common.ReticulumConfig
	IdentitySource    string
	EphemeralIdentity bool
	ClusterKey        []byte
	Capacity          map[string]uint32
	Node              *r1sv1.NodeCapabilities
	Tunnel            *TunnelAdvertisement
	AppName           string
	Aspect            string
	AnnounceInterval  time.Duration
	NetworkWait       time.Duration
	Interfaces        []interfaces.Interface
}

// Service describes an allocator learned from a realm-validated r1s
// descriptor.
type Service struct {
	Destination string
	Identity    string
	Descriptor  Descriptor
	Hops        uint8
}

// TunnelEndpoint returns the allocator's advertised tunnel endpoint.
func (s Service) TunnelEndpoint() (host string, port int, destination string, ok bool) {
	if s.Descriptor.TunnelPort <= 0 || s.Descriptor.TunnelHost == "" {
		return "", 0, "", false
	}
	return s.Descriptor.TunnelHost, s.Descriptor.TunnelPort, s.Descriptor.TunnelDestination, true
}

// Endpoint is the thin r1s facade over a public meshbus RNS endpoint.
type Endpoint struct {
	transport  *meshrns.Endpoint
	identity   []byte
	discovered chan Service
}

var _ coretransport.Endpoint = (*Endpoint)(nil)

// New constructs an r1s endpoint without starting its meshbus transport.
func New(config Config, handler coretransport.Handler) (*Endpoint, error) {
	if handler == nil {
		return nil, fmt.Errorf("%w: handler is required", ErrInvalidConfig)
	}
	if config.AppName == "" {
		config.AppName = defaultAppName
	}
	if config.Aspect == "" {
		config.Aspect = defaultAspect
	}

	discovered := make(chan Service, 32)
	passive := len(config.Capacity) == 0
	var presenceMetadata map[string]string
	if !passive {
		descriptor, descriptorErr := newDescriptor(config.Capacity, config.Node, config.Tunnel)
		if descriptorErr != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, descriptorErr)
		}
		presenceMetadata = descriptor.metadata()
	}
	transport, err := meshrns.New(meshrns.Config{
		Reticulum:         config.Reticulum,
		IdentitySource:    config.IdentitySource,
		EphemeralIdentity: config.EphemeralIdentity,
		RealmKey:          config.ClusterKey,
		AppName:           config.AppName,
		Aspect:            config.Aspect,
		AnnounceInterval:  config.AnnounceInterval,
		NetworkWait:       config.NetworkWait,
		Interfaces:        config.Interfaces,
		PresenceMetadata:  presenceMetadata,
		Passive:           passive,
		Handler:           envelopeHandler(handler),
	})
	if err != nil {
		return nil, err
	}
	if err := transport.SetPeerObserver(serviceObserver{discovered: discovered}); err != nil {
		_ = transport.Close()
		return nil, err
	}
	identityHash, err := hex.DecodeString(transport.Name())
	if err != nil {
		_ = transport.Close()
		return nil, fmt.Errorf("decode local identity: %w", err)
	}
	return &Endpoint{transport: transport, identity: identityHash, discovered: discovered}, nil
}

type serviceObserver struct{ discovered chan<- Service }

func (o serviceObserver) Discovered(peer meshbus.Peer) error {
	descriptor, err := parseDescriptorMetadata(peer.Metadata)
	if err != nil {
		return err
	}
	service := Service{Destination: peer.Route, Identity: peer.ID.String(), Descriptor: descriptor, Hops: peer.Hops}
	select {
	case o.discovered <- service:
	default:
	}
	return nil
}

func (serviceObserver) Authenticated(meshbus.PeerID, string) error { return nil }

func (e *Endpoint) Name() string                { return e.transport.Name() }
func (e *Endpoint) Destination() string         { return e.transport.Destination() }
func (e *Endpoint) Discoveries() <-chan Service { return e.discovered }
func (e *Endpoint) Start(ctx context.Context) error {
	return e.transport.Start(ctx)
}
func (e *Endpoint) Close() error { return e.transport.Close() }
func (e *Endpoint) DestinationForIdentity(identity string) (string, bool) {
	return e.transport.DestinationForIdentity(identity)
}

func translateSendError(err error) error {
	switch {
	case errors.Is(err, meshrns.ErrClosed):
		return coretransport.ErrEndpointClosed
	default:
		return err
	}
}

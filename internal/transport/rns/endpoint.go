// Package rns adapts r1s discovery descriptors and Protobuf envelopes to the
// reusable authenticated transport implemented by meshbus/transport/rns.
package rns

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/Quad4-Software/Reticulum-Go/pkg/common"
	"github.com/Quad4-Software/Reticulum-Go/pkg/debug"
	"github.com/Quad4-Software/Reticulum-Go/pkg/interfaces"
	meshbus "github.com/mytecor/meshbus/core"
	meshrns "github.com/mytecor/meshbus/transport/rns"
	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	coretransport "github.com/mytecor/r1s/internal/transport"
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

func init() {
	// Silence third-party mesh announce signature warnings and noisy path
	// requests on shared instances; keep errors and critical failures.
	debug.SetDebugLevel(debug.DebugError)
}

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
	node       *meshbus.Node
	advisoryMu sync.Mutex
	advisory   map[meshbus.PeerID]AllocatorStatus
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

	discovered := make(chan Service, 1)
	passive := len(config.Capacity) == 0
	var presenceMetadata map[string]string
	if !passive {
		descriptor, descriptorErr := newDescriptor(config.Capacity, config.Node, config.Tunnel)
		if descriptorErr != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, descriptorErr)
		}
		presenceMetadata = descriptor.metadata()
	}
	transportConfig := meshrns.Config{
		StackMode: func() meshrns.StackMode {
			if config.Reticulum != nil {
				return meshrns.StackStandalone
			}
			return meshrns.StackSharedClient
		}(),
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
	}
	e := &Endpoint{discovered: discovered, advisory: make(map[meshbus.PeerID]AllocatorStatus)}
	node, err := meshbus.NewNode(meshbus.NodeConfig{
		DirectHandler: envelopeHandler(handler),
		Transport: func(handler meshbus.Handler) (meshbus.NodeTransport, error) {
			transportConfig.Handler = handler
			transport, err := meshrns.New(transportConfig)
			e.transport = transport
			if err != nil {
				return nil, err
			}
			return &observedTransport{Endpoint: transport, notify: discovered}, nil
		},
	})
	if err != nil {
		return nil, err
	}
	e.node = node
	e.identity = node.Identity().Bytes()
	if _, err := node.Subscribe("r1s.allocator.>", e.receiveStatus); err != nil {
		_ = node.Close()
		return nil, err
	}
	return e, nil
}

// observedTransport keeps only a coalesced wake-up. Discovery contents live in
// the Node directories, so a burst never discards the newest peer state.
type observedTransport struct {
	*meshrns.Endpoint
	notify chan Service
}

// meshbus v0.4.0 SendMessage requires a destination mapping, but an inbound
// passive client has only an authenticated session keyed by identity. Keep
// this session-resolution workaround below the Node's PeerID boundary.
func (t *observedTransport) SendMessage(ctx context.Context, peer meshbus.PeerID, payload []byte) error {
	err := t.Endpoint.SendMessage(ctx, peer, payload)
	if errors.Is(err, meshbus.ErrUnknownPeer) {
		return t.Endpoint.SendToDestination(ctx, peer.String(), payload)
	}
	return err
}

func (t *observedTransport) SetPeerObserver(observer meshbus.PeerObserver) error {
	return t.Endpoint.SetPeerObserver(peerObserver{observer, t.notify, t.DestinationForIdentity})
}

type peerObserver struct {
	meshbus.PeerObserver
	notify  chan Service
	resolve func(string) (string, bool)
}

func (o peerObserver) Discovered(peer meshbus.Peer) error {
	err := o.PeerObserver.Discovered(peer)
	if descriptor, parseErr := parseDescriptorMetadata(peer.Metadata); parseErr == nil {
		destination, _ := o.resolve(peer.ID.String())
		select {
		case o.notify <- Service{Destination: destination, Identity: peer.ID.String(), Descriptor: descriptor, Hops: peer.Hops}:
		default:
		}
	}
	return err
}
func (o peerObserver) Authenticated(peer meshbus.PeerID) error {
	err := o.PeerObserver.Authenticated(peer)
	select {
	case o.notify <- Service{}:
	default:
	}
	return err
}

func (e *Endpoint) Name() string        { return e.node.Identity().String() }
func (e *Endpoint) Destination() string { return e.transport.Destination() }

// Discoveries is a compatibility event stream; use Services for lossless snapshots.
func (e *Endpoint) Discoveries() <-chan Service { return e.discovered }
func (e *Endpoint) Services() []Service {
	peers := append(e.node.DiscoveredPeers(), e.node.Peers()...)
	byIdentity := make(map[string]Service, len(peers))
	for _, peer := range peers {
		descriptor, err := parseDescriptorMetadata(peer.Metadata)
		if err != nil {
			continue
		}
		destination, _ := e.transport.DestinationForIdentity(peer.ID.String())
		e.advisoryMu.Lock()
		if status, ok := e.advisory[peer.ID]; ok && time.Since(status.receivedAt) < statusTTL {
			descriptor.Capacity = make(map[string]uint32, len(status.Capacity))
			for class, slots := range status.Capacity {
				if status.Ready && !status.Draining {
					descriptor.Capacity[class] = slots
				}
			}
		}
		e.advisoryMu.Unlock()
		byIdentity[peer.ID.String()] = Service{Destination: destination, Identity: peer.ID.String(), Descriptor: descriptor, Hops: peer.Hops}
	}
	result := make([]Service, 0, len(byIdentity))
	for _, service := range byIdentity {
		result = append(result, service)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Identity < result[j].Identity })
	return result
}
func (e *Endpoint) Start(ctx context.Context) error { return e.node.Start(ctx) }
func (e *Endpoint) Close() error                    { return e.node.Close() }
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

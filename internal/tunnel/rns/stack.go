// Package rns holds the private tunnel Reticulum data plane (F21-01). It is a
// deliberately minimal, self-contained transport: a separate Reticulum-Go
// stack that runs over the system yggdrasil service only as an IP/TCP underlay,
// never over the control-plane RNS. The main control RNS is used exclusively
// for the control plane.
//
// The package does not reuse internal/transport/rns (whose endpoint is tuned
// for the control plane: announces, allocator discovery, protobuf Envelope,
// cluster HMAC challenge, connection registry). It is a fresh implementation
// that holds Link / Channel / Buffer as the only tunnel data primitives and
// enforces the "one local TCP connection = one RNS Link = one tunnel stream"
// model.
package rns

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Quad4-Software/Reticulum-Go/pkg/common"
	"github.com/Quad4-Software/Reticulum-Go/pkg/interfaces"
	rnstransport "github.com/Quad4-Software/Reticulum-Go/pkg/transport"
)

// stack is the private tunnel Reticulum-Go transport. It is independent of the
// control-plane transport: EnableTransport is false, it never borrows an
// interface from the control RNS, and it carries exactly the interfaces the
// tunnel edge attaches (the allocator's single Ygg Backbone/TCP server, or the
// client's outbound Backbone/TCP client). It never participates in public RNS
// routing or discovery.
type stack struct {
	transport  *rnstransport.Transport
	interfaces []interfaces.Interface
	started    []interfaces.Interface
}

// baseConfig returns a Reticulum configuration for the private tunnel
// transport: transport disabled, ephemeral in-memory storage so the tunnel
// stack never persists path tables or known destinations, and no interfaces at
// construction (each edge attaches its own Backbone/TCP interface). Identity
// and cluster secret are carried separately by the edge, never in this config.
func baseConfig(configPath string, logLevel int) *common.ReticulumConfig {
	cfg := common.DefaultConfig()
	cfg.EnableTransport = false
	cfg.ShareInstance = false
	cfg.ConfigPath = configPath
	cfg.LogLevel = logLevel
	// The tunnel transport must never persist or reuse state from the control
	// plane, and must never side-channel into a shared instance.
	cfg.InMemoryStorage = true
	cfg.Interfaces = make(map[string]*common.InterfaceConfig)
	return cfg
}

func newStack(config *common.ReticulumConfig) (*stack, error) {
	result := &stack{transport: rnstransport.NewTransport(config)}
	return result, nil
}

// attach registers and starts an interface on the tunnel transport.
func (s *stack) attach(value interfaces.Interface) error {
	network, ok := value.(common.NetworkInterface)
	if !ok {
		return fmt.Errorf("tunnel RNS interface %q is not a network interface", value.GetName())
	}
	if err := network.Start(); err != nil {
		return fmt.Errorf("start tunnel RNS interface %q: %w", value.GetName(), err)
	}
	if err := s.transport.RegisterInterface(value.GetName(), network); err != nil {
		_ = network.Stop()
		return fmt.Errorf("register tunnel RNS interface %q: %w", value.GetName(), err)
	}
	s.interfaces = append(s.interfaces, value)
	s.started = append(s.started, value)
	return nil
}

// Start starts the transport and the path-request handler. Interfaces are
// attached by the edge; the transport must be running before outbound path
// requests are issued.
func (s *stack) Start() error {
	if err := s.transport.Start(); err != nil {
		return err
	}
	if err := s.transport.InitializePathRequestHandler(); err != nil {
		_ = s.transport.Close()
		return err
	}
	return nil
}

// Close stops attached interfaces and the transport. It is idempotent.
func (s *stack) Close() error {
	var first error
	for index := len(s.started) - 1; index >= 0; index-- {
		if err := s.started[index].Stop(); err != nil && first == nil {
			first = err
		}
	}
	s.started = nil
	if err := s.transport.Close(); err != nil && first == nil {
		first = err
	}
	return first
}

// backboneConfig builds a BackboneInterface (TCP server) or
// BackboneClientInterface (TCP client) config for the tunnel underlay. The
// cluster cipher is intentionally NOT applied here: Reticulum's IFAC machinery
// cannot handle a masked single-hop private underlay (see underlay.go), so the
// config carries no network_name/passphrase and the cipher is applied by the
// underlay wrappers instead.
func backboneConfig(name, bindAddress string, port int) *common.InterfaceConfig {
	cfg := &common.InterfaceConfig{
		Name:    name,
		Type:    "BackboneInterface",
		Enabled: true,
		Address: bindAddress,
		Port:    port,
	}
	return cfg
}

// interfaceCount returns the number of attached interfaces. Tests assert the
// tunnel transport has exactly its one Backbone/TCP interface.
func (s *stack) interfaceCount() int { return len(s.interfaces) }

// interfaceNames returns a comma-joined list of attached interface names.
func (s *stack) interfaceNames() string {
	names := make([]string, 0, len(s.interfaces))
	for _, value := range s.interfaces {
		names = append(names, value.GetName())
	}
	return strings.Join(names, ",")
}

// waitOnline blocks until at least one attached interface is online (the
// Backbone/TCP underlay is connected) or the context is done. It lets the
// dialer issue a path request only once the underlay can actually carry it.
func (s *stack) waitOnline(ctx context.Context) error {
	if s.anyOnlineLocked() {
		return nil
	}
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if s.anyOnlineLocked() {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *stack) anyOnlineLocked() bool {
	for _, value := range s.interfaces {
		if value.IsOnline() {
			return true
		}
	}
	return false
}

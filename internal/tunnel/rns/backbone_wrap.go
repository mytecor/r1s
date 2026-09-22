package rns

import (
	"github.com/Quad4-Software/Reticulum-Go/pkg/backbone"
	"github.com/Quad4-Software/Reticulum-Go/pkg/common"
	"github.com/Quad4-Software/Reticulum-Go/pkg/interfaces"
)

// backboneServer is the allocator-side private tunnel underlay: a Reticulum
// Backbone/TCP server wrapped in a cluster-wire underlay. Each accepted
// connection spawns a distinct client-half interface; the spawn hook wraps and
// binds every one so the server can talk to many clients (the
// one-Link-per-stream model), each under the same cluster cipher.
type backboneServer struct {
	parent         *interfaces.BackboneInterface
	parentUnderlay *underlay
	cipher         *wireCipher
}

// underlay returns the parent interface wrapped in its cluster underlay.
func (s *backboneServer) underlay() common.NetworkInterface {
	return s.parentUnderlay
}

// newBackboneServer constructs the wrapped Backbone/TCP server bound to
// bindAddress:port. The SpawnBackbone hook is wired internally: it wraps each
// spawned client with the same cluster cipher and registers it with the tunnel
// transport (which is live by the time a client connects). The cipher is never
// registered with Reticulum's IFAC machinery (see underlay.go).
func newBackboneServer(name, bindAddress string, port int, cipher *wireCipher, st *stack) (*backboneServer, error) {
	srv := &backboneServer{cipher: cipher}
	bindSpawned := func(client *interfaces.BackboneClientInterface) {
		srv.bindSpawned(client, st)
	}
	cfg := backboneConfig(name, bindAddress, port)
	raw, err := newReticulumBackbone(cfg, bindSpawned)
	if err != nil {
		return nil, err
	}
	srv.parent = raw
	srv.parentUnderlay = newUnderlay(raw, cipher)
	return srv, nil
}

// newReticulumBackbone builds the concrete Reticulum Backbone/TCP server and
// installs the SpawnBackbone hook (bindSpawned) that the tunnel package uses
// to wrap each spawned client.
func newReticulumBackbone(cfg *common.InterfaceConfig, bindSpawned func(*interfaces.BackboneClientInterface)) (*interfaces.BackboneInterface, error) {
	iface, err := interfaces.NewFromConfigWithContext(cfg.Name, cfg, &interfaces.FromConfigContext{
		BackboneHub:   backbone.Get(),
		SpawnBackbone: bindSpawned,
	})
	if err != nil {
		return nil, err
	}
	return iface.(*interfaces.BackboneInterface), nil
}

// bindSpawned wraps a concrete spawned Backbone client interface in a cluster
// underlay, binds its inbound path, and registers it with the tunnel
// transport.
func (s *backboneServer) bindSpawned(client *interfaces.BackboneClientInterface, st *stack) {
	under := newUnderlay(client, s.cipher)
	bindClientInbound(client, under)
	if err := st.transport.RegisterInterface(client.GetName(), under); err != nil {
		return
	}
}

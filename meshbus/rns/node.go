package rns

import (
	"fmt"
	"time"

	"github.com/mytecor/meshbus"
)

// NodeConfig configures the cohesive meshbus Node backed by Reticulum. The
// endpoint Handler must be left nil because Node installs its composed pub/sub
// and direct-message handler.
type NodeConfig struct {
	Endpoint      Config
	DirectHandler meshbus.Handler
	Bus           meshbus.BusConfig
	Directory     meshbus.DirectoryConfig
	PeerTTL       time.Duration
	SweepInterval time.Duration
	OnPeerError   func(error)
}

// NewNode constructs a cohesive meshbus Node backed by this RNS adapter.
// Callers configure realm membership and presence through Endpoint, but do not
// manually connect its PeerDirectory or inbound handler to the event Bus.
func NewNode(config NodeConfig) (*meshbus.Node, error) {
	if config.Endpoint.Handler != nil {
		return nil, fmt.Errorf("%w: RNS endpoint handler is managed by Node", meshbus.ErrInvalidNode)
	}
	directory := config.Directory
	if directory.MaxPeers == 0 && directory.MaxMetadataBytes == 0 {
		directory = meshbus.DirectoryConfig{
			MaxPeers: config.Endpoint.DirectoryConfig.MaxPeers, MaxMetadataBytes: config.Endpoint.DirectoryConfig.MaxMetadataBytes,
		}
	}
	return meshbus.NewNode(meshbus.NodeConfig{
		DirectHandler: config.DirectHandler,
		Bus:           config.Bus,
		Directory:     directory,
		PeerTTL:       config.PeerTTL,
		SweepInterval: config.SweepInterval,
		OnPeerError:   config.OnPeerError,
		Transport: func(handler meshbus.Handler) (meshbus.NodeTransport, error) {
			endpointConfig := config.Endpoint
			endpointConfig.DirectoryConfig = DirectoryConfig{
				MaxPeers: directory.MaxPeers, MaxMetadataBytes: directory.MaxMetadataBytes,
			}
			endpointConfig.Handler = handler
			return New(endpointConfig)
		},
	})
}

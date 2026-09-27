package rns

import (
	"fmt"

	"github.com/mytecor/r1s/meshbus"
)

// NodeConfig configures the cohesive meshbus Node backed by Reticulum. The
// endpoint Handler must be left nil because Node installs its composed pub/sub
// and direct-message handler.
type NodeConfig struct {
	Endpoint      Config
	DirectHandler meshbus.Handler
	Bus           meshbus.BusConfig
}

// NewNode constructs a cohesive meshbus Node backed by this RNS adapter.
// Callers configure realm membership and presence through Endpoint, but do not
// manually connect its PeerDirectory or inbound handler to the event Bus.
func NewNode(config NodeConfig) (*meshbus.Node, error) {
	if config.Endpoint.Handler != nil {
		return nil, fmt.Errorf("%w: RNS endpoint handler is managed by Node", meshbus.ErrInvalidNode)
	}
	return meshbus.NewNode(meshbus.NodeConfig{
		DirectHandler: config.DirectHandler,
		Bus:           config.Bus,
		Transport: func(handler meshbus.Handler) (meshbus.NodeTransport, error) {
			endpointConfig := config.Endpoint
			endpointConfig.Handler = handler
			return New(endpointConfig)
		},
	})
}

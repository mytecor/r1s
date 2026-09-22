package rns

import (
	"fmt"
	"net"
)

// Endpoint is the transport-neutral allocator tunnel advertisement carried
// through the control plane (F21-02). It describes the minimum needed to
// create a private tunnel transport: the allocator's Backbone/TCP listener
// address (its Ygg IPv6 address plus the tunnel listener port) and the tunnel
// RNS destination hash. It never carries an Ygg public key: the tunnel RNS
// identity is the persistent identity, verified over the Link via Identify,
// not a separately transferred key to pin.
type Endpoint struct {
	// Address is the allocator's Backbone/TCP listener as host:port (the
	// allocator's Ygg IPv6 address and tunnel port).
	Address string
	// DestinationHash is the hex-encoded tunnel RNS destination hash the
	// client dials to reach the allocator's tunnel edge.
	DestinationHash string
}

// hostAndPort splits the endpoint address into its dialable host and port.
func (e Endpoint) hostAndPort() (string, int, error) {
	host, port, err := net.SplitHostPort(e.Address)
	if err != nil {
		return "", 0, fmt.Errorf("invalid tunnel endpoint address %q: %w", e.Address, err)
	}
	var parsed int
	if _, err := fmt.Sscanf(port, "%d", &parsed); err != nil || parsed <= 0 {
		return "", 0, fmt.Errorf("invalid tunnel endpoint port in %q", e.Address)
	}
	return host, parsed, nil
}

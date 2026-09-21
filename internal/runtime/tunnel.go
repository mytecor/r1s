package runtime

import (
	"context"
	"errors"
	"net"
)

var ErrTunnelUnsupported = errors.New("runtime cannot dial execution ports")

// PortDialer connects only to a port inside the specified execution's network
// namespace. Implementations must never fall back to the allocator's network.
type PortDialer interface {
	DialExecution(context.Context, string, uint16) (net.Conn, error)
}

//go:build !linux

package containerd

import (
	"context"
	"net"

	r1sruntime "github.com/mytecor/r1s/internal/runtime"
)

func (b *clientBackend) dialExecution(context.Context, string, string, uint16) (net.Conn, error) {
	return nil, r1sruntime.ErrTunnelUnsupported
}

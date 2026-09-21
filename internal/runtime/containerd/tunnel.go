package containerd

import (
	"context"
	"net"

	r1sruntime "github.com/mytecor/r1s/internal/runtime"
)

// DialExecution opens a connection only for a live execution tracked by this
// adapter; its fingerprint binds the backend lookup to the recovered workload.
func (a *Adapter) DialExecution(ctx context.Context, executionID string, port uint16) (net.Conn, error) {
	m := a.executions
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, ErrClosed
	}
	current := m.entries[executionID]
	if current == nil || !channelClosed(current.ready) || channelClosed(current.done) || current.stopping || current.finishing {
		m.mu.Unlock()
		return nil, ErrExecutionMissing
	}
	fingerprint := current.fingerprint
	m.mu.Unlock()
	dialer, ok := m.backend.(interface {
		dialExecution(context.Context, string, string, uint16) (net.Conn, error)
	})
	if !ok {
		return nil, r1sruntime.ErrTunnelUnsupported
	}
	return dialer.dialExecution(ctx, executionID, fingerprint, port)
}

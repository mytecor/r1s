package transport

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"google.golang.org/protobuf/proto"
)

// Memory is a deterministic, synchronous transport for tests and local harnesses.
type Memory struct {
	mu        sync.Mutex
	endpoints map[string]*MemoryEndpoint
	closed    bool
}

// MemoryEndpoint is a named authenticated source on a Memory transport.
type MemoryEndpoint struct {
	network *Memory
	name    string
	handler Handler
	closed  bool
}

var _ Endpoint = (*MemoryEndpoint)(nil)

// NewMemory constructs an empty in-memory transport.
func NewMemory() *Memory {
	return &Memory{endpoints: make(map[string]*MemoryEndpoint)}
}

// Register adds one named destination. Names are also used as authenticated sender identities.
func (m *Memory) Register(name string, handler Handler) (*MemoryEndpoint, error) {
	if name == "" || handler == nil {
		return nil, ErrInvalidEndpoint
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrTransportClosed
	}
	if _, exists := m.endpoints[name]; exists {
		return nil, fmt.Errorf("%w: %q", ErrDuplicateEndpoint, name)
	}
	endpoint := &MemoryEndpoint{network: m, name: name, handler: handler}
	m.endpoints[name] = endpoint
	return endpoint, nil
}

// Close prevents further registration and delivery.
func (m *Memory) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true
	for _, endpoint := range m.endpoints {
		endpoint.closed = true
	}
	return nil
}

// Name returns the endpoint's stable identity.
func (e *MemoryEndpoint) Name() string { return e.name }

// Send clones an envelope, overwrites its sender, and synchronously invokes the destination.
func (e *MemoryEndpoint) Send(ctx context.Context, destination string, envelope *r1sv1.Envelope) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if envelope == nil {
		return fmt.Errorf("%w: envelope is required", ErrInvalidEndpoint)
	}
	m := e.network
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return ErrTransportClosed
	}
	if e.closed {
		m.mu.Unlock()
		return ErrEndpointClosed
	}
	target, exists := m.endpoints[destination]
	if !exists {
		m.mu.Unlock()
		return fmt.Errorf("%w: %q", ErrUnknownDestination, destination)
	}
	if target.closed {
		m.mu.Unlock()
		return fmt.Errorf("%w: %q", ErrDestinationClosed, destination)
	}
	handler := target.handler
	cloned := proto.Clone(envelope).(*r1sv1.Envelope)
	cloned.Sender = bytes.Clone([]byte(e.name))
	m.mu.Unlock()

	return handler(ctx, cloned)
}

// Close unregisters this endpoint for sending and receiving. It is idempotent.
func (e *MemoryEndpoint) Close() error {
	e.network.mu.Lock()
	defer e.network.mu.Unlock()
	e.closed = true
	return nil
}

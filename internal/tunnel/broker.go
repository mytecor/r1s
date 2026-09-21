package tunnel

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"time"
)

// MemoryBroker is the deterministic in-memory overlay that routes dials from a
// client edge to a remote edge by transport-neutral Endpoint address. It stands
// in for the Yggdrasil mesh in tests, enforcing peer-key pinning at dial time
// exactly as the real edge must.
type MemoryBroker struct {
	mu        sync.Mutex
	listeners map[string]*MemoryListener
	closed    bool
}

// NewMemoryBroker returns an empty in-memory overlay.
func NewMemoryBroker() *MemoryBroker {
	return &MemoryBroker{listeners: make(map[string]*MemoryListener)}
}

// Listen registers an inbound listener for one transport-neutral address. A
// pinKey limits which client PeerKey may dial; nil accepts any key. It returns
// the allocator-edge listener.
func (b *MemoryBroker) Listen(address, pinKey []byte) *MemoryListener {
	l := &MemoryListener{
		broker:   b,
		address:  append([]byte(nil), address...),
		incoming: make(chan *memoryPipe, 1),
		closeCh:  make(chan struct{}),
		pinKey:   append([]byte(nil), pinKey...),
	}
	b.mu.Lock()
	if !b.closed {
		b.listeners[string(address)] = l
	}
	b.mu.Unlock()
	return l
}

// Dial connects the memory-fake client edge to the listener at endpoint. It
// enforces the listener's pinKey: a dial whose client key does not match the
// pinned key mirrors a peer-key mismatch and never completes.
func (b *MemoryBroker) Dial(ctx context.Context, endpoint Endpoint, clientKey []byte) (Conn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	// Bound the connect attempt. If the caller gave the context a deadline,
	// abide by it; otherwise fall back to a modest default so a stalled
	// listener cannot hold a dialer forever.
	var timer *time.Timer
	if dl, ok := ctx.Deadline(); ok {
		timer = time.NewTimer(time.Until(dl))
	} else {
		timer = time.NewTimer(dialDefaultTimeout)
	}
	defer timer.Stop()

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, ErrMeshUnreachable
	}
	l := b.listeners[string(endpoint.Address)]
	b.mu.Unlock()
	if l == nil {
		return nil, ErrMeshUnreachable
	}
	if l.pinKey != nil && !bytes.Equal(l.pinKey, clientKey) {
		return nil, ErrPeerKeyMismatch
	}
	clientEnd, allocatorEnd := newConnectedPipe(clientKey, endpoint.PubKey)
	select {
	case l.incoming <- allocatorEnd:
		return clientEnd, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, ErrMeshUnreachable
	case <-l.closeCh:
		return nil, ErrListenerClosed
	}
}

// MemoryListener accepts connections dialed through a MemoryBroker. It is the
// in-memory allocator edge listener.
type MemoryListener struct {
	broker    *MemoryBroker
	address   []byte
	incoming  chan *memoryPipe
	closeCh   chan struct{}
	closeOnce sync.Once
	pinKey    []byte
}

// Accept implements Listener.
func (l *MemoryListener) Accept() (Conn, error) {
	select {
	case conn := <-l.incoming:
		return conn, nil
	case <-l.closeCh:
		return nil, ErrListenerClosed
	}
}

// Close implements Listener: it stops accepting and releases blocked Accepts.
func (l *MemoryListener) Close() error {
	l.closeOnce.Do(func() {
		l.broker.mu.Lock()
		delete(l.broker.listeners, string(l.address))
		l.broker.mu.Unlock()
		close(l.closeCh)
	})
	return nil
}

// MemoryDialer is a Dialer backed by a MemoryBroker, standing in for the client
// edge. It pins the peer key from the Endpoint at dial time.
type MemoryDialer struct {
	broker *MemoryBroker
	key    []byte // the local edge key the mesh authenticates with
}

// NewMemoryDialer returns an in-memory client-edge dialer with the given key.
func NewMemoryDialer(broker *MemoryBroker, key []byte) *MemoryDialer {
	return &MemoryDialer{broker: broker, key: append([]byte(nil), key...)}
}

// Dial implements Dialer.
func (d *MemoryDialer) Dial(ctx context.Context, endpoint Endpoint) (Conn, error) {
	return d.broker.Dial(ctx, endpoint, d.key)
}

// Close implements Dialer.
func (d *MemoryDialer) Close() error { return nil }

// MemoryEdge bundles a broker-side listener and dialer so the allocator and
// client edges can be constructed in tests with the same broker, mirroring the
// two-binary process topology (r1sd edge and r1s serve bridge).
type MemoryEdge struct {
	Broker *MemoryBroker
	Server *MemoryListener
	Client *MemoryDialer
}

// ErrMeshUnreachable reports that no remote edge is reachable at the endpoint.
var ErrMeshUnreachable = errors.New("tunnel mesh unreachable")

// dialDefaultTimeout is the connect bound for a MemoryBroker.Dial whose
// context carries no deadline. It is deliberately modest: the broker is test
// infrastructure, and a stalled listener should fail fast rather than hold a
// dialing goroutine open.
const dialDefaultTimeout = 2 * time.Second

// ErrListenerClosed reports that the allocator edge listener was closed.
var ErrListenerClosed = errors.New("tunnel listener closed")

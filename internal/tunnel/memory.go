package tunnel

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"time"
)

// byteQueue is an asynchronous, ordered, in-memory byte buffer with blocking
// reads, bounded writes, and a write-EOF flag for half-close. It is the
// backbone of the memory tunnel: the real overlay is unordered and unreliable
// at the packet layer, but the tunnel contract is a reliable ordered stream, so
// the fake implements that contract directly.
//
// maxBytes bounds the buffered bytes: a writer blocks while the buffer is full,
// so a fast producer cannot grow memory without bound and a stalled consumer
// exerts backpressure, mirroring a real stream buffer.
type byteQueue struct {
	mu       sync.Mutex
	cond     *sync.Cond
	buf      []byte
	maxBytes int
	writeEOF bool  // the writer half-closed: no more bytes will be appended
	aborted  error // the reader aborted (CloseRead); writers fail with this
}

func newByteQueue() *byteQueue {
	q := &byteQueue{maxBytes: 64 << 10}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// write appends b, blocking while the queue is at capacity (backpressure). It
// returns errWriteClosed if the writer already half-closed, or the peer's abort
// error if the peer aborted the read side.
func (q *byteQueue) write(b []byte) (int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.writeEOF {
		return 0, errWriteClosed
	}
	for len(q.buf) >= q.maxBytes && q.aborted == nil {
		q.cond.Wait()
	}
	if q.aborted != nil {
		return 0, q.aborted
	}
	if q.writeEOF {
		return 0, errWriteClosed
	}
	q.buf = append(q.buf, b...)
	q.cond.Broadcast()
	return len(b), nil
}

// read copies up to len(p) bytes, blocking until data is available, the writer
// half-closed with the buffer drained (returns io.EOF), or the reader aborted.
// When the queue was aborted with drain semantics (readAbortAfterDrain), any
// buffered bytes are returned first and the abort error surfaces only once the
// buffer is empty, honoring the ReasonCloser contract that a teardown reason is
// observable by the peer after buffered bytes are drained.
func (q *byteQueue) read(p []byte) (int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.buf) == 0 && !q.writeEOF && q.aborted == nil {
		q.cond.Wait()
	}
	if len(q.buf) > 0 {
		n := copy(p, q.buf)
		q.buf = q.buf[n:]
		q.cond.Broadcast()
		return n, nil
	}
	if q.aborted != nil {
		return 0, q.aborted
	}
	return 0, io.EOF
}

// writeClose sets the write-EOF flag: the reader drains the buffer then sees
// io.EOF. The writer cannot append afterward.
func (q *byteQueue) writeClose() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.writeEOF = true
	q.cond.Broadcast()
}

// readAbort aborts the read side (CloseRead): the buffer is discarded and
// writers fail with the given error, modeling a hard reset of the direction.
func (q *byteQueue) readAbort(err error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.aborted == nil {
		q.aborted = err
	}
	q.buf = nil
	q.cond.Broadcast()
}

// readAbortAfterDrain aborts the read side but keeps any buffered bytes: the
// reader drains them first and only then observes the abort error. Writers
// still fail immediately once the abort is set. Used by CloseWithReason so a
// teardown reason is observable by the peer after in-flight output is
// delivered.
func (q *byteQueue) readAbortAfterDrain(err error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.aborted == nil {
		q.aborted = err
	}
	q.cond.Broadcast()
}

var (
	errWriteClosed = errors.New("tunnel write side is closed")
)

// memoryPipe is a point-to-point reliable ordered byte pipe that implements
// Conn with per-direction half-close. It is the transport-neutral in-memory
// stand-in for the real overlay so the core, the protocol, the local API, and
// the CLI can be tested deterministically without a network library.
type memoryPipe struct {
	peer []byte // authenticated remote edge public key (opaque bytes)
	// Each direction is an independent byteQueue. This side writes to wq, which
	// the far side reads; this side reads from rq, which the far side writes.
	rq *byteQueue
	wq *byteQueue
}

// newConnectedPipe builds both ends of one tunnel so the client sees the
// allocator's key and the allocator sees the client's key.
func newConnectedPipe(clientKey, allocatorKey []byte) (*memoryPipe, *memoryPipe) {
	c2a := newByteQueue() // client -> allocator
	a2c := newByteQueue() // allocator -> client
	client := &memoryPipe{peer: append([]byte(nil), allocatorKey...), rq: a2c, wq: c2a}
	allocator := &memoryPipe{peer: append([]byte(nil), clientKey...), rq: c2a, wq: a2c}
	return client, allocator
}

// Read implements Conn.
func (p *memoryPipe) Read(b []byte) (int, error) { return p.rq.read(b) }

// Write implements Conn.
func (p *memoryPipe) Write(b []byte) (int, error) { return p.wq.write(b) }

// CloseWrite half-closes the write side: the far reader sees EOF once buffered
// bytes are drained. Independent of CloseRead.
func (p *memoryPipe) CloseWrite() error {
	p.wq.writeClose()
	return nil
}

// CloseRead stops reading: the far writer fails instead of seeing EOF. A
// subsequent Read on this side returns ErrReadAborted.
func (p *memoryPipe) CloseRead() error {
	// The read queue is where this side reads; aborting it makes the far side's
	// write (into this queue) fail.
	p.rq.readAbort(ErrReadAborted)
	return nil
}

// Close fully closes both directions.
func (p *memoryPipe) Close() error {
	_ = p.CloseRead()
	_ = p.CloseWrite()
	return nil
}

// PeerKey returns the authenticated remote edge public key.
func (p *memoryPipe) PeerKey() []byte { return append([]byte(nil), p.peer...) }

// CloseWithReason implements ReasonCloser: the allocator-side edge ends the
// session with a classified teardown outcome. The client's next Read observes
// a *SessionError instead of a bare EOF, so the relay can surface exactly why
// the session ended.
func (p *memoryPipe) CloseWithReason(reason Reason, detail string) error {
	// The peer reads the local write queue: abort it with drain semantics so any
	// already-buffered output is delivered before the reason surfaces on the
	// peer's Read (the ReasonCloser contract). The local read side ends so this
	// side stops reading.
	p.wq.readAbortAfterDrain(&SessionError{Reason: reason, Detail: detail})
	p.rq.readAbort(ErrReadAborted)
	return nil
}

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

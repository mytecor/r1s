package rns

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/Quad4-Software/Reticulum-Go/pkg/identity"
	"github.com/Quad4-Software/Reticulum-Go/pkg/link"
	rnstransport "github.com/Quad4-Software/Reticulum-Go/pkg/transport"
)

// StreamID is the single stream ID used for both directions on a tunnel
// Link's Channel. The one-Link-per-stream model means a single Link carries
// exactly one bidirectional byte stream. Both tunnel edges are symmetric and
// use the same newConn construction, so each side must receive on the ID the
// peer sends on; a single shared ID satisfies that on both ends. Channel data
// is transmitted to the remote endpoint only (never echoed to the local
// reader), so a shared ID cannot loop a writer's own messages back into its
// own reader.
const streamID = 1

// Conn is a reliable, ordered, bidirectional byte pipe between a client edge
// and the allocator edge over one tunnel RNS Link. It is the narrowed tunnel
// stream contract (F21-06 shapes internal/tunnel.Conn to this): a plain
// io.ReadWriteCloser plus CloseWrite for half-close propagation. It uses only
// stock Reticulum-Go Channel and Buffer primitives; there is no custom mux,
// framing, stream ID allocation, segmentation, or per-stream flow control.
//
// All methods are safe for concurrent use: the serve relay reads and writes
// the same Conn from different goroutines.
type Conn interface {
	io.ReadWriteCloser
	// CloseWrite half-closes the write side: the peer sees EOF on its read
	// side, and no further bytes are sent. Independent of Close.
	CloseWrite() error
	// RemoteIdentity returns the verified remote RNS identity hash (the other
	// side's persistent identity, established via Link.Identify()). It is
	// available once the Link has been identified on both ends; it is nil
	// before then.
	RemoteIdentity() []byte
}

// conn is the Conn implementation wrapping a tunnel Link's Channel and Buffer.
// One Link maps to exactly one stream: a Dialer/Acceptor creates one of these
// per established Link.
type conn struct {
	link     *link.Link
	stream   *reticulumCompatStream
	remoteID atomicBytes // verified remote identity hash

	// identified is signalled (closed) when the remote identity is captured.
	identified     chan struct{}
	identifiedOnce sync.Once
	// remoteIdentified, when non-nil, is invoked with the verified remote
	// identity once it is captured, whether that happens via the Link's
	// identified callback or via awaitIdentification's poll of the Link's
	// recorded identity.
	remoteIdentified func([]byte)
	closed           chan struct{}
	closedOnce       sync.Once

	closeOnce sync.Once
	closeErr  error
}

var _ Conn = (*conn)(nil)

// atomicBytes stores a byte slice read/written atomically.
type atomicBytes struct {
	mu sync.RWMutex
	v  []byte
}

func (a *atomicBytes) set(v []byte) {
	a.mu.Lock()
	a.v = append(a.v[:0], v...)
	a.mu.Unlock()
}

func (a *atomicBytes) get() []byte {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return append([]byte(nil), a.v...)
}

// newConn wraps an established tunnel Link's Channel in the one-stream Buffer
// and returns the Conn. The Buffer uses CreateBidirectionalBuffer: one reader
// and one writer over the Link's channel, with EOF carried by a terminal
// StreamDataMessage (half-close). remoteIdentified, when non-nil, is invoked
// with the verified remote identity when the peer calls Link.Identify().
//
// The Conn attaches itself to the transport's registered Link object for the
// link ID (transport.FindLink), not necessarily the link instance the
// established callback delivered. Reticulum-Go may deliver a different *Link
// pointer to the application than the one it registers in its link table, and
// inbound Channel data is routed to the registered object. Attaching the
// Channel and teardown to the registered Link guarantees the Conn reads and
// writes the same channel the transport feeds.
func newConn(transport *rnstransport.Transport, rnsLink *link.Link, remoteIdentified func([]byte)) (*conn, error) {
	preparedLink, stream, err := tunnelReticulumCompat.prepareConn(transport, rnsLink)
	if err != nil {
		return nil, err
	}
	rnsLink = preparedLink
	c := &conn{
		link:             rnsLink,
		stream:           stream,
		identified:       make(chan struct{}),
		remoteIdentified: remoteIdentified,
		closed:           make(chan struct{}),
	}
	rnsLink.SetRemoteIdentifiedCallback(func(_ *link.Link, remote *identity.Identity) {
		c.capture(remote)
	})
	rnsLink.SetLinkClosedCallback(func(*link.Link) {
		c.signalClosed()
	})
	return c, nil
}

// capture records the verified remote identity and signals identification. It
// is the single place the Conn learns who the peer is, reachable from either
// the Link's identified callback or awaitIdentification's poll of the Link's
// recorded identity, so both fast-path and late-registration identification
// converge on the same state.
func (c *conn) capture(remote *identity.Identity) {
	if remote == nil {
		return
	}
	c.remoteID.set(remote.Hash())
	if c.remoteIdentified != nil {
		c.remoteIdentified(remote.Hash())
	}
	c.identifiedOnce.Do(func() {
		close(c.identified)
	})
}

// Read reads from the Link's Buffer. It returns io.EOF when the peer has
// half-closed (CloseWrite) this direction and the buffer is drained.
func (c *conn) Read(p []byte) (int, error) {
	return c.stream.Read(p, c.closed)
}

// Write sends and flushes bytes through the compatibility-isolated stock
// Channel/Buffer stream.
func (c *conn) Write(p []byte) (int, error) {
	return c.stream.Write(p)
}

// CloseWrite half-closes the write side: it sends a terminal EOF-marked
// Channel message so the peer's read side sees io.EOF. It does not tear down
// the Link.
func (c *conn) CloseWrite() error {
	return c.stream.CloseWrite()
}

// Close tears down the whole tunnel Link, ending both directions. It is safe
// to call concurrently with Read/Write/CloseWrite.
func (c *conn) Close() error {
	c.closeOnce.Do(func() {
		c.signalClosed()
		c.link.Teardown()
	})
	return c.closeErr
}

func (c *conn) signalClosed() {
	c.closedOnce.Do(func() {
		close(c.closed)
	})
}

// RemoteIdentity returns the verified remote identity hash.
func (c *conn) RemoteIdentity() []byte { return c.remoteID.get() }

// awaitIdentification blocks until the remote identity has been captured (the
// peer called Link.Identify) or the context is done. It lets Accept/Dial
// return a Conn that is fully identified before the caller proceeds. It
// returns before the identification if a nil remote identity was reported but
// the caller supplied a fallback via identifyFrom; that is handled by the
// edge, not here.
func (c *conn) awaitIdentification(ctx context.Context) error {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-c.identified:
			if len(c.remoteID.get()) == 0 {
				return errors.New("tunnel RNS link closed without identification")
			}
			return nil
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		// Reticulum's HandleIdentification records the verified remote identity
		// regardless of whether the identified callback was registered when the
		// identification arrived. The peer's Identify may be processed before
		// Accept/Dial's newConn registers the callback (the registered callback
		// is captured at HandleIdentification time, and a late registration is
		// dropped by its once-only guard), so the callback alone is not a
		// reliable completion signal. Poll the Link's recorded identity to
		// close that race: whichever of the callback or this poll observes the
		// identity first completes identification.
		if remote := c.link.GetRemoteIdentity(); remote != nil {
			c.capture(remote)
			return nil
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

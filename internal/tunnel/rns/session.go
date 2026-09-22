package rns

import (
	"bufio"
	"context"
	"errors"
	"io"
	"sync"

	"github.com/Quad4-Software/Reticulum-Go/pkg/buffer"
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
	rw       *buffer.Buffer
	rawW     *buffer.RawChannelWriter
	remoteID atomicBytes // verified remote identity hash

	// identified is signalled (closed) when the remote identity is captured.
	identified chan struct{}

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
	if registered, ok := transport.FindLink(rnsLink.GetLinkID()).(*link.Link); ok && registered != nil {
		rnsLink = registered
	}
	ch := rnsLink.GetChannel()
	// Build the reader and writer directly so the Conn can reach the raw
	// channel writer for CloseWrite (EOF-marked terminal message).
	rawR := buffer.NewRawChannelReader(streamID, ch)
	rawW := buffer.NewRawChannelWriter(streamID, ch)
	reader := bufio.NewReader(rawR)
	writer := bufio.NewWriter(rawW)
	rw := &buffer.Buffer{ReadWriter: bufio.NewReadWriter(reader, writer)}
	c := &conn{link: rnsLink, rw: rw, rawW: rawW, identified: make(chan struct{})}
	rnsLink.SetRemoteIdentifiedCallback(func(_ *link.Link, remote *identity.Identity) {
		if remote == nil {
			return
		}
		c.remoteID.set(remote.Hash())
		if remoteIdentified != nil {
			remoteIdentified(remote.Hash())
		}
		select {
		case <-c.identified:
		default:
			close(c.identified)
		}
	})
	return c, nil
}

// Read reads from the Link's Buffer. It returns io.EOF when the peer has
// half-closed (CloseWrite) this direction and the buffer is drained.
func (c *conn) Read(p []byte) (int, error) {
	return c.rw.Read(p)
}

// Write writes through the Buffer to the Link's Channel. Payloads larger than
// the RNS MTU are carried as stock Channel messages; no application framing is
// injected.
func (c *conn) Write(p []byte) (int, error) {
	return c.rw.Write(p)
}

// CloseWrite half-closes the write side: it sends a terminal EOF-marked
// Channel message so the peer's read side sees io.EOF. It does not tear down
// the Link. buffer.Buffer.Close only flushes; the EOF-marked message is sent
// by the raw channel writer's Close, which sets its EOF flag and transmits a
// terminal zero-length StreamDataMessage.
func (c *conn) CloseWrite() error {
	if err := c.rw.ReadWriter.Writer.Flush(); err != nil {
		return err
	}
	return c.rawW.Close()
}

// Close tears down the whole tunnel Link, ending both directions. It is safe
// to call concurrently with Read/Write/CloseWrite.
func (c *conn) Close() error {
	c.closeOnce.Do(func() {
		_ = c.rawW.Close()
		c.link.Teardown()
	})
	return c.closeErr
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
	select {
	case <-c.identified:
		if len(c.remoteID.get()) == 0 {
			return errors.New("tunnel RNS link closed without identification")
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

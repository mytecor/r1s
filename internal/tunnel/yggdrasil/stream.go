package yggdrasil

import (
	"errors"
	"io"
	"net"
	"sync"
	"time"

	iwt "github.com/Arceliar/ironwood/types"

	"github.com/mytecor/r1s/internal/tunnel"
)

// streamConn is one tunnel session: a reliable ordered byte stream between two
// embedded nodes, carried over the yggdrasil packet interface. It adapts
// packets to the tunnel.Conn byte-stream contract:
//
//   - segmentation: Write splits bytes into frames no larger than one packet
//     (the packet MTU), so no write ever exceeds the mesh MTU;
//   - reassembly: frames arrive through ironwood's per-peer ordered queue, so
//     concatenating frame payloads in arrival order rebuilds the stream;
//   - half-close: a write-EOF frame ends the sender's write direction; the
//     peer drains buffered bytes then sees io.EOF (CloseWrite), and CloseRead
//     aborts the local read side independently;
//   - classified teardown: a close frame carries a tunnel.Reason so the peer's
//     Read observes a *tunnel.SessionError after buffered bytes are drained,
//     honoring the ReasonCloser contract across the wire.
//
// Packets are demultiplexed to this stream by the edge mux (mux.go): the mux
// is the only reader of the node's packet interface and feeds every packet
// bound to this session's remote key into ingest. Backpressure: ingest drops
// into a bounded wait when the decoded inbox is over the high-water mark, so
// a stalled consumer throttles the mesh instead of growing memory without
// limit.
type streamConn struct {
	peer []byte // authenticated remote edge node public key (opaque bytes)
	pkt  packetIO
	// remote is the packet-layer destination for writes: ironwood's types.Addr
	// is the remote's ed25519 public key, the same opaque bytes the grant's
	// endpoint advertisement carries.
	remote iwt.Addr

	maxSeg int // max payload bytes per packet

	// writeScratch is the reusable payload frame-assembly buffer, guarded by
	// mu. It is used only by Write (the sole payload writer, single-goroutine
	// under the splice); control frames (writeFrame) assemble into their own
	// per-call buffer so a concurrent teardown never races the payload path
	// on this shared buffer.
	writeScratch []byte

	// owner is the mux that demultiplexes this session's packets; its remove
	// hook is called exactly once when the session ends. Nil in tests.
	owner *mux

	mu         sync.Mutex
	raw        []byte // received packet bytes not yet decoded (handshake only)
	inbox      []byte // decoded, not yet consumed stream bytes
	eof        bool   // peer half-closed its write side: EOF after drain
	peerErr    error  // classified peer teardown (a *tunnel.SessionError)
	closed     bool   // local full close
	readDone   bool   // local read side aborted (CloseRead)
	writeEOF   bool   // local write side half-closed (CloseWrite)
	preambleOK bool   // the routing preamble was sent (client) or accepted (allocator)
	accepted   bool   // the allocator's accept frame arrived (client side)

	// cond wakes Read and readFrame when stream state changes.
	cond *sync.Cond

	// stopped is closed when the session ends; frameReady wakes a readFrame
	// wait when a handshake packet arrived.
	stopped    chan struct{}
	frameReady chan struct{}

	closeOnce sync.Once
}

// packetIO is the packet interface the stream runs over: the embedded
// yggdrasil node in production (its Core satisfies it via ReadFrom/WriteTo),
// or a fake in tests.
type packetIO interface {
	ReadFrom(p []byte) (n int, from net.Addr, err error)
	WriteTo(p []byte, addr net.Addr) (n int, err error)
	MTU() uint64
	Close() error
}

// ingest decodes one packet into the stream state. The embedded Core's
// MTU (ironwood default peerMaxMessageSize minus overheads) is far larger; the
// adapter clamps to a conservative value so one frame always fits one packet
// with room for mesh overhead, and mux read buffers are this size.
const maxPacketPayload = 16 * 1024

// readHighWater bounds decoded-but-unconsumed stream bytes per session: the
// mux pauses feeding a session above this mark, so a stalled consumer
// throttles the mesh instead of growing memory without bound.
const readHighWater = 4 * maxPacketPayload

// newStreamConn wires a stream to a packet interface. The mux registers the
// session before any packet is routed to it; owner.remove unregisters it on
// end.
func newStreamConn(pkt packetIO, remote []byte, peerKey []byte, owner *mux) *streamConn {
	maxSeg := int(pkt.MTU())
	if maxSeg <= 0 || maxSeg > maxPacketPayload {
		maxSeg = maxPacketPayload
	}
	c := &streamConn{
		peer:       append([]byte(nil), peerKey...),
		pkt:        pkt,
		remote:     iwt.Addr(append([]byte(nil), remote...)),
		maxSeg:     maxSeg,
		stopped:    make(chan struct{}),
		frameReady: make(chan struct{}, 1),
		owner:      owner,
	}
	c.cond = sync.NewCond(&c.mu)
	return c
}

// ingest decodes one packet into the stream state. Called by the mux for
// every packet routed to this session. A malformed frame ends the session
// instead of desynchronizing the stream.
func (c *streamConn) ingest(packet []byte) {
	if tracePackets {
		println("ingest", packet[0], len(packet))
	}
	f, _, ok, err := parseFrame(packet)
	if err != nil || !ok {
		if err == nil {
			err = &frameError{"short frame"}
		}
		c.end(&tunnel.SessionError{Reason: tunnel.ReasonSessionFailed, Detail: err.Error()})
		return
	}
	c.mu.Lock()
	switch f.typ {
	case frameTypePreamble, frameTypeAccept:
		// Handshake frames are consumed by readFrame on the dial/accept path
		// before the payload pump starts; arrival here after the handshake
		// means the peer is misbehaving. Stash into raw so readFrame (if
		// still waiting) sees them. After the handshake the pump no longer
		// reads raw, so a misbehaving peer must not be able to grow it
		// without bound: past the cap the session ends as failed instead.
		if len(c.raw) >= maxPacketPayload {
			c.mu.Unlock()
			c.end(&tunnel.SessionError{Reason: tunnel.ReasonSessionFailed, Detail: "stray handshake frame flood"})
			return
		}
		c.raw = append(c.raw, packet...)
		c.cond.Broadcast()
		c.mu.Unlock()
		select {
		case c.frameReady <- struct{}{}:
		default:
		}
		return
	case frameTypeData:
		if len(f.payload) == 0 {
			c.mu.Unlock()
			c.end(&tunnel.SessionError{Reason: tunnel.ReasonSessionFailed, Detail: "empty data frame"})
			return
		}
		// Backpressure: wait while the inbox is over the high-water mark.
		for len(c.inbox) >= readHighWater && !c.readDone && !c.closed && c.peerErr == nil {
			c.cond.Wait()
		}
		if c.closed || c.readDone {
			c.mu.Unlock()
			return
		}
		c.inbox = append(c.inbox, f.payload...)
	case frameTypeEOF:
		c.eof = true
	case frameTypeClose:
		if c.peerErr == nil && len(f.payload) > 0 {
			c.peerErr = parseClosePayload(f.payload)
		}
	}
	c.cond.Broadcast()
	c.mu.Unlock()
}

// Read implements tunnel.Conn. It blocks until bytes are available, the peer
// half-closed (io.EOF after drain), the peer tore the session down with a
// classified reason (a *tunnel.SessionError), or the local side closed.
func (c *streamConn) Read(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		if c.readDone {
			return 0, tunnel.ErrReadAborted
		}
		if len(c.inbox) > 0 {
			n := copy(p, c.inbox)
			c.inbox = c.inbox[n:]
			c.cond.Broadcast() // room freed; a backpressured ingest may resume
			return n, nil
		}
		if c.peerErr != nil {
			err := c.peerErr
			c.mu.Unlock()
			c.end(nil)
			c.mu.Lock()
			return 0, err
		}
		if c.eof {
			return 0, io.EOF
		}
		if c.closed {
			return 0, tunnel.ErrReadAborted
		}
		c.cond.Wait()
	}
}

// Write sends p as one or more frames, splitting at the packet MTU. A write
// after the peer reported a teardown (close frame) or after a local close or
// half-close fails instead of buffering into a dead session. On the client
// side, a payload Write before WritePreamble fails: the routing preamble must
// be the first frame of the session.
func (c *streamConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	if c.closed || c.writeEOF {
		c.mu.Unlock()
		return 0, errors.New("tunnel write side is closed")
	}
	if !c.preambleOK && c.owner != nil {
		// The client must send the routing preamble before any payload; the
		// allocator edge sets preambleOK when it promotes the session, and the
		// client sets it inside WritePreamble.
		c.mu.Unlock()
		return 0, errors.New("tunnel: the routing preamble must be written first")
	}
	if c.peerErr != nil {
		err := c.peerErr
		c.mu.Unlock()
		return 0, err
	}
	total := 0
	for total < len(p) {
		end := total + c.maxSeg
		if end > len(p) {
			end = len(p)
		}
		frameBytes := appendFrame(c.scratch(), frameTypeData, p[total:end])
		c.mu.Unlock()
		if _, err := c.pkt.WriteTo(frameBytes, c.remote); err != nil {
			return total, err
		}
		total = end
		c.mu.Lock()
		if c.closed || c.peerErr != nil {
			c.mu.Unlock()
			return total, errors.New("tunnel write side is closed")
		}
	}
	c.mu.Unlock()
	return total, nil
}

// writeFrame writes one control frame (EOF, close, preamble, accept). The
// payload must be small (all control payloads are); Write segments payload on
// its own.
func (c *streamConn) writeFrame(typ byte, payload []byte) error {
	c.mu.Lock()
	if c.closed || c.writeEOF {
		c.mu.Unlock()
		return errors.New("tunnel write side is closed")
	}
	// Assemble into a per-call buffer under mu, never the shared
	// c.writeScratch owned by Write: a teardown (end/CloseWithReason) can run
	// concurrently with a payload Write, and a shared assembly buffer would
	// race. Control payloads are small, so the per-call allocation is cheap
	// and not on the hot path.
	frame := appendFrame(make([]byte, 0, frameHeaderSize+len(payload)), typ, payload)
	c.mu.Unlock()
	_, err := c.pkt.WriteTo(frame, c.remote)
	return err
}

// scratch returns the reusable frame-assembly buffer. Callers hold mu.
func (c *streamConn) scratch() []byte {
	if cap(c.writeScratch) < frameHeaderSize+c.maxSeg {
		c.writeScratch = make([]byte, 0, frameHeaderSize+c.maxSeg)
	}
	return c.writeScratch[:0]
}

// CloseRead aborts the read side: buffered inbound bytes are discarded and
// local state stops accepting inbound data. The mesh cannot signal read-abort
// to the peer, so the session ends for the peer as a failed write; a later
// Read on this side returns ErrReadAborted.
func (c *streamConn) CloseRead() error {
	c.mu.Lock()
	c.readDone = true
	c.inbox = nil
	c.raw = nil
	c.cond.Broadcast()
	c.mu.Unlock()
	return nil
}

// CloseWrite half-closes the write side: the peer drains buffered bytes then
// sees io.EOF. Independent of CloseRead.
func (c *streamConn) CloseWrite() error {
	return c.writeFrame(frameTypeEOF, nil)
}

// Close fully closes the session without notifying the peer (a local abort:
// the peer's Write fails once its writes are no longer consumed). Prefer
// CloseWithReason for classified teardowns.
func (c *streamConn) Close() error {
	c.end(nil)
	return nil
}

// CloseWithReason implements tunnel.ReasonCloser: the session ends with a
// classified teardown outcome that the peer observes on its next Read after
// buffered bytes are delivered (a close frame goes out first, then the local
// shutdown).
func (c *streamConn) CloseWithReason(reason tunnel.Reason, detail string) error {
	c.end(&tunnel.SessionError{Reason: reason, Detail: detail})
	return nil
}

// PeerKey returns the authenticated remote edge node public key.
func (c *streamConn) PeerKey() []byte { return append([]byte(nil), c.peer...) }

// end tears the session down, optionally notifying the peer with a classified
// reason first, and unregisters the session from the mux.
func (c *streamConn) end(peerErr error) {
	c.closeOnce.Do(func() {
		if sessionErr, ok := peerErr.(*tunnel.SessionError); ok && sessionErr != nil {
			// Best effort: tell the peer why the session ended before the
			// packet interface goes away.
			_ = c.writeFrame(frameTypeClose, closePayload(sessionErr.Reason, sessionErr.Detail))
		}
		c.mu.Lock()
		c.closed = true
		c.cond.Broadcast()
		c.mu.Unlock()
		close(c.stopped)
		if c.owner != nil {
			c.owner.remove(string(c.remote))
		}
	})
}

// readFrame reads one complete frame with a deadline. It is used only by the
// handshake paths (dial waits for the accept frame, accept reads the
// preamble), never on the payload path. The handshake runs before the mux
// feeds the session's pump, so packets are staged in raw here.
func (c *streamConn) readFrame(timeout time.Duration) (frame, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	// Long-lived ticker for the re-check window; see the select below.
	poll := time.NewTicker(50 * time.Millisecond)
	defer poll.Stop()
	for {
		c.mu.Lock()
		f, consumed, ok, err := parseFrame(c.raw)
		if err != nil {
			c.mu.Unlock()
			return frame{}, err
		}
		if ok {
			c.raw = c.raw[consumed:]
			c.mu.Unlock()
			return f, nil
		}
		if c.peerErr != nil {
			peerErr := c.peerErr
			c.mu.Unlock()
			return frame{}, peerErr
		}
		if c.eof || c.closed {
			c.mu.Unlock()
			return frame{}, io.EOF
		}
		c.mu.Unlock()
		select {
		case <-c.stopped:
			return frame{}, io.EOF
		case <-timer.C:
			return frame{}, &frameError{"handshake frame timeout"}
		case <-poll.C:
			// Re-check; packets arrive through ingest (the mux) which appends
			// to raw and signals frameReady. A per-iteration time.After would
			// allocate one timer per short window, so a long-lived poll
			// ticker drives the re-check instead.
		case <-c.frameReady:
		}
	}
}

// WritePreamble implements tunnel.PreambleWriter: it sends the one-time
// routing preamble as the first frame of the session and waits for the
// allocator's accept frame. A rejected or unauthenticated session fails here
// with a classified reason, before a single payload byte is sent; after a
// successful return, payload Write is allowed and the splice is live.
//
// The first write triggers mesh path discovery, and a packet can be lost
// while the path is still being discovered (the mesh drops unroutable traffic
// silently). The preamble is safe to repeat until the session is promoted (an
// unpromoted pending session has consumed nothing), so the handshake retries
// the preamble on a fixed cadence until the accept (or close) frame arrives,
// bounded by maxHandshakeWait.
func (c *streamConn) WritePreamble(preamble tunnel.Preamble) error {
	c.mu.Lock()
	if c.closed || c.writeEOF {
		c.mu.Unlock()
		return errors.New("tunnel write side is closed")
	}
	if c.preambleOK {
		c.mu.Unlock()
		return errors.New("tunnel: the routing preamble was already written")
	}
	c.mu.Unlock()

	preamblePayload := encodePreamble(preamble)
	deadline := time.NewTimer(maxHandshakeWait)
	defer deadline.Stop()
	retry := time.NewTicker(300 * time.Millisecond)
	defer retry.Stop()
	for {
		if err := c.writeFrame(frameTypePreamble, preamblePayload); err != nil {
			return err
		}
		// Bounded wait for the accept frame between retries; a classified
		// rejection (a session error) is final and returns, an interim
		// timeout retries the preamble.
		err := c.awaitAcceptTimeout(300 * time.Millisecond)
		if err == nil {
			break
		}
		var sessionErr *tunnel.SessionError
		if errors.As(err, &sessionErr) || errors.Is(err, io.EOF) {
			return err
		}
		select {
		case <-c.stopped:
			return errors.New("tunnel: session ended during the handshake")
		case <-retry.C:
		case <-deadline.C:
			return errors.New("tunnel: the routing preamble was not accepted in time")
		}
	}
	c.mu.Lock()
	c.preambleOK = true
	c.mu.Unlock()
	return nil
}

// awaitAcceptTimeout waits for the allocator's accept frame within one short
// window. A close frame instead of an accept carries the classified rejection
// reason. A bare interim timeout is indistinguishable from "no frame yet" and
// is reported as an interim error the retry loop understands.
func (c *streamConn) awaitAcceptTimeout(wait time.Duration) error {
	f, err := c.readFrame(wait)
	if err != nil {
		return err
	}
	switch f.typ {
	case frameTypeAccept:
		return nil
	case frameTypeClose:
		return parseClosePayload(f.payload)
	default:
		return errors.New("tunnel: allocator did not accept the preamble")
	}
}

// readPreamble reads the one-time routing preamble from a fresh inbound
// session. The preamble frame must be the first frame; anything else is a
// protocol violation. Used by the allocator edge's Accept.
func (c *streamConn) readPreamble() (tunnel.Preamble, error) {
	f, err := c.readFrame(maxHandshakeWait)
	if err != nil {
		return tunnel.Preamble{}, err
	}
	if f.typ != frameTypePreamble {
		return tunnel.Preamble{}, errors.New("tunnel: the first frame must be the routing preamble")
	}
	return decodePreamble(f.payload)
}

// writeAccept confirms the allocator accepted the grant and preamble. Written
// after validation, before any payload is relayed, so a rejected session
// never carries payload; the client's handshake fails instead.
func (c *streamConn) writeAccept() error {
	return c.writeFrame(frameTypeAccept, nil)
}

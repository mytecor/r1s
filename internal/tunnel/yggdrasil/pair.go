package yggdrasil

import (
	"errors"
	"io"
	"sync"
	"time"

	iwt "github.com/Arceliar/ironwood/types"

	"github.com/mytecor/r1s/internal/tunnel"
)

// pairConn is one authenticated mesh connection between two node keys (F19-01):
// the "tunnelMux" that carries many logical streams with distinct stream IDs.
// Session identity is the remote node key (Model A): one live pair per pair of
// node keys, and the allocator registry enforces one live session per execution
// at accept. The pair owns the one-time routing preamble handshake and the
// per-stream registration; every stream on the pair is an independent tunnel.Conn.
//
// Two roles:
//
//   - Client edge: the caller registers a pair (mux.register), writes the
//     routing preamble through WritePreamble, waits for the allocator's accept
//     frame, then opens one or more streams with OpenStream. Each stream
//     carries an optional target_slot in its stream-open frame.
//   - Allocator edge: mux.accept returns a pending pair whose preamble has
//     arrived. The allocator core validates it (AcceptTunnel -> a Session with
//     the slot list), then Authorize sets the slot list and sends the accept
//     frame. From then on the pair opens allocator-side streams for authorized
//     stream-open frames and pushes them (with their resolved target) to
//     AcceptStream, so the r1sd edge can splice each stream independently.
//
// Packets arrive through the pair's owner mux, which routes a decoded frame to
// the targeted stream without holding a pair-wide lock while a slow stream is
// fed, so the pair readloop never blocks on a slow stream.
type pairConn struct {
	pkt    packetIO
	peer   []byte // authenticated remote edge node public key (opaque bytes)
	remote iwt.Addr
	maxSeg int
	owner  *mux

	mu       sync.Mutex
	streams  map[uint32]*streamConn
	nextID   uint32
	closed   bool
	closeErr error
	// session is the allocator-validated session (with the slot list) set by
	// Authorize; nil on the client edge.
	session *tunnel.Session
	// clientReady is set once the allocator's accept frame arrived, so the
	// client edge may open streams.
	clientReady bool
	// defaultStream is the client-side interactive-pipe stream opened right
	// after the preamble is accepted. The pair itself implements tunnel.Conn by
	// delegating to it, so Dial returns a value usable as a byte pipe (backward
	// compatible with F14) and as a StreamOpener (F19).
	defaultStream *streamConn

	// incoming carries allocator-side streams, each with its resolved target,
	// for the r1sd edge to splice. Buffered; a slow splice never blocks the
	// pair readloop.
	incoming chan *IncomingStream

	// frameReady wakes a blocked readPairFrame when a handshake packet arrived.
	frameReady chan struct{}

	// stopped is closed when the pair ends; cond wakes handshake readers.
	stopped   chan struct{}
	cond      *sync.Cond
	closeOnce sync.Once

	// raw accumulates pair-level handshake bytes (preamble/accept) until the
	// handshake reader consumes them.
	raw []byte
}

// IncomingStream is one allocator-side stream, authorized and ready to splice.
// It carries the resolved target slot the edge splices to and the byte stream.
type IncomingStream struct {
	Conn   tunnel.Conn
	Target tunnel.Target
	ID     uint32
}

// maxIncomingStreams bounds the allocator-side stream splice queue per pair, so
// a burst of stream-opens that the edge cannot yet splice does not grow memory
// without bound.
const maxIncomingStreams = 64

// newPair wires a pair to a packet interface. The mux registers the pair before
// any packet is routed to it; owner.removePair unregisters it on close.
func newPair(pkt packetIO, remote []byte, peerKey []byte, owner *mux) *pairConn {
	maxSeg := int(pkt.MTU()) - frameHeaderSize
	if maxSeg <= 0 || maxSeg > maxPacketPayload {
		maxSeg = maxPacketPayload
	}
	p := &pairConn{
		pkt:        pkt,
		peer:       append([]byte(nil), peerKey...),
		remote:     iwt.Addr(append([]byte(nil), remote...)),
		maxSeg:     maxSeg,
		owner:      owner,
		streams:    make(map[uint32]*streamConn),
		nextID:     1,
		incoming:   make(chan *IncomingStream, maxIncomingStreams),
		frameReady: make(chan struct{}, 1),
		stopped:    make(chan struct{}),
	}
	p.cond = sync.NewCond(&p.mu)
	return p
}

// PeerKey returns the authenticated remote edge node public key.
func (p *pairConn) PeerKey() []byte { return append([]byte(nil), p.peer...) }

// defaultConn returns the client-side default stream, or nil if none was
// opened yet (the pair is the allocator role, or the preamble was not written).
func (p *pairConn) defaultConn() *streamConn {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.defaultStream
}

// Read delegates to the default stream (client-side interactive pipe).
func (p *pairConn) Read(b []byte) (int, error) {
	if s := p.defaultConn(); s != nil {
		return s.Read(b)
	}
	return 0, errors.New("tunnel: no default stream; the pair preamble was not written")
}

// Write delegates to the default stream (client-side interactive pipe).
func (p *pairConn) Write(b []byte) (int, error) {
	if s := p.defaultConn(); s != nil {
		return s.Write(b)
	}
	return 0, errors.New("tunnel: no default stream; the pair preamble was not written")
}

// CloseRead delegates to the default stream.
func (p *pairConn) CloseRead() error {
	if s := p.defaultConn(); s != nil {
		return s.CloseRead()
	}
	return nil
}

// CloseWrite delegates to the default stream.
func (p *pairConn) CloseWrite() error {
	if s := p.defaultConn(); s != nil {
		return s.CloseWrite()
	}
	return nil
}

// Close fully closes the pair and every stream on it.
func (p *pairConn) Close() error {
	p.endAll(&tunnel.SessionError{Reason: tunnel.ReasonClosed})
	return nil
}

// CloseWithReason implements tunnel.ReasonCloser by tearing down the whole pair
// with a classified reason (a full mesh connection teardown).
func (p *pairConn) CloseWithReason(reason tunnel.Reason, detail string) error {
	p.endAll(&tunnel.SessionError{Reason: reason, Detail: detail})
	return nil
}

// OpenStream implements tunnel.StreamOpener: it opens a new logical stream on
// this pair for the given container port and returns its byte pipe. The pair is
// registered as a tunnel.Conn that also multiplexes, so the returned value is a
// separate stream carrying its own port.
func (p *pairConn) OpenStream(targetPort uint16) (tunnel.Conn, error) {
	return p.openStream(targetPort)
}

// openStream is the client-side stream opener. See OpenStream's doc.
func (p *pairConn) openStream(targetPort uint16) (tunnel.Conn, error) {
	p.mu.Lock()
	if p.closed || !p.clientReady {
		p.mu.Unlock()
		return nil, errors.New("tunnel: the pair is not ready to open streams")
	}
	id := p.nextID
	p.nextID++
	s := newStream(p, id)
	p.streams[id] = s
	p.mu.Unlock()
	if err := s.sendOpen(targetPort); err != nil {
		p.remove(id)
		return nil, err
	}
	return s, nil
}

// writeFrame sends one frame on this pair. It is safe on a closed pair: the mux
// tears everything down on close, and a write to a closed packet interface is a
// best-effort teardown signal.
func (p *pairConn) writeFrame(typ byte, streamID uint32, payload []byte) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrEdgeClosed
	}
	frame := appendFrame(make([]byte, 0, frameHeaderSize+len(payload)), typ, streamID, payload)
	p.mu.Unlock()
	_, err := p.pkt.WriteTo(frame, p.remote)
	return err
}

// ingest decodes one packet and routes the frame to the pair-level handshake or
// the targeted stream. It is called by the mux readloop and never blocks on a
// stream, so a slow stream cannot stall the pair's other streams.
func (p *pairConn) ingest(packet []byte) {
	f, _, ok, err := parseFrame(packet)
	if err != nil || !ok {
		if err == nil {
			err = &frameError{"short frame"}
		}
		p.endAll(&tunnel.SessionError{Reason: tunnel.ReasonSessionFailed, Detail: err.Error()})
		return
	}
	// Pair-level handshake frames.
	if f.streamID == streamIDNone {
		p.ingestPairFrame(f)
		return
	}
	// The first frame of a new stream is the stream-open header.
	if f.typ == frameTypeStreamOpen {
		p.openAllocatorStream(f)
		return
	}
	p.mu.Lock()
	stream := p.streams[f.streamID]
	p.mu.Unlock()
	if stream == nil {
		// A frame for a stream this side does not know: the peer opened it and
		// already closed it, or is misbehaving. Ignore data for unknown streams.
		return
	}
	stream.ingest(f)
}

// ingestPairFrame handles a preamble/accept frame at the pair level. These
// arrive only during the one-time handshake; a rogue peer trying to flood them
// is bounded by the raw cap.
func (p *pairConn) ingestPairFrame(f frame) {
	if f.typ == frameTypeClose {
		p.terminate(parseClosePayload(f.payload), false)
		return
	}
	p.mu.Lock()
	switch f.typ {
	case frameTypePreamble, frameTypeAccept:
		if len(p.raw) >= maxPacketPayload {
			p.mu.Unlock()
			p.endAll(&tunnel.SessionError{Reason: tunnel.ReasonSessionFailed, Detail: "stray handshake frame flood"})
			return
		}
		p.raw = append(p.raw, appendFrame(nil, f.typ, f.streamID, f.payload)...)
		p.cond.Broadcast()
	}
	p.mu.Unlock()
	select {
	case p.frameReady <- struct{}{}:
	default:
	}
}

// readPairFrame reads one pair-level frame with a deadline. Used only by the
// handshake paths (client waits for accept, allocator reads preamble).
func (p *pairConn) readPairFrame(timeout time.Duration) (frame, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	poll := time.NewTicker(50 * time.Millisecond)
	defer poll.Stop()
	for {
		p.mu.Lock()
		f, consumed, ok, err := parseFrame(p.raw)
		if err != nil {
			p.mu.Unlock()
			return frame{}, err
		}
		if ok {
			p.raw = p.raw[consumed:]
			p.mu.Unlock()
			return f, nil
		}
		if p.closed {
			err := p.closeErr
			p.mu.Unlock()
			return frame{}, err
		}
		p.mu.Unlock()
		select {
		case <-p.stopped:
			// Recheck the stored close reason under the lock.
			continue
		case <-timer.C:
			return frame{}, &frameError{"handshake frame timeout"}
		case <-poll.C:
			// Re-check under the lock; packets arrive through ingest which
			// appends to raw and signals frameReady.
		case <-p.frameReady:
		}
	}
}

// WritePreamble implements the client-side one-time routing handshake for the
// pair, mirroring the F14 session preamble but at the mesh-connection level. It
// sends the routing preamble as the pair's first frame and waits for the
// allocator's accept frame, so a rejected or unauthenticated session fails here
// before a single stream (and thus a payload byte) is opened. It is safe to
// repeat until accepted, bounded by maxHandshakeWait.
func (p *pairConn) WritePreamble(preamble tunnel.Preamble) error {
	preamblePayload := encodePreamble(preamble)
	deadline := time.NewTimer(maxHandshakeWait)
	defer deadline.Stop()
	retry := time.NewTicker(300 * time.Millisecond)
	defer retry.Stop()
	for {
		if err := p.writeFrame(frameTypePreamble, streamIDNone, preamblePayload); err != nil {
			return err
		}
		err := p.awaitAccept(300 * time.Millisecond)
		if err == nil {
			break
		}
		var sessionErr *tunnel.SessionError
		if errors.As(err, &sessionErr) || errors.Is(err, io.EOF) {
			return err
		}
		select {
		case <-p.stopped:
			return errors.New("tunnel: mesh connection ended during the handshake")
		case <-retry.C:
		case <-deadline.C:
			return errors.New("tunnel: the routing preamble was not accepted in time")
		}
	}
	p.mu.Lock()
	p.clientReady = true
	p.mu.Unlock()
	return nil
}

// awaitAccept waits for the allocator's accept frame within a short window.
func (p *pairConn) awaitAccept(wait time.Duration) error {
	f, err := p.readPairFrame(wait)
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

// readPreamble reads the one-time routing preamble from a fresh inbound pair.
// Used by the allocator edge's Accept.
func (p *pairConn) readPreamble() (tunnel.Preamble, error) {
	f, err := p.readPairFrame(maxHandshakeWait)
	if err != nil {
		return tunnel.Preamble{}, err
	}
	if f.typ != frameTypePreamble {
		return tunnel.Preamble{}, errors.New("tunnel: the first frame must be the routing preamble")
	}
	return decodePreamble(f.payload)
}

// writeAccept confirms the allocator accepted the pair's preamble. Written after
// validation, before any stream is opened, so a rejected session never carries
// payload.
func (p *pairConn) writeAccept() error {
	return p.writeFrame(frameTypeAccept, streamIDNone, nil)
}

// OpenStream opens a new client-side stream on this pair with a container port
// reference. It assigns a stream id, sends the stream-open frame, and returns
// the stream. The peer rejects an unauthorized port with a close frame that
// surfaces on the stream's Read before any payload is consumed.
// openAllocatorStream handles an inbound stream-open on the allocator side: it
// resolves the container port against the authorized session's port list and
// either authorizes the stream (surfacing it for the edge to splice) or rejects
// it with ReasonUnauthorized before any payload is spliced.
func (p *pairConn) openAllocatorStream(f frame) {
	p.mu.Lock()
	if p.closed || p.session == nil {
		p.mu.Unlock()
		p.writeFrame(frameTypeClose, f.streamID, closePayload(tunnel.ReasonUnauthorized, "unvalidated mesh session"))
		return
	}
	if _, exists := p.streams[f.streamID]; exists {
		p.mu.Unlock()
		return
	}
	session := p.session
	p.mu.Unlock()

	port := decodeStreamOpen(f.payload)
	target, ok := session.ResolveTarget(port)
	if !ok {
		p.writeFrame(frameTypeClose, f.streamID, closePayload(tunnel.ReasonUnauthorized, "target port was not pre-authorized"))
		p.remove(f.streamID)
		return
	}
	s := newStream(p, f.streamID)
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.streams[f.streamID] = s
	p.mu.Unlock()
	select {
	case p.incoming <- &IncomingStream{Conn: s, Target: target, ID: f.streamID}:
	default:
		// The edge is not keeping up; drop the newest stream rather than grow
		// unbounded. The client's stream on this slot fails to splice.
		p.remove(f.streamID)
		p.writeFrame(frameTypeClose, f.streamID, closePayload(tunnel.ReasonSessionFailed, "allocator edge is not accepting streams"))
	}
}

// Authorize marks the allocator-side pair validated and sends the accept frame.
// session carries the slot list the pair resolves stream-opens against.
func (p *pairConn) Authorize(session *tunnel.Session) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrEdgeClosed
	}

	select {
	case <-session.Done():
		p.mu.Unlock()
		return ErrEdgeClosed
	default:
	}
	p.session = session
	p.mu.Unlock()
	go func() {
		select {
		case <-session.Done():
			p.endAll(&tunnel.SessionError{Reason: tunnel.ReasonExecutionEnded})
		case <-p.stopped:
		}
	}()
	return p.writeAccept()
}

// AcceptStream returns the next authorized allocator-side stream, or an error
// when the pair closes.
func (p *pairConn) AcceptStream() (*IncomingStream, error) {
	select {
	case is := <-p.incoming:
		p.mu.Lock()
		closed, session := p.closed, p.session
		p.mu.Unlock()
		if closed {
			return nil, ErrEdgeClosed
		}
		if session != nil {
			select {
			case <-session.Done():
				return nil, ErrEdgeClosed
			default:
			}
		}
		return is, nil
	case <-p.stopped:
		return nil, ErrEdgeClosed
	}
}

// remove deletes a stream by id. It is the onEnd hook for every stream.
func (p *pairConn) remove(id uint32) {
	p.mu.Lock()
	delete(p.streams, id)
	p.mu.Unlock()
}

// endAll ends every stream and marks the pair closed, optionally notifying the
// peer with a classified reason.
func (p *pairConn) endAll(sessionErr error) { p.terminate(sessionErr, true) }

// terminate consumes remote closes without echoing them back.
func (p *pairConn) terminate(sessionErr error, notifyPeer bool) {
	p.closeOnce.Do(func() {
		// Notify the peer before taking the lock; writeFrame locks p.mu itself.
		if e, ok := sessionErr.(*tunnel.SessionError); ok && sessionErr != nil && notifyPeer {
			_ = p.writeFrame(frameTypeClose, streamIDNone, closePayload(e.Reason, e.Detail))
		}
		p.mu.Lock()
		p.closed = true
		p.closeErr = sessionErr
		if p.closeErr == nil {
			p.closeErr = io.EOF
		}
		streams := make([]*streamConn, 0, len(p.streams))
		for _, s := range p.streams {
			streams = append(streams, s)
		}
		p.streams = make(map[uint32]*streamConn)
		p.raw = nil
		p.cond.Broadcast()
		p.mu.Unlock()
		close(p.stopped)
		for _, s := range streams {
			s.mu.Lock()
			s.peerErr = p.closeErr
			s.mu.Unlock()
			s.end(nil)
		}
		if p.owner != nil {
			p.owner.removePair(string(p.peer))
		}
	})
}

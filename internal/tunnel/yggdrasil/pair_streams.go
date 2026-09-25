package yggdrasil

import (
	"errors"

	"github.com/mytecor/r1s/internal/tunnel"
)

// Frame demultiplexing on an established pair and the allocator-side stream
// authorization: inbound packets arrive through the mux's read loop, and this
// file routes each frame to the pair-level handshake, a new allocator-side
// stream, or the targeted stream. The client-side stream opener and the
// authorized-stream handoff to the r1sd edge also live here. The one-time
// routing handshake lives in pair_handshake.go.

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
		// unbounded. The client's stream on this port fails to splice.
		p.remove(f.streamID)
		p.writeFrame(frameTypeClose, f.streamID, closePayload(tunnel.ReasonSessionFailed, "allocator edge is not accepting streams"))
	}
}

// Authorize marks the allocator-side pair validated and sends the accept frame.
// session carries the target port list the pair resolves stream-opens against.
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

// IncomingStream is one allocator-side stream, authorized and ready to splice.
// It carries the resolved target port the edge splices to and the byte stream.
type IncomingStream struct {
	Conn   tunnel.Conn
	Target tunnel.Target
	ID     uint32
}

package yggdrasil

import (
	"errors"
	"io"
	"time"

	"github.com/mytecor/r1s/internal/tunnel"
)

// The pair's one-time routing handshake: the client sends the preamble as the
// pair's first frame and waits for the allocator's accept; the allocator reads
// the preamble and confirms with the accept frame. A rejected or
// unauthenticated session fails here, before a single stream (and thus a
// payload byte) is opened. Handshake frames arrive through ingestPairFrame and
// accumulate in the pair's raw buffer until a handshake reader consumes them.

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

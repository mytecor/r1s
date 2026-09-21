package yggdrasil

import (
	"encoding/binary"
	"errors"
	"io"
	"sync"

	"github.com/mytecor/r1s/internal/tunnel"
)

// streamConn is one logical stream within an authenticated pair (F19-01). A
// pair carries many streams with distinct stream IDs; each stream is a reliable,
// ordered, bidirectional byte pipe with per-stream half-close, teardown, and
// flow control. Streams close independently of each other: a half-close or
// classified teardown on one stream never affects the others on the same
// authenticated pair.
//
// Flow control is per-stream with a sliding window (HTTP/2 style): a receiver
// advertises recvAvail bytes; the sender blocks Write while the window is
// exhausted. A slow consumer blocks only its own stream's sender, never the pair
// readloop or any other stream on the same pair.
type streamConn struct {
	pair *pairConn
	peer []byte // authenticated remote edge node public key (opaque bytes)
	id   uint32

	mu       sync.Mutex
	inbox    []byte // decoded, not yet consumed stream bytes
	eof      bool   // peer half-closed its write side: EOF after drain
	peerErr  error  // classified peer teardown (a *tunnel.SessionError)
	closed   bool   // local full close
	readDone bool   // local read side aborted (CloseRead)
	writeEOF bool   // local write side half-closed (CloseWrite)

	// recvAvail is how many bytes this side may still accept; each Write consumes
	// this. grantPending accumulates consumed Read bytes to be granted back.
	recvAvail    int
	grantPending int
	// peerWindow is the peer's advertised receive window: Write blocks when it
	// reaches zero and resumes when a window-update frame adds space.
	peerWindow int

	// cond wakes Read when stream state changes; sendable wakes a blocked
	// Write when the peer's window is replenished.
	cond     *sync.Cond
	sendable *sync.Cond

	stopped chan struct{}
	// closeOnce is the pair's unregister hook: end always runs exactly once and
	// removes the stream from the pair.
	closeOnce sync.Once
}

// streamWindowSize is the initial per-stream receive window in bytes.
const streamWindowSize = 64 * 1024

// windowUpdateThreshold is the minimum consumed-bytes that accumulates before a
// window-update frame goes out, so a busy stream does not emit one update per
// Read call.
const windowUpdateThreshold = 16 * 1024

// newStream creates a local stream within a pair. A new stream is either the
// initiator's freshly opened stream (which sends stream-open and may write
// payload) or the responder's authorized copy (which receives frames after the
// stream-open was validated).
func newStream(pair *pairConn, id uint32) *streamConn {
	s := &streamConn{
		pair:       pair,
		peer:       pair.peer,
		id:         id,
		recvAvail:  streamWindowSize,
		peerWindow: streamWindowSize,
		stopped:    make(chan struct{}),
	}
	s.cond = sync.NewCond(&s.mu)
	s.sendable = sync.NewCond(&s.mu)
	return s
}

// sendOpen writes the stream-open frame for this stream. It must be called
// before any payload Write; the allocator resolves the container port against
// its grant-time port list and rejects an unauthorized port with a close frame
// before any payload is spliced.
func (s *streamConn) sendOpen(targetPort uint16) error {
	s.mu.Lock()
	if s.closed || s.writeEOF {
		s.mu.Unlock()
		return errors.New("tunnel write side is closed")
	}
	s.mu.Unlock()
	return s.pair.writeFrame(frameTypeStreamOpen, s.id, encodeStreamOpen(targetPort))
}

// sendData writes one payload frame, honoring the peer's flow-control window.
// It blocks while the window is exhausted; another stream's writes are never
// affected because each stream owns its own window.
func (s *streamConn) sendData(payload []byte) error {
	s.mu.Lock()
	for s.peerWindow < len(payload) && !s.closed && !s.writeEOF && s.peerErr == nil {
		s.sendable.Wait()
	}
	if s.closed || s.writeEOF {
		s.mu.Unlock()
		return errors.New("tunnel write side is closed")
	}
	if s.peerErr != nil {
		err := s.peerErr
		s.mu.Unlock()
		return err
	}
	s.peerWindow -= len(payload)
	s.mu.Unlock()
	return s.pair.writeFrame(frameTypeData, s.id, payload)
}

// Read implements tunnel.Conn. It blocks until bytes are available, the peer
// half-closed (io.EOF after drain), the peer tore the stream down with a
// classified reason, or the local side closed the stream. After returning bytes,
// it grants them back to the peer's window in a window-update frame, batched to
// the threshold so a busy stream does not emit one update per Read.
func (s *streamConn) Read(p []byte) (int, error) {
	for {
		s.mu.Lock()
		if s.readDone {
			s.mu.Unlock()
			return 0, tunnel.ErrReadAborted
		}
		if len(s.inbox) > 0 {
			n := copy(p, s.inbox)
			s.inbox = s.inbox[n:]
			// Accumulate consumed bytes toward the window-update threshold.
			s.recvAvail += n
			s.grantPending += n
			sendUpdate := s.grantPending >= windowUpdateThreshold
			var updatePayload []byte
			if sendUpdate {
				updatePayload = streamWindowPayload(s.grantPending)
				s.grantPending = 0
			}
			s.mu.Unlock()
			if sendUpdate {
				_ = s.pair.writeFrame(frameTypeWindowUpdate, s.id, updatePayload)
			}
			return n, nil
		}
		if s.peerErr != nil {
			err := s.peerErr
			s.mu.Unlock()
			s.end(nil)
			return 0, err
		}
		if s.eof {
			s.mu.Unlock()
			return 0, io.EOF
		}
		if s.closed {
			s.mu.Unlock()
			return 0, tunnel.ErrReadAborted
		}
		s.cond.Wait()
		s.mu.Unlock()
	}
}

// Write sends p as data frames, segmenting at the pair's packet MTU and honoring
// the peer's per-stream flow-control window. A write after a local close or
// half-close fails instead of buffering into a dead stream.
func (s *streamConn) Write(p []byte) (int, error) {
	total := 0
	for total < len(p) {
		end := total + s.pair.maxSeg
		if end > len(p) {
			end = len(p)
		}
		if err := s.sendData(p[total:end]); err != nil {
			return total, err
		}
		total = end
	}
	return total, nil
}

// CloseRead aborts the read side locally: buffered inbound bytes are discarded
// and no further inbound data is accepted. The mesh cannot signal read-abort to
// the peer, so the peer's writes ultimately fail; a later Read on this side
// returns ErrReadAborted.
func (s *streamConn) CloseRead() error {
	s.mu.Lock()
	s.readDone = true
	s.inbox = nil
	s.cond.Broadcast()
	s.mu.Unlock()
	return nil
}

// CloseWrite half-closes the write side: the peer drains buffered bytes then
// sees io.EOF. Independent of CloseRead.
func (s *streamConn) CloseWrite() error {
	s.mu.Lock()
	if s.closed || s.writeEOF {
		s.mu.Unlock()
		return nil
	}
	s.writeEOF = true
	s.sendable.Broadcast()
	s.mu.Unlock()
	return s.pair.writeFrame(frameTypeEOF, s.id, nil)
}

// Close fully closes the stream and releases the peer's blocked operations.
func (s *streamConn) Close() error {
	s.end(&tunnel.SessionError{Reason: tunnel.ReasonClosed})
	return nil
}

// CloseWithReason implements tunnel.ReasonCloser: the stream ends with a
// classified teardown outcome the peer observes on its next Read after buffered
// bytes are delivered.
func (s *streamConn) CloseWithReason(reason tunnel.Reason, detail string) error {
	s.end(&tunnel.SessionError{Reason: reason, Detail: detail})
	return nil
}

// PeerKey returns the authenticated remote edge node public key.
func (s *streamConn) PeerKey() []byte { return append([]byte(nil), s.peer...) }

// ingest delivers a decoded frame to the stream. Called by the pair's readloop.
// It never blocks, so a slow consumer of this stream does not stall the pair's
// other streams.
func (s *streamConn) ingest(f frame) {
	s.mu.Lock()
	switch f.typ {
	case frameTypeData:
		// Guard against a misbehaving or drifted peer sending beyond the window.
		if len(f.payload) > s.recvAvail {
			s.mu.Unlock()
			s.end(&tunnel.SessionError{Reason: tunnel.ReasonSessionFailed, Detail: "per-stream flow-control violation"})
			return
		}
		s.recvAvail -= len(f.payload)
		s.inbox = append(s.inbox, f.payload...)
	case frameTypeEOF:
		s.eof = true
	case frameTypeClose:
		if s.peerErr == nil && len(f.payload) > 0 {
			s.peerErr = parseClosePayload(f.payload)
		}
	case frameTypeWindowUpdate:
		if len(f.payload) >= 4 {
			s.peerWindow += int(binary.BigEndian.Uint32(f.payload[:4]))
		}
	}
	// Broadcast both so a Write blocked on the peer's window (sendable) and a
	// Read blocked on new data (cond) both observe an EOF, a classified peer
	// teardown, or a replenished window.
	s.cond.Broadcast()
	s.sendable.Broadcast()
	s.mu.Unlock()
}

// end tears the stream down, optionally notifying the peer with a classified
// reason first, and unregisters the stream from the pair.
func (s *streamConn) end(peerErr error) {
	s.closeOnce.Do(func() {
		if sessionErr, ok := peerErr.(*tunnel.SessionError); ok && sessionErr != nil {
			_ = s.pair.writeFrame(frameTypeClose, s.id, closePayload(sessionErr.Reason, sessionErr.Detail))
		}
		s.mu.Lock()
		s.closed = true
		s.cond.Broadcast()
		s.sendable.Broadcast()
		s.mu.Unlock()
		close(s.stopped)
		if s.pair != nil {
			s.pair.remove(s.id)
		}
	})
}

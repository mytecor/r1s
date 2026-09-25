package yggdrasil

import (
	"errors"
	"io"
	"sync"

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
//     names the container port it is spliced to in its stream-open frame.
//   - Allocator edge: mux.accept returns a pending pair whose preamble has
//     arrived. The allocator core opens the execution's owner-authenticated
//     session (OpenTunnelSession -> a Session carrying the client-supplied
//     target port list), then Authorize pins the session and sends the accept
//     frame. From then on the pair opens allocator-side streams for authorized
//     stream-open frames and pushes them (with their resolved target port) to
//     AcceptStream, so the r1sd edge can splice each stream independently.
//
// Packets arrive through the pair's owner mux, which routes a decoded frame to
// the targeted stream without holding a pair-wide lock while a slow stream is
// fed, so the pair readloop never blocks on a slow stream.
//
// This file holds the pair state and its teardown lifecycle. The one-time
// routing handshake lives in pair_handshake.go; frame demultiplexing and the
// allocator-side stream authorization live in pair_streams.go.
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
	// session is the allocator-validated session (with the client-supplied
	// target port list) set by Authorize; nil on the client edge.
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

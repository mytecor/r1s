package rns

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/Quad4-Software/Reticulum-Go/pkg/backbone"
	"github.com/Quad4-Software/Reticulum-Go/pkg/buffer"
	"github.com/Quad4-Software/Reticulum-Go/pkg/channel"
	"github.com/Quad4-Software/Reticulum-Go/pkg/link"
	"github.com/Quad4-Software/Reticulum-Go/pkg/packet"
	rnstransport "github.com/Quad4-Software/Reticulum-Go/pkg/transport"
)

// reticulumCompat contains the temporary Reticulum-Go v1.2.0 workarounds used
// by the private tunnel transport. Keeping all workarounds behind this value
// makes their removal mechanical once upstream issues #17 and #18 are released:
//
//  1. replace ensureBackbone with the normal backbone.Init(auto) path;
//  2. replace prepareConn in newConn with the stock Link/Buffer construction
//     (this also removes the keepalive liveness beacon and its contract tests,
//     which compensate for the v1.2.0 keepalive/staleness timeout — see below);
//  3. delete this file and its contract test.
//
// No wire format, Channel message, or application framing is changed here: the
// compat keepalive frame uses a dedicated user-range Channel message type
// (compatKeepaliveType) distinct from the stream's, so it never appears in the
// byte stream. Stream data remains the stock StreamDataMessage wire format; the
// compat writer only selects its standard uncompressed form so Python RNS peers
// decode it without a protocol extension. The adapter chooses a safe public
// Backbone backend, serializes the public LinkInterface ingress boundary,
// supplies the v1.2.0-safe Buffer bridge, and keeps each established Link alive
// through the keepalive/staleness watchdog (BACKLOG entry 10) by periodically
// delivering a peer-visible inbound Channel frame from every edge.
var tunnelReticulumCompat reticulumCompat

type reticulumCompat struct{}

func ensureBackbone() error {
	return tunnelReticulumCompat.ensureBackbone()
}

// ensureBackbone selects Reticulum-Go's public synchronous Go backend for the
// tunnel-enabled process. v1.2.0's kqueue/epoll hub can lose write interest when
// QueueSend races writeStream; BackendGo bypasses that poller path and applies
// backpressure through the blocking net.Conn write instead.
//
// The Backbone hub is process-global, so this compatibility choice also
// applies to any other Backbone interface in the same process. Silently
// accepting an already-created native hub would reintroduce the bug; fail
// closed and make startup order explicit if another component initialised it
// first.
func (reticulumCompat) ensureBackbone() error {
	if hub := backbone.Get(); hub != nil {
		if hub.Backend() != backbone.BackendGo {
			return fmt.Errorf("tunnel RNS requires Reticulum-Go Backbone backend %q, found %q", backbone.BackendGo, hub.Backend())
		}
		return nil
	}
	hub, err := backbone.Init(backbone.BackendGo)
	if err != nil {
		return fmt.Errorf("initialise tunnel RNS Backbone compatibility backend: %w", err)
	}
	if hub == nil || hub.Backend() != backbone.BackendGo {
		return errors.New("tunnel RNS Backbone compatibility backend was not selected")
	}
	return nil
}

// prepareConn is the single session-side attachment point for the compatibility
// layer. It installs ordered Link ingress and builds the v1.2.0-safe Buffer
// adapter before any identification or application bytes can be exchanged.
func (c reticulumCompat) prepareConn(transport *rnstransport.Transport, candidate *link.Link) (*link.Link, *reticulumCompatStream, error) {
	prepared, err := c.prepareLink(transport, candidate)
	if err != nil {
		return nil, nil, err
	}
	return prepared, newReticulumCompatStream(prepared, prepared.GetChannel()), nil
}

// prepareLink resolves the Link instance actually registered in the Transport
// and replaces that registry entry with a serial ingress proxy. The v1.2.0
// transport dispatches packets on several workers; without this proxy two
// HandleInbound calls for one Link can invoke Channel handlers out of order.
// Serializing the entire Link call preserves the Channel RX ring's intended
// behavior: an early N+1 waits in the ring, and N later drains both in order.
func (reticulumCompat) prepareLink(transport *rnstransport.Transport, candidate *link.Link) (*link.Link, error) {
	if transport == nil || candidate == nil {
		return nil, errors.New("prepare tunnel RNS Link compatibility: transport and link are required")
	}

	registered := transport.FindLink(candidate.GetLinkID())
	switch value := registered.(type) {
	case *serializedInboundLink:
		return value.link, nil
	case *link.Link:
		candidate = value
	case nil:
		// The callback-provided Link is authoritative when registration has not
		// become visible yet. Register the proxy below before application data is
		// allowed onto the established Link.
	default:
		return nil, fmt.Errorf("prepare tunnel RNS Link compatibility: unsupported registered link %T", registered)
	}

	transport.RegisterLink(candidate.GetLinkID(), &serializedInboundLink{
		LinkInterface: candidate,
		link:          candidate,
	})
	return candidate, nil
}

// serializedInboundLink is deliberately only a transport registry proxy. All
// Link behavior is delegated unchanged except HandleInbound, whose full call
// (including Channel callbacks and packet proof emission) is serialized per
// Link. Different Links retain independent locks and remain concurrent.
type serializedInboundLink struct {
	rnstransport.LinkInterface
	link *link.Link
	mu   sync.Mutex
}

func (l *serializedInboundLink) HandleInbound(pkt *packet.Packet) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.LinkInterface.HandleInbound(pkt)
}

var _ rnstransport.LinkInterface = (*serializedInboundLink)(nil)

// compatKeepaliveType is a user-range Channel message type (0x4000) used by
// the sustained-transfer staleness workaround below. It is separate from the
// stream's StreamDataMessageType (0xff00, system-reserved range) so a keepalive
// frame is never mistaken for stream bytes. The Channel's per-link sequence
// space is shared across message types, so a keepalive envelope carried between
// two stream envelopes is still drained in order: the stream reader declines it
// (HandleMessage returns false for non-stream types) and the keepalive handler
// consumes it, leaving stream byte order untouched.
const compatKeepaliveType uint16 = 0x4000

// tunnelKeepaliveInterval is how often each tunnel edge advertises itself as
// alive to the peer while its Link is Active. It is well below Reticulum-Go
// v1.2.0's low-RTT staleTime floor (keepalive = KeepaliveMinSec = 5s, so
// staleTime = 10s), leaving margin even if a tick is dropped or deferred.
// Keep it under staleTime/2; see TestSustainedTransferOutlivesStaleTime.
const tunnelKeepaliveInterval = 3 * time.Second

// tunnelChannelReadyPollInterval is the temporary Reticulum-Go #19 workaround.
// Channel.WaitReady polls every 5ms, which caps a fast Link at roughly
// WindowMaxFast*payload/5ms (about 4 MiB/s at the negotiated 423-byte tunnel
// payload). The public Channel API has no delivery notification to wait on, so
// the compatibility layer polls at a much shorter interval until upstream can
// provide an event-driven wait. A reusable timer in waitReady avoids allocating
// one timer per poll.
const tunnelChannelReadyPollInterval = 100 * time.Microsecond

// reticulumCompatStream adapts the remaining v1.2.0 Buffer API gaps behind a
// single byte-stream boundary: dynamic MDU sizing, TX-window backpressure,
// blocking reads, serialized write/half-close, an uncompressed writer policy,
// and a periodic liveness beacon that keeps the Link out of the
// keepalive/staleness timeout described in BACKLOG entry 10. Once upstream
// provides those semantics, newConn can replace this type with the stock
// bidirectional Buffer and the compatibility file can be deleted as one unit.
//
// The liveness beacon is flat-out required for any sustained one-direction
// transfer longer than ~10s and therefore lives here (not in session.go): on a
// low-RTT Link Reticulum-Go v1.2.0 floors keepalive at KeepaliveMinSec (5s) and
// staleTime at 10s, and during a one-way transfer the writer's lastInbound is
// refreshed only by Channel ACK/proof traffic, which the transport handles
// without ever invoking Link.HandleInbound (no recordInbound, no lastInbound
// update). The result is that exactly at staleTime the writer's watchdog CASes
// its own Link ACTIVE -> STALE and WaitReady fails the write with
// ErrLinkNotReady. This workaround makes every edge send a tiny Channel frame
// to the peer on a tick: the peer's Link.HandleInbound receives it as ordinary
// inbound data and refreshes its lastInboundNs, which is exactly the signal the
// watchdog needs. A busy writer whose own TX window is saturated skips its
// send, but the idle peer's send always gets through and keeps the writer
// alive. Removal is the keepalive block in newReticulumCompatStream plus the
// contract test that gates it.
type reticulumCompatStream struct {
	rw        *buffer.Buffer
	writer    *reticulumUncompressedWriter
	ch        *channel.Channel
	frag      int
	readReady chan struct{}
	writeMu   sync.Mutex
	// keepaliveHandlerID records the liveness-beacon message handler slot so
	// the removal contract test can assert the beacon is wired on every Conn.
	keepaliveHandlerID int
}

// keepaliveInstalled reports whether the periodic liveness beacon (BACKLOG
// entry 10 workaround) is wired into this stream. It exists so the removal
// contract test fails loudly if the beacon is deleted without removing the
// test; it is not part of the tunnel behaviour itself.
func (s *reticulumCompatStream) keepaliveInstalled() bool { return s.keepaliveHandlerID > 0 }

func newReticulumCompatStream(lnk *link.Link, ch *channel.Channel) *reticulumCompatStream {
	rawR := buffer.NewRawChannelReader(streamID, ch)
	readReady := make(chan struct{}, 1)
	rawR.AddReadyCallback(func(int) {
		select {
		case readReady <- struct{}{}:
		default:
		}
	})
	frag := reticulumFragmentSize(ch)
	reader := bufio.NewReader(rawR)
	uncompressedWriter := &reticulumUncompressedWriter{ch: ch, frag: frag, status: lnk.GetStatus}
	writer := bufio.NewWriterSize(uncompressedWriter, frag)
	stream := &reticulumCompatStream{
		rw:        &buffer.Buffer{ReadWriter: bufio.NewReadWriter(reader, writer)},
		writer:    uncompressedWriter,
		ch:        ch,
		frag:      frag,
		readReady: readReady,
	}

	// Register the keepalive beacon: recognize the compat keepalive type for
	// inbound dispatch and consume it so it never reaches the byte stream
	// reader, and start the per-edge sender. A custom constructor keeps the
	// envelope structured even though the generic one would also unpack it.
	_ = ch.RegisterMessageType(compatKeepaliveType, func() channel.MessageBase {
		return &channel.GenericMessage{Type: compatKeepaliveType}
	})
	handlerID := ch.AddMessageHandler(func(msg channel.MessageBase) bool {
		if msg == nil || msg.GetType() != compatKeepaliveType {
			return false
		}
		return true
	})
	stream.keepaliveHandlerID = handlerID
	go stream.keepaliveLoop(lnk)

	return stream
}

// keepaliveLoop periodically reports this edge's liveness to the peer while the
// Link is Active, and stops as soon as the Link leaves Active (the Conn is
// closed/teardown). Sending is strictly best-effort: when the Channel TX window
// is full the beat is skipped rather than queued, so a saturated data stream is
// never delayed by liveness traffic; the idle peer's beat still arrives.
func (s *reticulumCompatStream) keepaliveLoop(lnk *link.Link) {
	ticker := time.NewTicker(tunnelKeepaliveInterval)
	defer ticker.Stop()
	for range ticker.C {
		if lnk.GetStatus() != link.StatusActive {
			return
		}
		if !s.ch.IsReadyToSend() {
			continue
		}
		_ = s.ch.Send(&channel.GenericMessage{Type: compatKeepaliveType, Data: []byte{0}})
	}
}

// reticulumFragmentSize mirrors Python RawChannelWriter: Channel.MDU already
// excludes the six-byte Channel header, and each StreamDataMessage consumes a
// further two-byte stream header. v1.2.0 instead caps against a fixed 457-byte
// payload that can exceed a live Link's negotiated MDU.
func reticulumFragmentSize(ch *channel.Channel) int {
	n := ch.MDU() - buffer.StreamHeaderSize
	if n > buffer.MaxDataLen {
		n = buffer.MaxDataLen
	}
	if n < 1 {
		n = 1
	}
	return n
}

// reticulumUncompressedWriter is the temporary Reticulum-Go #18 compatibility
// policy for tunnel streams. It emits the existing StreamDataMessage wire type
// with Compressed=false, which is fully interoperable with Python RNS, while
// avoiding v1.2.0's three synchronous bzip2 probes for every payload over 32
// bytes. It also turns a full Channel window into backpressure without using
// v1.2.0's throughput-limiting 5ms WaitReady poll. Closing the Link changes
// status, so an in-flight write returns ErrLinkNotReady after at most one short
// compatibility polling interval.
type reticulumUncompressedWriter struct {
	ch     *channel.Channel
	frag   int
	status func() byte
	closed bool
}

func (w *reticulumUncompressedWriter) waitReady(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	isActive := func() bool {
		if w.status == nil {
			return false
		}
		status := w.status()
		return status == link.StatusActive || status == rnstransport.StatusActive
	}
	if !isActive() {
		return channel.ErrLinkNotReady
	}
	if w.ch.IsReadyToSend() {
		return nil
	}

	timer := time.NewTimer(tunnelChannelReadyPollInterval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			if !isActive() {
				return channel.ErrLinkNotReady
			}
			if w.ch.IsReadyToSend() {
				return nil
			}
			timer.Reset(tunnelChannelReadyPollInterval)
		}
	}
}

func (w *reticulumUncompressedWriter) send(msg *buffer.StreamDataMessage) error {
	for {
		if err := w.waitReady(context.Background()); err != nil {
			return err
		}
		err := w.ch.Send(msg)
		if errors.Is(err, channel.ErrLinkNotReady) {
			// A best-effort keepalive can consume the last slot between the
			// readiness check and Send. Wait for the next slot and retry; Channel
			// reserves no sequence and transmits nothing on this error.
			continue
		}
		return err
	}
}

func (w *reticulumUncompressedWriter) Write(p []byte) (int, error) {
	if w.closed {
		return 0, io.ErrClosedPipe
	}
	if len(p) == 0 {
		return 0, nil
	}
	if len(p) > w.frag {
		p = p[:w.frag]
	}
	if err := w.send(&buffer.StreamDataMessage{
		StreamID: streamID,
		Data:     p,
	}); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (w *reticulumUncompressedWriter) Close() error {
	if w.closed {
		return nil
	}
	if err := w.send(&buffer.StreamDataMessage{
		StreamID: streamID,
		EOF:      true,
	}); err != nil {
		return err
	}
	w.closed = true
	return nil
}

func (s *reticulumCompatStream) Read(p []byte, closed <-chan struct{}) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for {
		n, err := s.rw.Read(p)
		if n != 0 || err != nil {
			return n, err
		}
		select {
		case <-s.readReady:
		case <-closed:
			return 0, io.ErrClosedPipe
		}
	}
}

func (s *reticulumCompatStream) Write(p []byte) (int, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	written := 0
	for len(p) > 0 {
		chunk := p
		if len(chunk) > s.frag {
			chunk = chunk[:s.frag]
		}
		n, err := s.rw.Write(chunk)
		written += n
		p = p[n:]
		if err != nil {
			return written, err
		}
		if n == 0 {
			return written, io.ErrShortWrite
		}
	}
	if err := s.rw.ReadWriter.Writer.Flush(); err != nil {
		return written, err
	}
	return written, nil
}

func (s *reticulumCompatStream) CloseWrite() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.rw.ReadWriter.Writer.Flush(); err != nil {
		return err
	}
	return s.writer.Close()
}

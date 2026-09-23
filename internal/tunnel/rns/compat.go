// The tunnel RNS compatibility layer (backbone.go, compat.go, writer.go)
// contains the temporary Reticulum-Go v1.2.0 workarounds split by
// responsibility: backbone.go holds backend selection and the serialized
// inbound Link proxy, compat.go the one-stream Buffer adapter plus the
// keepalive/staleness beacon, and writer.go the uncompressed-writer policy.
// See the package comment on tunnelReticulumCompat in backbone.go for the
// removal plan and the wire-compatibility guarantees that must survive it.
package rns

import (
	"bufio"
	"io"
	"sync"
	"time"

	"github.com/Quad4-Software/Reticulum-Go/pkg/buffer"
	"github.com/Quad4-Software/Reticulum-Go/pkg/channel"
	"github.com/Quad4-Software/Reticulum-Go/pkg/link"
)

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

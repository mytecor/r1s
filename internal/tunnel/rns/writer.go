package rns

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/Quad4-Software/Reticulum-Go/pkg/buffer"
	"github.com/Quad4-Software/Reticulum-Go/pkg/channel"
	"github.com/Quad4-Software/Reticulum-Go/pkg/link"
	rnstransport "github.com/Quad4-Software/Reticulum-Go/pkg/transport"
)

// tunnelChannelReadyPollInterval is the temporary Reticulum-Go #19 workaround.
// Channel.WaitReady polls every 5ms, which caps a fast Link at roughly
// WindowMaxFast*payload/5ms (about 4 MiB/s at the negotiated 423-byte tunnel
// payload). The public Channel API has no delivery notification to wait on, so
// the compatibility layer polls at a much shorter interval until upstream can
// provide an event-driven wait. A reusable timer in waitReady avoids allocating
// one timer per poll.
const tunnelChannelReadyPollInterval = 100 * time.Microsecond

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

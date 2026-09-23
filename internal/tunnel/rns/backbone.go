package rns

import (
	"errors"
	"fmt"
	"sync"

	"github.com/Quad4-Software/Reticulum-Go/pkg/backbone"
	"github.com/Quad4-Software/Reticulum-Go/pkg/link"
	"github.com/Quad4-Software/Reticulum-Go/pkg/packet"
	rnstransport "github.com/Quad4-Software/Reticulum-Go/pkg/transport"
)

// tunnelReticulumCompat contains the temporary Reticulum-Go v1.2.0 workarounds
// used by the private tunnel transport. Keeping all workarounds behind this
// value makes their removal mechanical once upstream issues #17 and #18 are
// released:
//
//  1. replace ensureBackbone with the normal backbone.Init(auto) path;
//  2. replace prepareConn in newConn with the stock Link/Buffer construction
//     (this also removes the keepalive liveness beacon and its contract tests,
//     which compensate for the v1.2.0 keepalive/staleness timeout — see
//     compat.go);
//  3. delete the compat files and their contract tests.
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

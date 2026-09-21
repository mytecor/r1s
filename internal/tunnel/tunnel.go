// Package tunnel defines the generic direct-access tunnel contract used by
// r1s. It is deliberately free of any network library: Yggdrasil (or another
// overlay) stays a network choice behind the generic endpoint contract, and
// the core, protocol, and transport adapter build without it.
//
// The F14 contract has two halves:
//
//   - F14-01: an execution-scoped, short-lived, single-use access grant minted
//     by the allocator over the authenticated control plane, and the generic
//     endpoint advertisement the allocator returns. Everything here is in
//     memory and deterministic.
//   - F14-02: the edge adapters (Listener/Dialer/Conn) that carry arbitrary
//     bytes between the client and the allocator-local target. Their generic
//     shape lives here; the Yggdrasil implementation lives in
//     internal/tunnel/yggdrasil and is the only package that imports a
//     network library.
package tunnel

import (
	"context"
	"errors"
	"io"
)

// Target names one client-owned container-side destination port a tunnel stream
// is spliced to. The client owns the port list and sends it in the tunnel grant
// request; the allocator validates only its shape, binds it into the minted
// grant unchanged, and proxies/splices each stream to the port the client named
// (resolved on the allocator loopback, 127.0.0.1:<port>). There are no named
// slots: each target is just the port to export. The client binds its own local
// listener and names Port in each stream-open.
type Target struct {
	Port uint16
}

// MaxPeerKeySize is the upper bound on the peer public key wire format. A
// Yggdrasil public key (Curve25519) is 32 bytes; any other key format used
// under the same field must be no larger. This limit is enforced by wire
// validation before the allocator processes the grant request.
const MaxPeerKeySize = 64

// Endpoint is a transport-neutral allocation advertisement returned to the
// client with a minted grant. Address and PubKey are opaque bytes: the
// protocol never interprets them, so the field carries no Yggdrasil-specific
// address or key type. Nothing secret is embedded in an endpoint.
type Endpoint struct {
	// Address is the transport-specific destination (for example an overlay
	// address) the client connects to.
	Address []byte
	// PubKey is the transport-specific edge public key the client pins at
	// connect time. It is an opaque public-key value, not a bearer secret.
	PubKey []byte
}

// Errors returned by the registry. They describe authorization and lifecycle
// outcomes for a tunnel grant or session; the allocator core maps them to
// explicit CommandError codes.
var (
	ErrGrantNotFound   = errors.New("tunnel grant not found")
	ErrGrantExpired    = errors.New("tunnel grant expired")
	ErrGrantReused     = errors.New("tunnel grant already used")
	ErrPeerKeyMismatch = errors.New("tunnel peer key does not match the pinned grant")
	ErrSessionBusy     = errors.New("execution already has a live tunnel session")
	// ErrUnknownTargetPort reports that a stream header referenced a container
	// port the allocator did not pre-authorize. It is rejected before any
	// payload byte moves.
	ErrUnknownTargetPort = errors.New("tunnel target port was not pre-authorized")
)

// ErrReadAborted reports that the local read side of a tunnel was aborted: the
// connection was torn down from this side (CloseRead or a full Close), so the
// peer is gone or has stopped reading. The relay can recognize it on Read to
// distinguish a local teardown from a genuine mid-session transport failure
// instead of misreporting a negotiated teardown reason.
var ErrReadAborted = errors.New("tunnel read side aborted")

// Conn is a reliable, ordered, bidirectional byte pipe between a client edge
// and the allocator edge (F14-02). It is the F14-02 stream contract: the raw
// transport carries arbitrary bytes (SSH, HTTP, or anything else the owner
// chooses) with no protocol-level framing injected into the payload beyond the
// one-time routing preamble.
//
// It is transport-neutral and deterministic. Yggdrasil (or another overlay)
// supplies one implementation; the in-memory fake (memory.go) supplies another
// for tests. The core, protocol, and local API depend only on this shape.
// Implementations MUST be safe for concurrent Read and Write (and Close), since
// the serve relay reads and writes the same Conn from different goroutines.
type Conn interface {
	io.ReadWriteCloser
	// PeerKey returns the authenticated remote edge node public key as opaque
	// bytes. The edge MUST populate it from the transport's authenticated
	// identity, never from payload bytes: the allocator pins this key into the
	// grant at connect time, and the client pins the allocator's key before
	// sending a byte. It is available as soon as the connection is established.
	PeerKey() []byte
	// CloseRead half-closes the read side: the peer sees EOF on its write
	// side, and no further bytes are accepted from the peer. Interactive
	// protocols (SSH and similar) rely on independent per-direction closure.
	CloseRead() error
	// CloseWrite half-closes the write side: the peer sees EOF on its read
	// side, and no further bytes are sent. Independent of CloseRead.
	CloseWrite() error
}

// Listener accepts inbound tunnel connections on the allocator edge. Each
// accepted Conn already carries its authenticated PeerKey, so the allocator
// core can validate the grant and preamble before a single payload byte is
// spliced to the target.
type Listener interface {
	// Accept blocks until an inbound tunnel connection is ready or the
	// listener is closed. The returned Conn is reliable and ordered.
	Accept() (Conn, error)
	// Close stops accepting and releases any blocked Accept call.
	Close() error
}

// Dialer opens a tunnel connection from a client edge to a remote edge
// described by a transport-neutral Endpoint. The implementation pins the
// Endpoint's public key before returning, so a relay or man-in-the-middle has
// nothing usable even if it observed the advertisement.
type Dialer interface {
	// Dial connects to the remote edge at endpoint and returns an established,
	// authenticated Conn whose PeerKey matches the endpoint's pinned key. ctx
	// bounds the connect attempt.
	Dial(ctx context.Context, endpoint Endpoint) (Conn, error)
	// Close releases the dialer's node and any local resources.
	Close() error
}

// Preamble is the one-time routing header the client sends as the first bytes
// of a tunnel stream before any payload. It disambiguates the registry lookup
// (one peer key may hold grants for several executions), and the allocator
// core resolves the record under its lock and re-validates execution liveness
// at splice time before a single payload byte is relayed.
type Preamble struct {
	ExecutionID string
	GrantID     string
}

// PreambleWriter is implemented by Conn implementations whose transport does
// not carry the routing header implicitly, so the caller must write the
// one-time routing preamble as the first bytes before any payload. The
// in-memory fake does not implement it (the test relay stays byte-clean); the
// real framing/yggdrasil edge will, so the allocator core can validate the
// grant under its lock before splicing.
type PreambleWriter interface {
	// WritePreamble sends the one-time routing preamble as the first bytes of
	// the stream. It must be called once, before any payload byte is written.
	WritePreamble(Preamble) error
}

// StreamOpener is implemented by a mesh connection (F19-01) that can carry
// several logical streams over one authenticated pair. Dial returns a Conn that
// also implements StreamOpener when the transport multiplexes: each
// OpenStream names the container port it is spliced to.
type StreamOpener interface {
	// OpenStream opens a new logical stream on the same authenticated pair and
	// returns its byte pipe. It must be called after the routing preamble has
	// been written. targetPort is the container-side destination port the
	// allocator splices the stream to; it must be one the client supplied in
	// the grant.
	OpenStream(targetPort uint16) (Conn, error)
}

// Reason is the surfaced session teardown outcome. The CLI maps a non-normal
// reason to a non-zero exit and a clear diagnostic on stderr; stdout is always
// byte-clean (only tunnel bytes).
type Reason string

const (
	// ReasonClosed reports a clean, mutual close of the tunnel.
	ReasonClosed Reason = "closed"
	// ReasonExecutionEnded reports that the execution reached a terminal phase
	// while the tunnel was open; losing a tunnel never cancels the execution.
	ReasonExecutionEnded Reason = "execution_ended"
	// ReasonGrantRejected reports that the spliced grant was rejected
	// (unknown grant, wrong execution, busy session) before payload flowed.
	ReasonGrantRejected Reason = "grant_rejected"
	// ReasonGrantExpired reports that the spliced grant had expired.
	ReasonGrantExpired Reason = "grant_expired"
	// ReasonGrantRevoked reports that the grant was invalidated while the
	// session was being established or was running.
	ReasonGrantRevoked Reason = "grant_revoked"
	// ReasonUnauthorized reports an authenticated peer key that did not match
	// the grant's pinned key, or a non-owner draft of the grant.
	ReasonUnauthorized Reason = "unauthorized"
	// ReasonMeshUnreachable reports that the overlay could not be reached or
	// the remote edge refused the connection before a session was established.
	ReasonMeshUnreachable Reason = "mesh_unreachable"
	// ReasonSessionFailed reports an unrecoverable mid-session transport or
	// stream failure that carries no more specific classification: the tunnel
	// read side ended abnormally without a negotiated teardown reason and the
	// failure is not a clean mutual close.
	ReasonSessionFailed Reason = "session_failed"
)

// Normal reports whether the teardown reason is a clean outcome: anything other
// than a mutual close is abnormal and should surface a non-zero exit.
func (r Reason) Normal() bool { return r == ReasonClosed }

// SessionError reports that a tunnel session ended with a known teardown
// outcome. The allocator edge returns it from a Conn's Read (or carries it
// across the wire in the future framing layer) so the client-side relay can
// surface the exact reason rather than a bare EOF.
//
// The name is chosen to mirror CommandError: it is a classified, user-visible
// outcome, not a transport failure. A session that ends because the payload
// protocol closed it cleanly yields ReasonClosed.
type SessionError struct {
	Reason Reason
	Detail string
}

func (e *SessionError) Error() string {
	if e.Detail != "" {
		return string(e.Reason) + ": " + e.Detail
	}
	return string(e.Reason)
}

// ReasonCloser is implemented by Conn implementations whose close can carry a
// teardown reason (the allocator edge, the in-memory fake, and the future
// framing layer). The generic relay treats a Conn that only implements plain
// Close as ending cleanly.
type ReasonCloser interface {
	// CloseWithReason ends the session with a classified teardown outcome,
	// Observable by the peer after buffered bytes are drained.
	CloseWithReason(reason Reason, detail string) error
}

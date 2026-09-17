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

import "errors"

// Target names a concrete local endpoint on the allocator where a tunnel
// session is spliced. It is resolved at grant time from allocator-local
// configuration, never from a client-supplied destination and never from
// per-execution metadata.
type Target struct {
	Host string
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
)

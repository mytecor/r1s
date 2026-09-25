package allocator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
	r1sruntime "github.com/mytecor/r1s/internal/runtime"
	"github.com/mytecor/r1s/internal/tunnel"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TunnelConfig is the allocator-local authorization surface for direct-access
// tunnels. It carries no Yggdrasil-specific type: the endpoint advertisement is
// opaque bytes supplied by whatever edge is running inside r1sd, so the
// allocator core and the control protocol stay transport-neutral.
type TunnelConfig struct {
	// Enabled turns on direct-access tunneling. When disabled, any open
	// request fails with a clear CommandError.
	Enabled bool
	// Endpoint is the allocator's transport-neutral overlay advertisement
	// returned in every open ack. The allocator edge (F14-02) supplies it when
	// it starts; without it an open fails as "endpoint not ready".
	Endpoint tunnel.Endpoint
}

// tunnel errors map to explicit CommandError codes in errorResponse.
var (
	ErrTunnelDisabled   = errors.New("direct-access tunnels are disabled on this allocator")
	ErrTunnelNoEndpoint = errors.New("tunnel endpoint is not ready")
)

// OpenTunnelSession is the allocator-side accept-time authorization the edge
// (F14-02) calls before splicing a tunnel stream: it re-validates that the
// execution is still live and that the authenticated mesh peer key matches the
// execution's owner-bound key, then opens the execution's single session
// (F22-06). There is no grant token to consume and no TTL: the binding lives
// for the execution's lifetime. A failed validation leaves the binding intact
// and reusable, so a transient rejection (for example from a peer reconnecting)
// never burns the binding the way a consumed grant could.
func (a *Allocator) OpenTunnelSession(executionID string, peerKey []byte) (*tunnel.Session, error) {
	a.mu.Lock()
	record, ok := a.executions[executionID]
	if !ok {
		a.mu.Unlock()
		return nil, fmt.Errorf("%w: %q", ErrExecutionNotFound, executionID)
	}
	if protocol.Terminal(record.phase) {
		a.mu.Unlock()
		return nil, fmt.Errorf("%w: execution %q is %s", ErrInvalidTransition, record.id, record.phase)
	}
	a.mu.Unlock()

	// The peer-key-to-owner binding was established at open time by the
	// authenticated owner; here the edge's authenticated mesh peer key must
	// match the bound key.
	session, err := a.tunnels.Open(executionID, peerKey)
	if err != nil {
		return nil, err
	}
	// A splice opens only for a live execution: re-check under the lock so a
	// terminal transition committed between Open and splice cannot leak a
	// session onto a dead execution.
	a.mu.Lock()
	defer a.mu.Unlock()
	record = a.executions[executionID]
	if record == nil || protocol.Terminal(record.phase) {
		a.tunnels.CloseSession(executionID)
		return nil, fmt.Errorf("%w: execution %q is no longer live", ErrInvalidTransition, executionID)
	}
	return session, nil
}

// TunnelSession returns the active session for an execution, if any. Used by
// lifecycle tests and the edge plumbing.
func (a *Allocator) TunnelSession(executionID string) (*tunnel.Session, bool) {
	return a.tunnels.Session(executionID)
}

// handleTunnelOpen records the owned run's execution tunnel binding for the
// authenticated execution owner (F22-06). It replaces the retired mint/reply
// grant flow: there is no grant ID, TTL, or single-use token. It binds the
// owner's declared edge node public key and the client-supplied destination
// container port list to the execution for the execution's lifetime, and
// returns the transport-neutral allocator-local endpoint advertisement. A
// re-open for the same execution replaces the binding (repinning the peer key
// and target list) so a reconnected run re-establishes cleanly; the single
// live session per execution is still enforced at accept.
func (a *Allocator) handleTunnelOpen(envelope *r1sv1.Envelope, open *r1sv1.ExecutionTunnelOpen) ([]*r1sv1.Envelope, error) {
	peerKey := open.GetYggPeerPubkey()
	now := a.now().UTC()
	a.mu.Lock()
	record, ok := a.executions[open.GetExecutionId()]
	if !ok {
		a.mu.Unlock()
		return nil, fmt.Errorf("%w: %q", ErrExecutionNotFound, open.GetExecutionId())
	}
	// The transport-verified sender is the only authority for ownership: the
	// declared peer key is bound only because the authenticated owner asked
	// for it, never because a payload claimed it.
	if !bytes.Equal(record.client, envelope.GetSender()) {
		a.mu.Unlock()
		return nil, ErrUnauthorized
	}
	if protocol.Terminal(record.phase) {
		a.mu.Unlock()
		return nil, fmt.Errorf("%w: execution %q is %s", ErrInvalidTransition, record.id, record.phase)
	}
	a.mu.Unlock()

	if !a.tunnelConfig.Enabled {
		return nil, ErrTunnelDisabled
	}
	// Validate and convert the client-supplied container port list. The
	// allocator does not resolve targets from its own configuration; it binds
	// whatever the client sent, validated only for well-formedness.
	targets, err := protocol.ValidateTunnelTargets(open.GetTargets())
	if err != nil {
		return nil, err
	}
	endpoint := a.tunnelEndpoint()
	if len(endpoint.Address) == 0 || len(endpoint.PubKey) == 0 {
		return nil, ErrTunnelNoEndpoint
	}

	if err := a.tunnels.Bind(open.GetExecutionId(), peerKey, targets, endpoint); err != nil {
		return nil, err
	}
	messageID := a.newID()
	if messageID == "" {
		return nil, fmt.Errorf("%w: empty message ID", ErrInvalidConfig)
	}
	response := &r1sv1.Envelope{
		MessageId:     messageID,
		Sender:        bytes.Clone(a.identity),
		CorrelationId: envelope.GetMessageId(),
		SentAt:        timestamppb.New(now),
		Payload: &r1sv1.Envelope_ExecutionTunnelOpenAck{ExecutionTunnelOpenAck: &r1sv1.ExecutionTunnelOpenAck{
			ExecutionId:             open.GetExecutionId(),
			AllocatorEndpoint:       bytes.Clone(endpoint.Address),
			AllocatorEndpointPubkey: bytes.Clone(endpoint.PubKey),
			Targets:                 protocol.TargetsToProto(targets),
		}},
	}
	return []*r1sv1.Envelope{response}, nil
}

func (a *Allocator) tunnelEndpoint() tunnel.Endpoint {
	return tunnel.Endpoint{
		Address: bytes.Clone(a.tunnelConfig.Endpoint.Address),
		PubKey:  bytes.Clone(a.tunnelConfig.Endpoint.PubKey),
	}
}

// CloseTunnel releases an edge's session without changing execution lifetime.
func (a *Allocator) CloseTunnel(session *tunnel.Session) { a.tunnels.Release(session) }

// DialTunnelTarget revalidates execution authority at each splice, then delegates
// network namespace resolution to the runtime. The edge cancels ctx on session
// close and owns closing the returned connection on revocation.
func (a *Allocator) DialTunnelTarget(ctx context.Context, session *tunnel.Session, port uint16) (net.Conn, error) {
	a.mu.Lock()
	if !a.tunnels.Active(session) {
		a.mu.Unlock()
		return nil, ErrUnauthorized
	}
	record := a.executions[session.ExecutionID]
	if record == nil || protocol.Terminal(record.phase) || record.phase == r1sv1.ExecutionPhase_EXECUTION_PHASE_CANCELLING {
		a.mu.Unlock()
		return nil, ErrInvalidTransition
	}
	if _, ok := session.ResolveTarget(port); !ok {
		a.mu.Unlock()
		return nil, ErrUnauthorized
	}
	dialer, ok := a.runtime.(r1sruntime.PortDialer)
	a.mu.Unlock()
	if !ok {
		return nil, r1sruntime.ErrTunnelUnsupported
	}
	conn, err := dialer.DialExecution(ctx, session.ExecutionID, port)
	if err != nil {
		return nil, err
	}
	// A completion during a slow dial cannot return a usable connection.
	if !a.tunnels.Active(session) {
		conn.Close()
		return nil, ErrInvalidTransition
	}
	return conn, nil
}

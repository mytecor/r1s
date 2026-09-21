package allocator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
	r1sruntime "github.com/mytecor/r1s/internal/runtime"
	"github.com/mytecor/r1s/internal/tunnel"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TunnelConfig is the allocator-local authorization surface for F14
// direct-access tunnels. It carries no Yggdrasil-specific type: the endpoint
// advertisement is opaque bytes supplied by whatever edge is running inside
// r1sd, so the allocator core and the control protocol stay transport-neutral.
type TunnelConfig struct {
	// Enabled turns on direct-access tunneling. When disabled, any mint request
	// fails with a clear CommandError.
	Enabled bool
	// GrantTTL bounds a minted grant. Expiry is evaluated lazily at mint and at
	// accept; there is no TTL sweeper goroutine. Zero uses the default.
	GrantTTL time.Duration
	// Endpoint is the allocator's transport-neutral overlay advertisement
	// returned in every minted grant ack. The allocator edge (F14-02) supplies
	// it when it starts; without it a mint fails as "endpoint not ready".
	Endpoint tunnel.Endpoint
}

const defaultTunnelGrantTTL = 5 * time.Minute

// DefaultTunnelGrantTTL is the default lifetime of a minted tunnel grant. It is
// exported so the allocator daemon can surface it as the flag default.
const DefaultTunnelGrantTTL = defaultTunnelGrantTTL

// tunnel errors map to explicit CommandError codes in errorResponse.
var (
	ErrTunnelDisabled   = errors.New("direct-access tunnels are disabled on this allocator")
	ErrTunnelNoEndpoint = errors.New("tunnel endpoint is not ready")
)

// AcceptTunnel is the allocator-side accept-time authorization the edge (F14-02)
// calls before splicing a tunnel stream: it re-validates that the execution is
// still live and that the authenticated peer key matches the pinned grant, then
// consumes the grant and opens the execution's single session. A failed
// validation leaves the grant unconsumed and reusable until expiry.
func (a *Allocator) AcceptTunnel(executionID, grantID string, peerKey []byte) (*tunnel.Session, error) {
	a.mu.Lock()
	record, ok := a.executions[executionID]
	if !ok {
		a.mu.Unlock()
		return nil, fmt.Errorf("%w: %q", ErrExecutionNotFound, executionID)
	}
	if terminal(record.phase) {
		a.mu.Unlock()
		return nil, fmt.Errorf("%w: execution %q is %s", ErrInvalidTransition, record.id, record.phase)
	}
	a.mu.Unlock()

	// The grant-to-owner binding was established at mint by the authenticated
	// sender; here the edge's authenticated peer key must match the pinned key.
	session, err := a.tunnels.Accept(executionID, grantID, peerKey, a.now().UTC())
	if err != nil {
		return nil, err
	}
	// A splice opens only for a live execution: re-check under the lock so a
	// terminal transition committed between Accept and splice cannot leak a
	// session onto a dead execution.
	a.mu.Lock()
	defer a.mu.Unlock()
	record = a.executions[executionID]
	if record == nil || terminal(record.phase) {
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

// handleTunnelGrant mints an execution-scoped, single-use access grant for the
// authenticated execution owner. It binds execution ID, owner (the verified
// sender), the client's edge node public key, and an expiry to the grant, and
// returns the allocator-local endpoint advertisement. Minting is cheap and
// cancellation happens at accept time (one live session per execution), so a
// repeat mint replaces an outstanding unconfirmed grant. The client supplies
// the destination slot list; the allocator validates only its shape and binds
// it into the grant unchanged.
func (a *Allocator) handleTunnelGrant(envelope *r1sv1.Envelope, grant *r1sv1.ExecutionTunnelGrant) ([]*r1sv1.Envelope, error) {
	peerKey := grant.GetYggPeerPubkey()
	now := a.now().UTC()
	a.mu.Lock()
	record, ok := a.executions[grant.GetExecutionId()]
	if !ok {
		a.mu.Unlock()
		return nil, fmt.Errorf("%w: %q", ErrExecutionNotFound, grant.GetExecutionId())
	}
	if !bytes.Equal(record.client, envelope.GetSender()) {
		a.mu.Unlock()
		return nil, ErrUnauthorized
	}
	if terminal(record.phase) {
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
	targets, err := protocol.ValidateTunnelTargets(grant.GetTargets())
	if err != nil {
		return nil, err
	}
	endpoint := a.tunnelEndpoint()
	if len(endpoint.Address) == 0 || len(endpoint.PubKey) == 0 {
		return nil, ErrTunnelNoEndpoint
	}

	minted, err := a.tunnels.Mint(grant.GetExecutionId(), peerKey, targets, endpoint, a.tunnelGrantTTL(), now)
	if err != nil {
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
		Payload: &r1sv1.Envelope_ExecutionTunnelGrantAck{ExecutionTunnelGrantAck: &r1sv1.ExecutionTunnelGrantAck{
			ExecutionId:             minted.ExecutionID,
			GrantId:                 minted.ID,
			ExpiresAt:               timestamppb.New(minted.ExpiresAt),
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

// tunnelGrantTTL returns the configured grant TTL, or the default when unset.
func (a *Allocator) tunnelGrantTTL() time.Duration {
	if a.tunnelConfig.GrantTTL <= 0 {
		return defaultTunnelGrantTTL
	}
	return a.tunnelConfig.GrantTTL
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
	if record == nil || terminal(record.phase) || record.phase == r1sv1.ExecutionPhase_EXECUTION_PHASE_CANCELLING {
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

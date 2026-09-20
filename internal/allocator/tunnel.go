package allocator

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
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
	// TargetByClass resolves the allocator-local target slot list at grant
	// time from allocator-local configuration. A workload on the host network
	// namespace has no free "the running execution" address, so every slot is
	// defined here, never a client-supplied destination and never
	// per-execution metadata.
	TargetByClass map[string][]tunnel.Target
	// DefaultTarget is the mandatory fallback slot used when no class matches
	// and a stream carries no target_slot (the interactive pipe). A missing
	// default fails the mint with a clear error instead of connecting by
	// guesswork.
	DefaultTarget *tunnel.Target
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
	ErrTunnelNoTarget   = errors.New("no tunnel target configured for this resource class")
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
// repeat mint replaces an outstanding unconfirmed grant.
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
	resourceClass := record.resourceClass
	a.mu.Unlock()

	if !a.tunnelsEnabled() {
		return nil, ErrTunnelDisabled
	}
	targets, defaultTarget, err := a.resolveTunnelTargets(resourceClass)
	if err != nil {
		return nil, err
	}
	endpoint := a.tunnelEndpoint()
	if len(endpoint.Address) == 0 || len(endpoint.PubKey) == 0 {
		return nil, ErrTunnelNoEndpoint
	}

	minted, err := a.tunnels.Mint(grant.GetExecutionId(), peerKey, targets, defaultTarget, endpoint, a.tunnelGrantTTL(), now)
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
		}},
	}
	return []*r1sv1.Envelope{response}, nil
}

func (a *Allocator) tunnelsEnabled() bool {
	config := a.tunnelConfig
	return config.Enabled
}

func (a *Allocator) tunnelEndpoint() tunnel.Endpoint {
	return tunnel.Endpoint{
		Address: bytes.Clone(a.tunnelConfig.Endpoint.Address),
		PubKey:  bytes.Clone(a.tunnelConfig.Endpoint.PubKey),
	}
}

func (a *Allocator) tunnelGrantTTL() time.Duration {
	if a.tunnelConfig.GrantTTL <= 0 {
		return defaultTunnelGrantTTL
	}
	return a.tunnelConfig.GrantTTL
}

// resolveTunnelTargets resolves the target slot list for a resource class at
// grant time: the per-class list wins, the mandatory default slot is the
// fallback, and a miss fails clearly instead of connecting by guesswork. The
// returned default target must name a real, usable endpoint (or be an explicit
// member of the list) so a stream with no target_slot still splices
// somewhere controlled.
func (a *Allocator) resolveTunnelTargets(resourceClass string) ([]tunnel.Target, tunnel.Target, error) {
	config := a.tunnelConfig
	if slots, ok := config.TargetByClass[resourceClass]; ok && len(slots) > 0 {
		return cloneTargetsForAllocator(slots), slotDefault(slots), nil
	}
	if config.DefaultTarget != nil && usableTarget(*config.DefaultTarget) {
		return []tunnel.Target{*config.DefaultTarget}, *config.DefaultTarget, nil
	}
	return nil, tunnel.Target{}, ErrTunnelNoTarget
}

// usableTarget reports whether a target slot points at a concrete endpoint.
func usableTarget(target tunnel.Target) bool {
	return strings.TrimSpace(target.Host) != "" && target.Port != 0
}

// cloneTargetsForAllocator deep-copies a target slot list so the allocator
// never aliases its configuration slice into a minted grant.
func cloneTargetsForAllocator(slots []tunnel.Target) []tunnel.Target {
	cloned := make([]tunnel.Target, len(slots))
	copy(cloned, slots)
	return cloned
}

// slotDefault returns the default slot for a per-class slot list: the
// unnamed (empty ID) slot if present, else a copy of the first slot. A stream
// with no target_slot splices to this slot.
func slotDefault(slots []tunnel.Target) tunnel.Target {
	for _, slot := range slots {
		if slot.ID == "" {
			return slot
		}
	}
	if len(slots) > 0 {
		return slots[0]
	}
	return tunnel.Target{}
}

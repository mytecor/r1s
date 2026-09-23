package protocol

import (
	"strings"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/tunnel"
)

// validateTunnelGrant validates a client's request to mint a direct-access
// tunnel grant. The peer node public key is an opaque value; it is required
// because the allocator pins it into the grant (nothing secret is embedded,
// but the key must be present to authorize the tunnel edge at connect time).
func validateTunnelGrant(grant *r1sv1.ExecutionTunnelGrant) error {
	if grant == nil {
		return invalid("execution_tunnel_grant", "is required")
	}
	if strings.TrimSpace(grant.GetExecutionId()) == "" {
		return invalid("execution_tunnel_grant.execution_id", "is required")
	}
	if len(grant.GetYggPeerPubkey()) == 0 {
		return invalid("execution_tunnel_grant.ygg_peer_pubkey", "is required")
	}
	if len(grant.GetYggPeerPubkey()) > tunnel.MaxPeerKeySize {
		return invalid("execution_tunnel_grant.ygg_peer_pubkey", "is too large")
	}
	if len(grant.GetTargets()) == 0 {
		return invalid("execution_tunnel_grant.targets", "is required")
	}
	if _, err := ValidateTunnelTargets(grant.GetTargets()); err != nil {
		return err
	}
	return nil
}

// validateTunnelGrantAck validates the allocator's mint reply. The endpoint
// advertisement is opaque transport-neutral bytes: it may be empty on an older
// peer that acknowledges a grant before its edge advertises an endpoint, so
// only the structural fields are enforced here. The ontology of "grant bound to
// the wrong execution" lives in the allocator, not in wire validation: the ack
// echoes the execution the allocator minted for.
func validateTunnelGrantAck(ack *r1sv1.ExecutionTunnelGrantAck) error {
	if ack == nil {
		return invalid("execution_tunnel_grant_ack", "is required")
	}
	if strings.TrimSpace(ack.GetExecutionId()) == "" {
		return invalid("execution_tunnel_grant_ack.execution_id", "is required")
	}
	if strings.TrimSpace(ack.GetGrantId()) == "" {
		return invalid("execution_tunnel_grant_ack.grant_id", "is required")
	}
	if err := validTimestamp("execution_tunnel_grant_ack.expires_at", ack.GetExpiresAt()); err != nil {
		return err
	}
	return nil
}

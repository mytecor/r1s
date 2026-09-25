package protocol

import (
	"strings"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/tunnel"
)

// validateTunnelOpen validates a client's declaration of its tunnel binding to
// an execution (F22-06). It replaces the retired ExecutionTunnelGrant: there is
// no grant token, no grant ID, and no TTL. The transport-verified sender is the
// authenticated owner; the declared edge node public key is an opaque value the
// allocator binds to the execution so a mesh peer presenting that key is
// authorized directly. The peer key is required (it is the binding the
// allocator authorizes), but it is not a bearer secret: it only binds the
// mesh-authenticated peer, and the client holding the corresponding private key
// is what proves ownership.
func validateTunnelOpen(open *r1sv1.ExecutionTunnelOpen) error {
	if open == nil {
		return invalid("execution_tunnel_open", "is required")
	}
	if strings.TrimSpace(open.GetExecutionId()) == "" {
		return invalid("execution_tunnel_open.execution_id", "is required")
	}
	if len(open.GetYggPeerPubkey()) == 0 {
		return invalid("execution_tunnel_open.ygg_peer_pubkey", "is required")
	}
	if len(open.GetYggPeerPubkey()) > tunnel.MaxPeerKeySize {
		return invalid("execution_tunnel_open.ygg_peer_pubkey", "is too large")
	}
	if len(open.GetTargets()) == 0 {
		return invalid("execution_tunnel_open.targets", "is required")
	}
	if _, err := ValidateTunnelTargets(open.GetTargets()); err != nil {
		return err
	}
	return nil
}

// validateTunnelOpenAck validates the allocator's reply to an ExecutionTunnelOpen.
// The endpoint advertisement is opaque transport-neutral bytes: it may be empty
// on an older peer that acknowledges an open before its edge advertises an
// endpoint, so only the structural fields are enforced here. The ontology of
// "open bound to the wrong execution" lives in the allocator, not in wire
// validation: the ack echoes the execution the allocator bound.
func validateTunnelOpenAck(ack *r1sv1.ExecutionTunnelOpenAck) error {
	if ack == nil {
		return invalid("execution_tunnel_open_ack", "is required")
	}
	if strings.TrimSpace(ack.GetExecutionId()) == "" {
		return invalid("execution_tunnel_open_ack.execution_id", "is required")
	}
	return nil
}

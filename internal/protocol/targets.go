package protocol

import (
	"fmt"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/tunnel"
)

// ValidateTunnelTargets validates a client-supplied container port list and
// converts it to the internal tunnel.Target representation. Each target must
// name a positive container port in [1, 65535]; a zero or out-of-range port is
// invalid. At least one target is required. There are no named slots: each
// target is just the container port to export, resolved on the allocator
// loopback.
func ValidateTunnelTargets(protoTargets []*r1sv1.TunnelTarget) ([]tunnel.Target, error) {
	if len(protoTargets) == 0 {
		return nil, fmt.Errorf("%w: no tunnel targets supplied", ErrInvalidEnvelope)
	}
	targets := make([]tunnel.Target, len(protoTargets))
	for i, pt := range protoTargets {
		port := pt.GetPort()
		if port == 0 {
			return nil, fmt.Errorf("%w: tunnel target[%d]: port must be positive", ErrInvalidEnvelope, i)
		}
		if port > 0xFFFF {
			return nil, fmt.Errorf("%w: tunnel target[%d]: port too large", ErrInvalidEnvelope, i)
		}
		targets[i] = tunnel.Target{Port: uint16(port)}
	}
	return targets, nil
}

// TargetsToProto converts a list of internal tunnel targets (container ports)
// to the protobuf representation. It is used by the client to populate the
// grant request and by the allocator to echo the port list in the grant ack.
func TargetsToProto(targets []tunnel.Target) []*r1sv1.TunnelTarget {
	if targets == nil {
		return nil
	}
	proto := make([]*r1sv1.TunnelTarget, len(targets))
	for i, t := range targets {
		proto[i] = &r1sv1.TunnelTarget{Port: uint32(t.Port)}
	}
	return proto
}

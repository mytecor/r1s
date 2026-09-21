package protocol

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/tunnel"
)

// ValidateTunnelTargets validates a client-supplied target slot list and
// converts it to the internal tunnel.Target representation. Each slot must
// name a concrete host:port endpoint; an empty host or a zero port is invalid.
// At least one target is required. host may be a hostname or an IP literal.
func ValidateTunnelTargets(protoTargets []*r1sv1.TunnelTarget) ([]tunnel.Target, error) {
	if len(protoTargets) == 0 {
		return nil, fmt.Errorf("%w: no tunnel targets supplied", ErrInvalidEnvelope)
	}
	targets := make([]tunnel.Target, len(protoTargets))
	for i, pt := range protoTargets {
		host := strings.TrimSpace(pt.GetHost())
		port := pt.GetPort()
		if host == "" {
			return nil, fmt.Errorf("%w: tunnel target[%d]: host is required", ErrInvalidEnvelope, i)
		}
		if port == 0 {
			return nil, fmt.Errorf("%w: tunnel target[%d]: port must be a positive uint16", ErrInvalidEnvelope, i)
		}
		if port > 0xFFFF {
			return nil, fmt.Errorf("%w: tunnel target[%d]: port too large", ErrInvalidEnvelope, i)
		}
		// Validate the host:port shape with net.SplitHostPort so malformed
		// addresses (missing port, wrong format) are rejected. host may be an
		// IPv6 literal, a hostname, or an IPv4 address.
		if _, _, err := net.SplitHostPort(net.JoinHostPort(host, strconv.FormatUint(uint64(port), 10))); err != nil {
			return nil, fmt.Errorf("%w: tunnel target[%d]: %s", ErrInvalidEnvelope, i, err.Error())
		}
		targets[i] = tunnel.Target{ID: strings.TrimSpace(pt.GetName()), Host: host, Port: uint16(port)}
	}
	return targets, nil
}

// TargetsToProto converts a list of internal tunnel targets to the protobuf
// representation. It is used by the client to populate the grant request and
// by the allocator to echo the slot list in the grant ack.
func TargetsToProto(targets []tunnel.Target) []*r1sv1.TunnelTarget {
	if targets == nil {
		return nil
	}
	proto := make([]*r1sv1.TunnelTarget, len(targets))
	for i, t := range targets {
		proto[i] = &r1sv1.TunnelTarget{Name: t.ID, Host: t.Host, Port: uint32(t.Port)}
	}
	return proto
}

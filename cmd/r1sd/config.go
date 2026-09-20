package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strconv"
	"strings"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/cluster"
	"github.com/mytecor/r1s/internal/protocol"
	"github.com/mytecor/r1s/internal/transport/rns"
	"github.com/mytecor/r1s/internal/tunnel"
	"google.golang.org/protobuf/encoding/protojson"
)

func allocatorClusterSource(value string) (string, error) {
	if strings.TrimSpace(value) != "" {
		return value, nil
	}
	return cluster.DefaultPath()
}

// parseNodeCapabilities decodes and validates an optional --node JSON
// advertisement. An empty value means no placement advertisement. The decoded
// message is validated against the same bounded-capability contract the
// protocol enforces, so an oversized or malformed advertisement fails startup
// instead of being propagated.
func parseNodeCapabilities(value string) (*r1sv1.NodeCapabilities, error) {
	if strings.TrimSpace(value) == "" {
		return nil, nil
	}
	node := new(r1sv1.NodeCapabilities)
	if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal([]byte(value), node); err != nil {
		return nil, fmt.Errorf("--node: decode JSON: %w", err)
	}
	if err := protocol.ValidateCapabilities(node); err != nil {
		return nil, fmt.Errorf("--node: %w", err)
	}
	return node, nil
}

func identityDataDirectory(source string) (string, error) {
	if !rns.IsInlineIdentitySource(source) {
		return filepath.Dir(source), nil
	}
	defaultClusterPath, err := cluster.DefaultPath()
	if err != nil {
		return "", err
	}
	return filepath.Dir(defaultClusterPath), nil
}

func parseCapacity(value string) (map[string]uint32, error) {
	capacity := make(map[string]uint32)
	for _, entry := range strings.Split(value, ",") {
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" {
			return nil, fmt.Errorf("invalid capacity %q: expected class=slots", entry)
		}
		slots, err := strconv.ParseUint(strings.TrimSpace(parts[1]), 10, 32)
		if err != nil || slots == 0 {
			return nil, fmt.Errorf("invalid capacity %q: slots must be a positive uint32", entry)
		}
		class := strings.TrimSpace(parts[0])
		if _, duplicate := capacity[class]; duplicate {
			return nil, fmt.Errorf("invalid capacity %q: duplicate class", entry)
		}
		capacity[class] = uint32(slots)
	}
	return capacity, nil
}

// ParseTunnelTargets parses a comma-separated per-resource-class tunnel target
// map, for example "default=127.0.0.1:9000,gpu=127.0.0.1:9001". The target is
// allocator-local and resolved at grant time; it is never a client-supplied
// destination.
func ParseTunnelTargets(value string) (map[string]tunnel.Target, error) {
	targets := make(map[string]tunnel.Target)
	if strings.TrimSpace(value) == "" {
		return targets, nil
	}
	for _, entry := range strings.Split(value, ",") {
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" {
			return nil, fmt.Errorf("invalid tunnel target %q: expected class=host:port", entry)
		}
		target, err := ParseTunnelTarget(parts[1])
		if err != nil {
			return nil, fmt.Errorf("invalid tunnel target %q: %w", entry, err)
		}
		class := strings.TrimSpace(parts[0])
		if _, duplicate := targets[class]; duplicate {
			return nil, fmt.Errorf("invalid tunnel target %q: duplicate class", entry)
		}
		targets[class] = target
	}
	return targets, nil
}

// ParseTunnelTarget parses a single host:port target. A missing port is an
// error: the tunnel must always terminate at a concrete local endpoint.
func ParseTunnelTarget(value string) (tunnel.Target, error) {
	host, portText, err := net.SplitHostPort(strings.TrimSpace(value))
	if err != nil {
		return tunnel.Target{}, fmt.Errorf("expected host:port")
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil || port == 0 {
		return tunnel.Target{}, fmt.Errorf("port must be a positive uint16")
	}
	if strings.TrimSpace(host) == "" {
		return tunnel.Target{}, fmt.Errorf("host is required")
	}
	return tunnel.Target{Host: host, Port: uint16(port)}, nil
}

// parseHexBytes decodes an opaque hex-encoded value, allowing an empty string.
// Used for the transport-neutral tunnel endpoint advertisement flags.
func parseHexBytes(name, value string) ([]byte, error) {
	if strings.TrimSpace(value) == "" {
		return nil, nil
	}
	decoded, err := hex.DecodeString(strings.TrimSpace(value))
	if err != nil {
		return nil, fmt.Errorf("--%s must be hex-encoded bytes: %w", name, err)
	}
	return decoded, nil
}

func newFlagSet(name string, output io.Writer) *flag.FlagSet {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(output)
	flags.Usage = func() {
		fmt.Fprintf(output, "Usage of %s:\n", name)
		flags.VisitAll(func(candidate *flag.Flag) {
			fmt.Fprintf(output, "  --%s value\n    \t%s", candidate.Name, candidate.Usage)
			if candidate.DefValue != "" && candidate.DefValue != "false" && candidate.DefValue != "0" {
				fmt.Fprintf(output, " (default %s)", strconv.Quote(candidate.DefValue))
			}
			fmt.Fprintln(output)
		})
	}
	return flags
}

// tunnelPeerList parses a comma-separated bootstrap peer URI list for the
// tunnel edge. Empty entries are dropped; an empty value joins the standard
// public Yggdrasil overlay. Peering is edge configuration, never a protocol
// feature.
func tunnelPeerList(value string) []string {
	var peers []string
	for _, peer := range strings.Split(value, ",") {
		if trimmed := strings.TrimSpace(peer); trimmed != "" {
			peers = append(peers, trimmed)
		}
	}
	return peers
}

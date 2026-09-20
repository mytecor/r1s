package protocol

import (
	"regexp"
	"slices"
	"strings"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
)

// Bounds for node capability metadata. Every value is deliberately small so a
// node advertisement always fits the RNS announce app-data budget (255 bytes)
// and a client request stays within the command envelope size. Capability
// identity strings are lowercase ASCII; anything else is rejected by Validate.
const (
	MaxCapabilityKeyLen     = 64
	MaxCapabilityValueLen   = 128
	MaxCapabilityLabels     = 8
	MaxCapabilityDevices    = 8
	MaxCapabilityProfiles   = 16
	MaxCapabilityCache      = 16
	MaxCapabilityCacheImage = 256
)

var capabilityKey = regexp.MustCompile(`^[a-z0-9][a-z0-9._/-]*$`)
var capabilityValue = regexp.MustCompile(`^[a-z0-9][a-z0-9._/:+-]*$`)

// ValidateCapabilities bounds node capability metadata. It is applied by the
// allocator when it builds its advertisement and by the transport when it
// parses a descriptor; a malformed or oversized advertisement is rejected
// rather than trusted.
func ValidateCapabilities(node *r1sv1.NodeCapabilities) error {
	if node == nil {
		return nil
	}
	for field, value := range map[string]string{
		"os": node.GetOs(), "arch": node.GetArch(), "runtime": node.GetRuntime(),
	} {
		if value == "" {
			continue
		}
		if len(value) > MaxCapabilityValueLen || !capabilityValue.MatchString(value) {
			return invalid("node_capabilities."+field, "must be lowercase ASCII and bounded")
		}
	}
	if len(node.GetCachedImages()) > MaxCapabilityCache {
		return invalid("node_capabilities.cached_images", "too many entries")
	}
	for _, image := range node.GetCachedImages() {
		if len(image) == 0 || len(image) > MaxCapabilityCacheImage {
			return invalid("node_capabilities.cached_images", "entry must be a bounded image reference")
		}
	}
	if len(node.GetDevices()) > MaxCapabilityDevices {
		return invalid("node_capabilities.devices", "too many devices")
	}
	for _, device := range node.GetDevices() {
		if len(device) == 0 || len(device) > MaxCapabilityValueLen || !capabilityValue.MatchString(device) {
			return invalid("node_capabilities.devices", "each device must be a lowercase ASCII name")
		}
	}
	if len(node.GetResourceProfiles()) > MaxCapabilityProfiles {
		return invalid("node_capabilities.resource_profiles", "too many profiles")
	}
	for _, profile := range node.GetResourceProfiles() {
		if len(profile) == 0 || len(profile) > MaxCapabilityValueLen || !capabilityValue.MatchString(profile) {
			return invalid("node_capabilities.resource_profiles", "each profile must be a bounded lowercase name")
		}
	}
	if len(node.GetLabels()) > MaxCapabilityLabels {
		return invalid("node_capabilities.labels", "too many labels")
	}
	for key, value := range node.GetLabels() {
		if len(key) == 0 || len(key) > MaxCapabilityKeyLen || !capabilityKey.MatchString(key) {
			return invalid("node_capabilities.labels", "each label key must be lowercase ASCII and bounded")
		}
		if len(value) > MaxCapabilityValueLen || !capabilityValue.MatchString(value) {
			return invalid("node_capabilities.labels", "each label value must be bounded")
		}
	}
	return nil
}

// ValidateConstraints bounds and normalizes placement constraints. Constraints
// travel in client requests, so they are bounded against the same budget as
// capabilities: they can only ask for a subset of what a node may declare.
func ValidateConstraints(constraints *r1sv1.PlacementConstraints) error {
	if constraints == nil {
		return nil
	}
	for field, value := range map[string]string{
		"os": constraints.GetOs(), "arch": constraints.GetArch(), "runtime": constraints.GetRuntime(),
	} {
		if value != "" {
			if len(value) > MaxCapabilityValueLen || !capabilityValue.MatchString(value) {
				return invalid("placement_constraints."+field, "must be lowercase ASCII and bounded")
			}
		}
	}
	if len(constraints.GetDevices()) > MaxCapabilityDevices {
		return invalid("placement_constraints.devices", "too many devices")
	}
	for _, device := range constraints.GetDevices() {
		if len(device) == 0 || len(device) > MaxCapabilityValueLen || !capabilityValue.MatchString(device) {
			return invalid("placement_constraints.devices", "each device must be a lowercase ASCII name")
		}
	}
	if len(constraints.GetLabels()) > MaxCapabilityLabels {
		return invalid("placement_constraints.labels", "too many labels")
	}
	for key, value := range constraints.GetLabels() {
		if len(key) == 0 || len(key) > MaxCapabilityKeyLen || !capabilityKey.MatchString(key) {
			return invalid("placement_constraints.labels", "each label key must be lowercase ASCII and bounded")
		}
		if len(value) > MaxCapabilityValueLen || !capabilityValue.MatchString(value) {
			return invalid("placement_constraints.labels", "each label value must be bounded")
		}
	}
	return nil
}

// PlacementCompatible is the advisory companion to PlacementMatches, used by
// the client when deciding whether to send a request to a discovered allocator.
// A nil node is "unknown", not "incompatible": an allocator that advertises no
// capabilities is still tried, and its local admission is the authority. Only a
// node with positive evidence of mismatch is skipped. Empty constraints always
// match.
func PlacementCompatible(constraints *r1sv1.PlacementConstraints, node *r1sv1.NodeCapabilities) bool {
	if constraints == nil {
		return true
	}
	if len(constraints.GetOs())+len(constraints.GetArch())+len(constraints.GetRuntime())+len(constraints.GetLabels())+len(constraints.GetDevices()) == 0 {
		return true
	}
	if node == nil {
		return true
	}
	return PlacementMatches(constraints, node)
}
func PlacementMatches(constraints *r1sv1.PlacementConstraints, node *r1sv1.NodeCapabilities) bool {
	if constraints == nil {
		return true
	}
	if node == nil {
		return false
	}
	switch {
	case constraints.GetOs() != "" && constraints.GetOs() != node.GetOs():
		return false
	case constraints.GetArch() != "" && constraints.GetArch() != node.GetArch():
		return false
	case constraints.GetRuntime() != "" && constraints.GetRuntime() != node.GetRuntime():
		return false
	}
	for key, value := range constraints.GetLabels() {
		if node.GetLabels()[key] != value {
			return false
		}
	}
	for _, required := range constraints.GetDevices() {
		if !contains(node.GetDevices(), required) {
			return false
		}
	}
	return true
}

// PlacementConflictDetails describes why a node does not satisfy constraints,
// for the explicit INCOMPATIBLE rejection detail. It must never contain
// workload output or secrets.
func PlacementConflictDetails(constraints *r1sv1.PlacementConstraints, node *r1sv1.NodeCapabilities) string {
	if constraints == nil || node == nil {
		return "node satisfies no placement constraints"
	}
	var missing []string
	if constraints.GetOs() != "" && constraints.GetOs() != node.GetOs() {
		missing = append(missing, "os="+constraints.GetOs())
	}
	if constraints.GetArch() != "" && constraints.GetArch() != node.GetArch() {
		missing = append(missing, "arch="+constraints.GetArch())
	}
	if constraints.GetRuntime() != "" && constraints.GetRuntime() != node.GetRuntime() {
		missing = append(missing, "runtime="+constraints.GetRuntime())
	}
	for key, value := range constraints.GetLabels() {
		if node.GetLabels()[key] != value {
			missing = append(missing, "label="+key+"="+value)
		}
	}
	for _, required := range constraints.GetDevices() {
		if !contains(node.GetDevices(), required) {
			missing = append(missing, "device="+required)
		}
	}
	if len(missing) == 0 {
		return "node satisfies no placement constraints"
	}
	return "node does not satisfy placement constraints: " + strings.Join(missing, ",")
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

// Contains reports whether values holds target. It is the small set-membership
// helper for bounded capability lists, shared by the allocator.
func Contains(values []string, target string) bool {
	return contains(values, target)
}

// CapabilitiesEqual reports deep equality of two capability sets. It is used
// by the client to detect a changed advertisement and by tests.
func CapabilitiesEqual(left, right *r1sv1.NodeCapabilities) bool {
	if left == nil || right == nil {
		return left == right
	}
	if left.GetOs() != right.GetOs() || left.GetArch() != right.GetArch() || left.GetRuntime() != right.GetRuntime() {
		return false
	}
	if !slices.Equal(left.GetDevices(), right.GetDevices()) {
		return false
	}
	if !slices.Equal(left.GetResourceProfiles(), right.GetResourceProfiles()) {
		return false
	}
	if !slices.Equal(left.GetCachedImages(), right.GetCachedImages()) {
		return false
	}
	if len(left.GetLabels()) != len(right.GetLabels()) {
		return false
	}
	for key, value := range left.GetLabels() {
		if right.GetLabels()[key] != value {
			return false
		}
	}
	return true
}

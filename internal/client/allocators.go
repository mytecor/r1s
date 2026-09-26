package client

import (
	"bytes"
	"encoding/hex"
	"sort"
	"strings"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
	"google.golang.org/protobuf/proto"
)

// RegisterAllocator records a verified discovery or authenticated session route.
func (o *Client) RegisterAllocator(candidate Allocator) error {
	if len(candidate.Identity) == 0 || strings.TrimSpace(candidate.Destination) == "" {
		return ErrInvalidAllocator
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	previous, existed := o.allocators.lookup(candidate.Identity)
	if existed && candidate.Capacity == nil {
		candidate.Capacity = previous.Capacity
		candidate.Hops = previous.Hops
	}
	if existed && candidate.Node == nil {
		candidate.Node = previous.Node
	}
	// A registration that carries no tunnel advertisement (for example one
	// learned from an authenticated outbound session, whose descriptor was
	// never seen) preserves the previous tunnel advertisement rather than
	// blanking it. A discovery that explicitly advertises no tunnel edge would
	// arrive with TunnelPort == 0, which is indistinguishable here; the
	// descriptor is re-discovered on the next announce, so this is only ever
	// advisory staleness, matching Node's semantics.
	if existed && candidate.TunnelPort == 0 {
		candidate.TunnelHost = previous.TunnelHost
		candidate.TunnelPort = previous.TunnelPort
		candidate.TunnelDestination = previous.TunnelDestination
	}
	o.allocators.put(candidate)
	return nil
}

// AllocatorDestinations returns every currently known allocator that may
// satisfy the resource class and placement constraints. The sorted result is
// discovery state, not pinning: a fresh attempt remains free to select any
// allocator that offers, including later discoveries.
func (o *Client) AllocatorDestinations(resourceClass string, constraints *r1sv1.PlacementConstraints) []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	var destinations []string
	for _, allocator := range o.allocators.all() {
		if allocator.Capacity[resourceClass] == 0 || !protocol.PlacementCompatible(constraints, allocator.Node) {
			continue
		}
		destinations = append(destinations, allocator.Destination)
	}
	sort.Strings(destinations)
	return destinations
}

// allocatorCatalog owns authenticated allocator routes and the deterministic
// offer-ranking policy. The Client remains responsible for durable workflow
// transitions; callers must hold Client.mu while using the catalog.
type allocatorCatalog struct {
	byIdentity map[string]Allocator
}

func newAllocatorCatalog() allocatorCatalog {
	return allocatorCatalog{byIdentity: make(map[string]Allocator)}
}

func (c *allocatorCatalog) lookup(identity []byte) (Allocator, bool) {
	value, ok := c.byIdentity[hex.EncodeToString(identity)]
	return value, ok
}

func (c *allocatorCatalog) put(candidate Allocator) {
	c.byIdentity[hex.EncodeToString(candidate.Identity)] = cloneAllocator(candidate)
}

func (c *allocatorCatalog) remove(identity []byte) {
	delete(c.byIdentity, hex.EncodeToString(identity))
}

func (c *allocatorCatalog) all() []Allocator {
	result := make([]Allocator, 0, len(c.byIdentity))
	for _, candidate := range c.byIdentity {
		result = append(result, cloneAllocator(candidate))
	}
	return result
}

func (c *allocatorCatalog) choose(offers map[string]*offerRecord, now time.Time) (*offerRecord, Allocator, bool) {
	return c.chooseWithPreferences(offers, now, nil, "")
}

// chooseWithPreferences ranks live offers, optionally narrowing to nodes that
// satisfy placement constraints and preferring allocators whose advertised
// image-cache already holds the workload image (a pure advisory hint, never an
// authority). An empty constraint set matches every node.
func (c *allocatorCatalog) chooseWithPreferences(offers map[string]*offerRecord, now time.Time, constraints *r1sv1.PlacementConstraints, image string) (*offerRecord, Allocator, bool) {
	candidates := make([]*offerRecord, 0, len(offers))
	for _, offer := range offers {
		allocator, ok := c.lookup(offer.allocatorID)
		if !ok || strings.TrimSpace(allocator.Destination) == "" || !offer.offer.GetExpiresAt().AsTime().After(now) {
			continue
		}
		// Advisory placement check: the offer's embedded node is the freshest
		// evidence; fall back to the catalog entry when the offer omits it.
		node := offer.offer.GetNode()
		if node == nil {
			node = allocator.Node
		}
		if !protocol.PlacementMatches(constraints, node) {
			continue
		}
		candidates = append(candidates, offer)
	}
	if len(candidates) == 0 {
		return nil, Allocator{}, false
	}
	sort.Slice(candidates, func(left, right int) bool {
		leftAllocator, _ := c.lookup(candidates[left].allocatorID)
		rightAllocator, _ := c.lookup(candidates[right].allocatorID)
		if leftAllocator.Hops != rightAllocator.Hops {
			return leftAllocator.Hops < rightAllocator.Hops
		}
		// Prefer an allocator that already caches the workload image: fewer
		// bytes over the network and a faster start. This is advisory only; a
		// node may cache an image while local policy still refuses it.
		leftCached := image != "" && nodeCaches(candidates[left].offer.GetNode(), image)
		rightCached := image != "" && nodeCaches(candidates[right].offer.GetNode(), image)
		if leftCached != rightCached {
			return leftCached
		}
		if compared := bytes.Compare(candidates[left].allocatorID, candidates[right].allocatorID); compared != 0 {
			return compared < 0
		}
		return candidates[left].offer.GetOfferId() < candidates[right].offer.GetOfferId()
	})
	selected := candidates[0]
	allocator, _ := c.lookup(selected.allocatorID)
	return selected, allocator, true
}

func nodeCaches(node *r1sv1.NodeCapabilities, image string) bool {
	if node == nil {
		return false
	}
	for _, cached := range node.GetCachedImages() {
		if cached == image {
			return true
		}
	}
	return false
}

func cloneAllocator(value Allocator) Allocator {
	result := Allocator{Identity: bytes.Clone(value.Identity), Destination: value.Destination, Hops: value.Hops}
	if value.Capacity != nil {
		result.Capacity = make(map[string]uint32, len(value.Capacity))
		for class, slots := range value.Capacity {
			result.Capacity[class] = slots
		}
	}
	if value.Node != nil {
		result.Node = proto.Clone(value.Node).(*r1sv1.NodeCapabilities)
	}
	result.TunnelHost = value.TunnelHost
	result.TunnelPort = value.TunnelPort
	result.TunnelDestination = value.TunnelDestination
	return result
}

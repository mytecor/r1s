package client

import (
	"bytes"
	"context"
	"encoding/hex"
	"sort"
	"strings"
	"time"
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
	o.allocators.put(candidate)
	if err := o.persistLocked(context.Background()); err != nil {
		if existed {
			o.allocators.put(previous)
		} else {
			o.allocators.remove(candidate.Identity)
		}
		return err
	}
	return nil
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
	candidates := make([]*offerRecord, 0, len(offers))
	for _, offer := range offers {
		allocator, ok := c.lookup(offer.allocatorID)
		if ok && strings.TrimSpace(allocator.Destination) != "" && offer.offer.GetExpiresAt().AsTime().After(now) {
			candidates = append(candidates, offer)
		}
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
		if compared := bytes.Compare(candidates[left].allocatorID, candidates[right].allocatorID); compared != 0 {
			return compared < 0
		}
		return candidates[left].offer.GetOfferId() < candidates[right].offer.GetOfferId()
	})
	selected := candidates[0]
	allocator, _ := c.lookup(selected.allocatorID)
	return selected, allocator, true
}

func cloneAllocator(value Allocator) Allocator {
	result := Allocator{Identity: bytes.Clone(value.Identity), Destination: value.Destination, Hops: value.Hops}
	if value.Capacity != nil {
		result.Capacity = make(map[string]uint32, len(value.Capacity))
		for class, slots := range value.Capacity {
			result.Capacity[class] = slots
		}
	}
	return result
}

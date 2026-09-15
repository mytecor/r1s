package allocator

import (
	"fmt"
	"maps"
	"strings"
)

// capacityLedger is the allocator's single authority for slot accounting.
// Callers must hold Allocator.mu while using it.
type capacityLedger struct {
	limits map[string]uint32
	used   map[string]uint32
}

func newCapacityLedger(configured map[string]uint32) (*capacityLedger, error) {
	if len(configured) == 0 {
		return nil, fmt.Errorf("%w: capacity is required", ErrInvalidConfig)
	}
	limits := make(map[string]uint32, len(configured))
	for class, slots := range configured {
		if strings.TrimSpace(class) == "" || slots == 0 {
			return nil, fmt.Errorf("%w: capacity classes and slots must be non-zero", ErrInvalidConfig)
		}
		limits[class] = slots
	}
	return &capacityLedger{limits: limits, used: make(map[string]uint32, len(limits))}, nil
}

func (l *capacityLedger) reserve(class string) error {
	limit, ok := l.limits[class]
	if !ok || l.used[class] >= limit {
		return fmt.Errorf("%w: resource class %q", ErrCapacityExhausted, class)
	}
	l.used[class]++
	return nil
}

func (l *capacityLedger) release(class string) {
	if l.used[class] > 0 {
		l.used[class]--
	}
}

func (l *capacityLedger) available(class string) uint32 {
	limit := l.limits[class]
	if l.used[class] >= limit {
		return 0
	}
	return limit - l.used[class]
}

func (l *capacityLedger) snapshot() map[string]uint32 {
	return maps.Clone(l.used)
}

func (l *capacityLedger) restore(used map[string]uint32) {
	l.used = used
}

package rns

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	meshbus "github.com/mytecor/meshbus/core"
)

const statusTTL = 15 * time.Second

// AllocatorStatus is advisory only. It never grants capacity, changes a lease,
// or replaces allocator-side admission and placement checks.
type AllocatorStatus struct {
	Revision   uint64            `json:"revision"`
	Capacity   map[string]uint32 `json:"capacity"`
	Ready      bool              `json:"ready"`
	Draining   bool              `json:"draining"`
	receivedAt time.Time
}

func (e *Endpoint) receiveStatus(_ context.Context, event meshbus.ReceivedEvent) error {
	if event.Topic != "r1s.allocator.capacity" && event.Topic != "r1s.allocator.ready" && event.Topic != "r1s.allocator.draining" {
		return nil
	}
	var status AllocatorStatus
	if len(event.Payload) > 8192 || json.Unmarshal(event.Payload, &status) != nil || status.Revision == 0 || len(status.Capacity) > 32 {
		return nil
	}
	status.receivedAt = time.Now()
	e.advisoryMu.Lock()
	for peer, previous := range e.advisory {
		if time.Since(previous.receivedAt) >= statusTTL {
			delete(e.advisory, peer)
		}
	}
	// The map is bounded by the Node's allocator directory: arbitrary realm
	// members cannot create unlimited advisory entries or advertise a new role.
	known := false
	for _, peer := range append(e.node.DiscoveredPeers(), e.node.Peers()...) {
		if peer.ID == event.Sender {
			_, err := parseDescriptorMetadata(peer.Metadata)
			known = err == nil
			break
		}
	}
	if known && (len(e.advisory) < 4096 || e.advisory[event.Sender].Revision != 0) && (status.Revision > e.advisory[event.Sender].Revision || time.Since(e.advisory[event.Sender].receivedAt) >= statusTTL) {
		e.advisory[event.Sender] = status
	}
	e.advisoryMu.Unlock()
	select {
	case e.discovered <- Service{}:
	default:
	}
	return nil
}

// PublishStatus sends a bounded best-effort update to authenticated subscribers.
// The caller supplies a monotonic process-local revision; ephemeral transport
// sessions and presence remain separate from durable execution authority.
func (e *Endpoint) PublishStatus(ctx context.Context, topic string, status AllocatorStatus) error {
	if topic != "r1s.allocator.capacity" && topic != "r1s.allocator.ready" && topic != "r1s.allocator.draining" {
		return fmt.Errorf("invalid allocator status topic %q", topic)
	}
	if status.Revision == 0 || len(status.Capacity) > 32 {
		return fmt.Errorf("allocator status revision is required")
	}
	data, err := json.Marshal(status)
	if err != nil {
		return err
	}

	if len(data) > 8192 {
		return fmt.Errorf("allocator status exceeds size limit")
	}
	_, err = e.node.Publish(ctx, topic, data, meshbus.PublishOptions{TTL: statusTTL, ContentType: "application/json", RemoteOnly: true})
	return err
}

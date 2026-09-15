package rns

import (
	"bytes"
	"context"
	"encoding/hex"
	"time"

	"quad4/reticulum-go/pkg/identity"
)

func (e *Endpoint) announceLoop(ctx context.Context) {
	ticker := time.NewTicker(e.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = e.destination.Announce(false, nil, nil)
		}
	}
}

type announceHandler struct {
	endpoint *Endpoint
	aspect   string
}

func (h *announceHandler) AspectFilter() []string     { return []string{h.aspect} }
func (h *announceHandler) ReceivePathResponses() bool { return true }
func (h *announceHandler) ReceivedAnnounce(destinationHash []byte, announced any, appData []byte, hops uint8) error {
	key := hex.EncodeToString(destinationHash)
	for _, waiter := range h.endpoint.connections.takeWaiters(key) {
		close(waiter)
	}
	descriptor, err := parseDescriptor(appData)
	if err != nil {
		return nil
	}
	announcedCluster, err := hex.DecodeString(descriptor.ClusterID)
	if err != nil || !bytes.Equal(announcedCluster, h.endpoint.clusterID) {
		return nil
	}
	announcedIdentity, ok := announced.(*identity.Identity)
	if !ok || announcedIdentity == nil {
		return nil
	}
	service := Service{
		Destination: key,
		Identity:    hex.EncodeToString(announcedIdentity.Hash()),
		Descriptor:  descriptor,
		Hops:        hops,
	}
	h.endpoint.connections.rememberDestination(announcedIdentity.Hash(), destinationHash)
	select {
	case h.endpoint.discovered <- service:
	default:
	}
	return nil
}

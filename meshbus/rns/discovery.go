package rns

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/Quad4-Software/Reticulum-Go/pkg/identity"
	"github.com/mytecor/r1s/meshbus"
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

// announceHandler registers on the local aspect and turns validated
// realm announces into meshbus presence records.
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
	// The codec decides whether this announce is usable and confirms the realm.
	// Forged or foreign descriptors never reach the peer directory.
	metadata, err := h.endpoint.codec.Parse(appData, h.endpoint.realmID)
	if err != nil {
		return nil
	}
	announcedIdentity, ok := announced.(*identity.Identity)
	if !ok || announcedIdentity == nil {
		return nil
	}
	peerID, err := meshbus.NewPeerID(announcedIdentity.Hash())
	if err != nil {
		return nil
	}
	seen := h.endpoint.directory.Remember(meshbus.Peer{
		ID:       peerID,
		Route:    key,
		Metadata: metadata,
		Hops:     hops,
	})
	if seen != nil {
		return nil
	}
	h.endpoint.connections.rememberDestination(announcedIdentity.Hash(), destinationHash)
	if h.endpoint.onDiscover != nil {
		h.endpoint.onDiscover(peerID, key, hops, metadata, appData)
	}
	return nil
}

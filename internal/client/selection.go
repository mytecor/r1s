package client

import (
	"bytes"
	"fmt"

	meshbus "github.com/mytecor/meshbus/core"
	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
)

// Select asks the route catalog to rank live offers, then atomically records
// both the chosen assignment and release intents for every losing offer.
func (o *Client) Select(requestID string) (meshbus.PeerID, *r1sv1.Envelope, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	record := o.requests[requestID]
	if record == nil {
		return meshbus.PeerID{}, nil, fmt.Errorf("%w: %q", ErrRequestNotFound, requestID)
	}
	if record.executionID != "" {
		execution := o.executions[record.executionID]
		return execution.peer, o.assignmentEnvelopeLocked(execution), nil
	}
	now := o.now().UTC()
	selected, allocator, ok := o.allocators.chooseWithPreferences(record.offers, now, record.request.GetConstraints(), record.request.GetWorkload().GetImage())
	if !ok {
		return meshbus.PeerID{}, nil, ErrNoOffer
	}
	executionID := o.uniqueExecutionIDLocked()
	messageID := o.newID()
	if executionID == "" || messageID == "" {
		return meshbus.PeerID{}, nil, fmt.Errorf("%w: ID generator returned an empty or duplicate ID", ErrInvalidConfig)
	}
	execution := &executionRecord{
		id: executionID, requestID: requestID, offerID: selected.offer.GetOfferId(), allocatorID: bytes.Clone(selected.allocatorID),
		peer: allocator.Peer, assignmentMessageID: messageID, assignmentSentAt: now,
	}
	prepared := make(map[*offerRecord]*releaseIntent)
	for _, offer := range record.offers {
		if offer == selected || offer.release != nil {
			continue
		}
		intent, err := o.newReleaseLocked(offer)
		if err != nil {
			return meshbus.PeerID{}, nil, err
		}
		prepared[offer] = intent
	}
	for offer, intent := range prepared {
		offer.release = intent
	}
	record.executionID = executionID
	o.executions[executionID] = execution
	return execution.peer, o.assignmentEnvelopeLocked(execution), nil
}

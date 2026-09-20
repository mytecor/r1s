package client

import (
	"bytes"
	"context"
	"fmt"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
)

// Select asks the route catalog to rank live offers, then atomically records
// both the chosen assignment and release intents for every losing offer.
func (o *Client) Select(requestID string) (string, *r1sv1.Envelope, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	record := o.requests[requestID]
	if record == nil {
		return "", nil, fmt.Errorf("%w: %q", ErrRequestNotFound, requestID)
	}
	if record.executionID != "" {
		execution := o.executions[record.executionID]
		return execution.destination, o.assignmentEnvelopeLocked(execution), nil
	}
	now := o.now().UTC()
	selected, allocator, ok := o.allocators.chooseWithPreferences(record.offers, now, record.request.GetConstraints(), record.request.GetWorkload().GetImage())
	if !ok {
		return "", nil, ErrNoOffer
	}
	executionID := o.uniqueExecutionIDLocked()
	messageID := o.newID()
	if executionID == "" || messageID == "" {
		return "", nil, fmt.Errorf("%w: ID generator returned an empty or duplicate ID", ErrInvalidConfig)
	}
	execution := &executionRecord{
		id: executionID, requestID: requestID, offerID: selected.offer.GetOfferId(), allocatorID: bytes.Clone(selected.allocatorID),
		destination: allocator.Destination, assignmentMessageID: messageID, assignmentSentAt: now,
	}
	prepared := make(map[*offerRecord]*releaseIntent)
	for _, offer := range record.offers {
		if offer == selected || offer.release != nil {
			continue
		}
		intent, err := o.newReleaseLocked(offer)
		if err != nil {
			return "", nil, err
		}
		prepared[offer] = intent
	}
	for offer, intent := range prepared {
		offer.release = intent
	}
	record.executionID = executionID
	o.executions[executionID] = execution
	if err := o.persistLocked(context.Background()); err != nil {
		for offer := range prepared {
			offer.release = nil
		}
		record.executionID = ""
		delete(o.executions, executionID)
		return "", nil, err
	}
	return execution.destination, o.assignmentEnvelopeLocked(execution), nil
}

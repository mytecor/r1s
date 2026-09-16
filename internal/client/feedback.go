package client

import (
	"bytes"
	"fmt"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
)

type RemoteError struct {
	Code, Detail string
	Retryable    bool
}

func (e *RemoteError) Error() string { return fmt.Sprintf("allocator %s: %s", e.Code, e.Detail) }

func (o *Client) handleErrorLocked(e *r1sv1.Envelope) error {
	authorized := o.logAuthorityLocked(e)
	id := e.GetCorrelationId()
	for _, request := range o.requests {
		if request.messageID == id {
			_, authorized = o.allocators.lookup(e.GetSender())
		}
		for _, offer := range request.offers {
			if offer.release != nil && offer.release.MessageID == id && bytes.Equal(offer.allocatorID, e.GetSender()) {
				authorized = true
			}
		}
	}
	for _, execution := range o.executions {
		if bytes.Equal(execution.allocatorID, e.GetSender()) && (id == execution.assignmentMessageID || id == execution.inspectMessageID || id == execution.cancelMessageID || id == execution.leaseRenewMessageID) {
			authorized = true
			if id == execution.leaseRenewMessageID {
				o.markRenewalFailureLocked(execution.id, e.GetCommandError().GetCode())
			}
		}
	}
	if !authorized {
		return ErrUnauthorized
	}
	return nil
}

func RemoteFailure(e *r1sv1.Envelope) error {
	if failure := e.GetCommandError(); failure != nil {
		return &RemoteError{Code: failure.GetCode(), Detail: failure.GetDetail(), Retryable: failure.GetRetryable()}
	}
	return nil
}

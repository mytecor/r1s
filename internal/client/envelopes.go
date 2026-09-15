package client

import (
	"bytes"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (o *Client) requestEnvelopeLocked(request *r1sv1.ExecutionRequest, messageID string, sentAt time.Time) *r1sv1.Envelope {
	return &r1sv1.Envelope{
		MessageId: messageID, Sender: bytes.Clone(o.identity), SentAt: timestamppb.New(sentAt),
		Payload: &r1sv1.Envelope_ExecutionRequest{ExecutionRequest: proto.Clone(request).(*r1sv1.ExecutionRequest)},
	}
}

func (o *Client) assignmentEnvelopeLocked(record *executionRecord) *r1sv1.Envelope {
	return &r1sv1.Envelope{
		MessageId: record.assignmentMessageID, Sender: bytes.Clone(o.identity), SentAt: timestamppb.New(record.assignmentSentAt),
		Payload: &r1sv1.Envelope_ExecutionAssign{ExecutionAssign: &r1sv1.ExecutionAssign{
			RequestId: record.requestID, OfferId: record.offerID, ExecutionId: record.id,
		}},
	}
}

func (o *Client) uniqueIDLocked(existing map[string]*requestRecord) string {
	value := o.newID()
	if value == "" || existing[value] != nil {
		return ""
	}
	return value
}

func (o *Client) uniqueExecutionIDLocked() string {
	value := o.newID()
	if value == "" || o.executions[value] != nil {
		return ""
	}
	return value
}

func cloneWorkload(value *r1sv1.Workload) *r1sv1.Workload {
	if value == nil {
		return nil
	}
	return proto.Clone(value).(*r1sv1.Workload)
}

func clonePolicy(value *r1sv1.ExecutionPolicy) *r1sv1.ExecutionPolicy {
	if value == nil {
		return nil
	}
	return proto.Clone(value).(*r1sv1.ExecutionPolicy)
}

func terminal(phase r1sv1.ExecutionPhase) bool {
	return phase == r1sv1.ExecutionPhase_EXECUTION_PHASE_CANCELLED || phase == r1sv1.ExecutionPhase_EXECUTION_PHASE_COMPLETED || phase == r1sv1.ExecutionPhase_EXECUTION_PHASE_FAILED
}

package client

import (
	"bytes"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Logs creates one explicit request. Intent is transient; restart never resumes it.
func (o *Client) Logs(id, stream string, offset uint64, limit uint32) (string, *r1sv1.Envelope, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	record := o.executions[id]
	if record == nil {
		return "", nil, ErrExecutionNotFound
	}
	request := &r1sv1.ExecutionLogsRequest{ExecutionId: id, Stream: stream, Offset: offset, MaxBytes: limit}
	e := &r1sv1.Envelope{MessageId: o.newID(), Sender: bytes.Clone(o.identity), SentAt: timestamppb.New(o.now().UTC()), Payload: &r1sv1.Envelope_ExecutionLogsRequest{ExecutionLogsRequest: request}}
	if err := protocol.ValidateEnvelope(e); err != nil {
		return "", nil, err
	}
	// A client has at most one outstanding explicit log read. No background log queue.
	o.logMessageID = e.GetMessageId()
	o.logRequest = proto.Clone(request).(*r1sv1.ExecutionLogsRequest)
	return record.destination, e, nil
}

func (o *Client) logAuthorityLocked(e *r1sv1.Envelope) bool {
	if o.logRequest == nil || e.GetCorrelationId() != o.logMessageID {
		return false
	}
	record := o.executions[o.logRequest.GetExecutionId()]
	return record != nil && bytes.Equal(record.allocatorID, e.GetSender())
}

func (o *Client) handleLogsLocked(e *r1sv1.Envelope) error {
	if !o.logAuthorityLocked(e) {
		return ErrUnauthorized
	}
	q, r := o.logRequest, e.GetExecutionLogsResponse()
	if r.GetExecutionId() != q.GetExecutionId() || r.GetStream() != q.GetStream() || r.GetOffset() != q.GetOffset() || len(r.GetData()) > int(q.GetMaxBytes()) {
		return ErrConflict
	}
	return nil
}

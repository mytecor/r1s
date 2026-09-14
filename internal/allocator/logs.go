package allocator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	r1sruntime "github.com/mytecor/r1s/internal/runtime"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Log data is never stored in the replay cache or attached to lifecycle responses.
func (a *Allocator) handleLogs(ctx context.Context, e *r1sv1.Envelope) ([]*r1sv1.Envelope, error) {
	q := e.GetExecutionLogsRequest()
	a.mu.Lock()
	record := a.executions[q.GetExecutionId()]
	authorized := record != nil && bytes.Equal(record.client, e.GetSender())
	a.mu.Unlock()
	if !authorized {
		return a.errorResponse(e, ErrExecutionNotFound), ErrExecutionNotFound
	}
	if a.logs == nil {
		return a.errorResponse(e, ErrExecutionNotFound), ErrExecutionNotFound
	}
	chunk, err := a.logs.Read(ctx, q.GetExecutionId(), q.GetStream(), q.GetOffset(), q.GetMaxBytes())
	if err != nil {
		if errors.Is(err, r1sruntime.ErrLogsMissing) {
			err = ErrExecutionNotFound
		}
		if errors.Is(err, r1sruntime.ErrLogOffset) {
			err = ErrExecutionConflict
		}
		return a.errorResponse(e, err), err
	}
	hash := sha256.Sum256(chunk.Data)
	return []*r1sv1.Envelope{{MessageId: randomID(), Sender: bytes.Clone(a.identity), SentAt: timestamppb.New(a.now().UTC()), CorrelationId: e.GetMessageId(), Payload: &r1sv1.Envelope_ExecutionLogsResponse{ExecutionLogsResponse: &r1sv1.ExecutionLogsResponse{
		ExecutionId: q.GetExecutionId(), Stream: q.GetStream(), Offset: q.GetOffset(), Data: chunk.Data, NextOffset: chunk.NextOffset, Eof: chunk.EOF, Truncated: chunk.Truncated, Sha256: hash[:],
	}}}}, nil
}

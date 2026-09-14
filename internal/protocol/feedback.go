package protocol

import (
	"bytes"
	"crypto/sha256"
	"strings"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
)

const MaxLogBytes = 128

var ErrorCodes = map[string]bool{
	"INVALID_REQUEST": true, "CAPACITY": true, "NOT_FOUND": true,
	"CONFLICT": true, "EXPIRED": true, "UNAVAILABLE": true, "DENIED": true,
	"INTERNAL": true, "OUTCOME_UNKNOWN": true,
}

func validateError(e *r1sv1.Envelope, failure *r1sv1.CommandError) error {
	if failure == nil || !ErrorCodes[failure.GetCode()] || strings.TrimSpace(e.GetCorrelationId()) == "" || len(failure.GetDetail()) > 256 {
		return invalid("command_error", "requires a known code, correlation, and bounded detail")
	}
	return nil
}

func validateLogsRequest(q *r1sv1.ExecutionLogsRequest) error {
	if q == nil || strings.TrimSpace(q.GetExecutionId()) == "" || (q.GetStream() != "stdout" && q.GetStream() != "stderr") || q.GetMaxBytes() == 0 || q.GetMaxBytes() > MaxLogBytes {
		return invalid("execution_logs_request", "requires execution, stdout/stderr, and 1..128 bytes")
	}
	return nil
}

func validateLogsResponse(e *r1sv1.Envelope, r *r1sv1.ExecutionLogsResponse) error {
	if r == nil || e.GetCorrelationId() == "" || r.GetExecutionId() == "" || (r.GetStream() != "stdout" && r.GetStream() != "stderr") || len(r.GetData()) > MaxLogBytes || r.GetNextOffset() < r.GetOffset() || r.GetNextOffset()-r.GetOffset() != uint64(len(r.GetData())) {
		return invalid("execution_logs_response", "invalid range or correlation")
	}
	hash := sha256.Sum256(r.GetData())
	if !bytes.Equal(hash[:], r.GetSha256()) {
		return invalid("execution_logs_response.sha256", "checksum mismatch")
	}
	return nil
}

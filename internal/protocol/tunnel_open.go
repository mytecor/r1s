package protocol

import (
	"fmt"
	"strings"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
)

// Package-level classification helpers for the F21-02 tunnel Open flow. The
// Open/result messages are the first application bytes on an identified tunnel
// Link (a private RNS data plane), so they are never validated as Envelope
// payloads: they are validated here before the allocator edge authorizes them.
// The result error values mirror LocalTunnelClose.Reason and the tunnel
// package's classified teardown reasons so one diagnostic surfaces everywhere.

// ValidateTunnelOpen validates a client's Open message (execution ID and
// container-side destination port). The sender identity is never validated
// here: the allocator edge authorizes it from Link.Identify() and the
// execution's owner, not from any payload field.
func ValidateTunnelOpen(open *r1sv1.TunnelOpen) error {
	if open == nil {
		return invalid("tunnel_open", "is required")
	}
	if strings.TrimSpace(open.GetExecutionId()) == "" {
		return invalid("tunnel_open.execution_id", "is required")
	}
	port := open.GetTargetPort()
	if port == 0 {
		return invalid("tunnel_open.target_port", "is required and must be positive")
	}
	if port > 0xFFFF {
		return invalid("tunnel_open.target_port", "is too large")
	}
	return nil
}

// ValidateTunnelOpenResult validates an allocator's Open reply. A successful
// open (ok) must carry no error classification; a failed one must carry a
// known error value. detail is optional diagnostic text on either path.
func ValidateTunnelOpenResult(result *r1sv1.TunnelOpenResult) error {
	if result == nil {
		return invalid("tunnel_open_result", "is required")
	}
	if result.GetOk() {
		if result.GetError() != r1sv1.TunnelOpenError_TUNNEL_OPEN_ERROR_UNSPECIFIED {
			return invalid("tunnel_open_result.error", "must be unspecified on an ok open")
		}
		return nil
	}
	switch result.GetError() {
	case r1sv1.TunnelOpenError_TUNNEL_OPEN_ERROR_UNAUTHORIZED,
		r1sv1.TunnelOpenError_TUNNEL_OPEN_ERROR_NOT_RUNNING,
		r1sv1.TunnelOpenError_TUNNEL_OPEN_ERROR_INVALID_PORT,
		r1sv1.TunnelOpenError_TUNNEL_OPEN_ERROR_NO_ENDPOINT,
		r1sv1.TunnelOpenError_TUNNEL_OPEN_ERROR_MESH_UNREACHABLE:
		return nil
	default:
		return invalid("tunnel_open_result.error", "must be a known error on a failed open")
	}
}

// ClassifyTunnelOpenResult validates an Open reply and returns its normalized
// outcome.
func ClassifyTunnelOpenResult(result *r1sv1.TunnelOpenResult) (classification TunnelOpenErrorStatus, err error) {
	if err := ValidateTunnelOpenResult(result); err != nil {
		return TunnelOpenErrorStatus{}, err
	}
	if result.GetOk() {
		return TunnelOpenErrorStatus{OK: true}, nil
	}
	return TunnelOpenErrorStatus{Error: result.GetError(), ErrorDetail: result.GetDetail()}, nil
}

// TunnelOpenErrorStatus is the normalized outcome of an Open, exported so the
// allocator edge and client edge can classify without importing the tunnel
// package's teardown vocabulary into the wire-validating layer.
type TunnelOpenErrorStatus struct {
	OK          bool
	Error       r1sv1.TunnelOpenError
	ErrorDetail string
}

// Error() is unavailable because the proto's error field owns the name; use
// Message for a classified diagnostic string.
func (s TunnelOpenErrorStatus) Message() string {
	if s.OK {
		return ""
	}
	message := fmt.Sprintf("tunnel open rejected: %s", s.Error)
	if s.ErrorDetail != "" {
		message += ": " + s.ErrorDetail
	}
	return message
}

// Is reports whether err represents this rejected Open (a non-OK outcome).
func (s TunnelOpenErrorStatus) Is(err error) bool {
	return err != nil && !s.OK
}

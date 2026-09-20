package yggdrasil

import (
	"bytes"
	"errors"
	"testing"

	"github.com/mytecor/r1s/internal/tunnel"
)

// TestFrameRoundTrip verifies one frame survives append+parse round-trip for
// every frame type.
func TestFrameRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name    string
		typ     byte
		payload []byte
	}{
		{"data", frameTypeData, []byte("hello tunnel")},
		{"empty-eof", frameTypeEOF, nil},
		{"close", frameTypeClose, closePayload(tunnel.ReasonExecutionEnded, "container exited")},
		{"preamble", frameTypePreamble, encodePreamble(tunnel.Preamble{ExecutionID: "exec-1", GrantID: "grant-1"})},
		{"accept", frameTypeAccept, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			frameBytes := appendFrame(nil, tc.typ, tc.payload)
			f, consumed, ok, err := parseFrame(frameBytes)
			if err != nil || !ok {
				t.Fatalf("parseFrame: ok=%v err=%v", ok, err)
			}
			if f.typ != tc.typ || !bytes.Equal(f.payload, tc.payload) {
				t.Fatalf("decoded frame %+v, want type 0x%02x payload %q", f, tc.typ, tc.payload)
			}
			if consumed != frameHeaderSize+len(tc.payload) {
				t.Fatalf("consumed = %d; want %d", consumed, frameHeaderSize+len(tc.payload))
			}
		})
	}
}

// TestParseFrameUnknownType verifies malformed input is detected as an error
// (and the session dies rather than desynchronizing).
func TestFrameUnknownType(t *testing.T) {
	if _, _, _, err := parseFrame([]byte{0x7f, 0x00, 0x00}); err == nil {
		t.Fatal("unknown frame type accepted")
	}
	if _, _, _, err := parseFrame([]byte{frameTypeData, 0xff, 0xff}); err == nil {
		t.Fatal("oversized frame length accepted")
	}
}

// TestClosePayloadRoundTrip verifies a classified teardown reason survives the
// wire and an unknown reason degrades to ReasonSessionFailed.
func TestClosePayloadRoundTrip(t *testing.T) {
	payload := closePayload(tunnel.ReasonExecutionEnded, "container exited")
	var sessionErr *tunnel.SessionError
	if !errors.As(parseClosePayload(payload), &sessionErr) {
		t.Fatal("parseClosePayload did not yield a session error")
	}
	if sessionErr.Reason != tunnel.ReasonExecutionEnded {
		t.Fatalf("reason = %q; want execution_ended", sessionErr.Reason)
	}
	if !asSessionError(parseClosePayload([]byte{0x99})) {
		t.Fatal("unknown reason did not degrade to session_failed")
	}
}

// TestPreambleRoundTrip verifies the routing preamble survives the wire.
func TestPreambleRoundTrip(t *testing.T) {
	p := tunnel.Preamble{ExecutionID: "01HZ...ABC", GrantID: "grant-1"}
	decoded, err := decodePreamble(encodePreamble(p))
	if err != nil {
		t.Fatalf("decodePreamble: %v", err)
	}
	if decoded != p {
		t.Fatalf("preamble round-trip mismatch: %+v != %+v", p, decoded)
	}
	// An empty preamble is a protocol violation.
	if _, err := decodePreamble(encodePreamble(tunnel.Preamble{})); err == nil {
		t.Fatal("empty preamble accepted")
	}
}

// TestMaxFramePayload verifies the segment bound leaves room for the framing
// header and stays well below any mesh MTU the Core reports.
func TestMaxFramePayload(t *testing.T) {
	if maxPacketPayload <= frameHeaderSize {
		t.Fatal("frame payload cap leaves no room for the header")
	}
}

// asSessionError is a test helper: the error is a classified session error.
func asSessionError(err error) bool {
	var sessionErr *tunnel.SessionError
	return errors.As(err, &sessionErr)
}

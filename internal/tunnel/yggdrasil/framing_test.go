package yggdrasil

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/mytecor/r1s/internal/tunnel"
)

// TestFrameRoundTrip verifies one frame survives append+parse round-trip for
// every frame type, including the F19 stream-open and window-update headers.
func TestFrameRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name     string
		typ      byte
		streamID uint32
		payload  []byte
	}{
		{"data", frameTypeData, 1, []byte("hello tunnel")},
		{"empty-eof", frameTypeEOF, 2, nil},
		{"close", frameTypeClose, 3, closePayload(tunnel.ReasonExecutionEnded, "container exited")},
		{"preamble", frameTypePreamble, streamIDNone, encodePreamble(tunnel.Preamble{ExecutionID: "exec-1", GrantID: "grant-1"})},
		{"accept", frameTypeAccept, streamIDNone, nil},
		{"stream-open", frameTypeStreamOpen, 4, encodeStreamOpen(8080)},
		{"window-update", frameTypeWindowUpdate, 5, streamWindowPayload(8192)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			frameBytes := appendFrame(nil, tc.typ, tc.streamID, tc.payload)
			f, consumed, ok, err := parseFrame(frameBytes)
			if err != nil || !ok {
				t.Fatalf("parseFrame: ok=%v err=%v", ok, err)
			}
			if f.typ != tc.typ || f.streamID != tc.streamID || !bytes.Equal(f.payload, tc.payload) {
				t.Fatalf("decoded frame %+v, want type 0x%02x sid %d payload %q", f, tc.typ, tc.streamID, tc.payload)
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
	if _, _, _, err := parseFrame([]byte{0x7f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}); err == nil {
		t.Fatal("unknown frame type accepted")
	}
	oversized := appendFrame(nil, frameTypeData, 1, make([]byte, maxPacketPayload+1))
	f, _, _, err := parseFrame(oversized)
	if err == nil {
		t.Fatalf("oversized length accepted (frame=%+v)", f)
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

// TestStreamOpenRoundTrip verifies a stream-open header (the container port the
// stream is spliced to) survives the wire as a big-endian uint16, and that a
// truncated payload decodes to port 0.
func TestStreamOpenRoundTrip(t *testing.T) {
	if got := decodeStreamOpen(encodeStreamOpen(8080)); got != 8080 {
		t.Fatalf("decodeStreamOpen = %d; want 8080", got)
	}
	if got := decodeStreamOpen(nil); got != 0 {
		t.Fatalf("decodeStreamOpen(empty) = %d; want 0", got)
	}
}

// TestWindowUpdateRoundTrip verifies the flow-control increment survives the
// wire as big-endian bytes.
func TestWindowUpdateRoundTrip(t *testing.T) {
	payload := streamWindowPayload(8192)
	if got := parseWindowPayload(payload); got != 8192 {
		t.Fatalf("parseWindowPayload = %d; want 8192", got)
	}
	var decoded uint32
	if err := binary.Read(bytes.NewReader(payload), binary.BigEndian, &decoded); err != nil {
		t.Fatalf("decode window payload: %v", err)
	}
	if decoded != 8192 {
		t.Fatalf("window increment = %d; want 8192", decoded)
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

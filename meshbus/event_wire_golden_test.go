package meshbus

import (
	"bytes"
	"encoding/hex"
	"testing"
	"time"
)

func TestEventV1GoldenVector(t *testing.T) {
	event := Event{
		ID:    EventID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
		Topic: "a.b", PublishedAt: time.UnixMilli(1_700_000_000_123).UTC(), TTL: time.Minute,
		ContentType: "text/plain", Payload: []byte("ok"),
	}
	wire, err := encodeEvent(event, defaultMaxEventPayload, defaultMaxEventTTL)
	if err != nil {
		t.Fatal(err)
	}
	want, _ := hex.DecodeString("4d424501000102030405060708090a0b0c0d0e0f0000018bcfe5687b0000ea600003000a00000002612e62746578742f706c61696e6f6b")
	if !bytes.Equal(wire, want) {
		t.Fatalf("event wire = %x, want %x", wire, want)
	}
	decoded, err := decodeEvent(want, defaultMaxEventPayload, defaultMaxEventTTL)
	if err != nil || decoded.ID != event.ID || decoded.Topic != event.Topic || !bytes.Equal(decoded.Payload, event.Payload) {
		t.Fatalf("decoded = %+v, error = %v", decoded, err)
	}
}

func FuzzDecodeEvent(f *testing.F) {
	seed, _ := hex.DecodeString("4d424501000102030405060708090a0b0c0d0e0f0000018bcfe5687b0000ea600003000a00000002612e62746578742f706c61696e6f6b")
	f.Add(seed)
	f.Add([]byte("MBE\x01"))
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = decodeEvent(data, defaultMaxEventPayload, defaultMaxEventTTL)
	})
}

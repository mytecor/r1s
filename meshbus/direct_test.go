package meshbus

import (
	"bytes"
	"errors"
	"testing"
)

func TestReceivedMessageIsolatesAuthenticatedSenderAndPayload(t *testing.T) {
	sender := []byte{0x01, 0x02}
	payload := []byte("payload")
	message, err := NewReceivedMessage(sender, payload)
	if err != nil {
		t.Fatal(err)
	}
	sender[0] = 0xff
	payload[0] = 'X'

	if got := message.Sender().Bytes(); !bytes.Equal(got, []byte{0x01, 0x02}) {
		t.Fatalf("sender = %x", got)
	}
	if message.Sender().String() != "0102" {
		t.Fatalf("sender text = %q", message.Sender().String())
	}
	gotPayload := message.Payload()
	if string(gotPayload) != "payload" {
		t.Fatalf("payload = %q", gotPayload)
	}
	gotPayload[0] = 'Y'
	if string(message.Payload()) != "payload" {
		t.Fatal("returned payload aliases message storage")
	}
}

func TestReceivedMessageRejectsMissingAuthorityOrPayload(t *testing.T) {
	if _, err := NewReceivedMessage(nil, []byte("payload")); !errors.Is(err, ErrInvalidPeerID) {
		t.Fatalf("missing sender error = %v", err)
	}
	if _, err := NewReceivedMessage([]byte{1}, nil); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("missing payload error = %v", err)
	}
}

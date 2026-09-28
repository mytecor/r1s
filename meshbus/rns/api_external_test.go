package rns_test

import (
	"bytes"
	"strings"
	"testing"

	meshrns "github.com/mytecor/meshbus/rns"
)

func TestPublicPresenceCodec(t *testing.T) {
	presence := meshrns.Presence{
		Protocol: "meshbus.v1",
		Realm:    strings.Repeat("ab", 32),
		Metadata: map[string]string{"service": "example"},
	}
	wire, err := presence.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := meshrns.ParsePresence(wire)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Realm != presence.Realm || decoded.Metadata["service"] != "example" {
		t.Fatalf("decoded presence = %+v", decoded)
	}
	if _, err := meshrns.ParsePresence(bytes.Repeat([]byte{'x'}, 257)); err == nil {
		t.Fatal("oversized presence was accepted")
	}
}

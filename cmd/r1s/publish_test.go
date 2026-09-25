package main

import (
	"errors"
	"net"
	"testing"
)

func TestParsePort(t *testing.T) {
	cases := []struct {
		raw     string
		want    uint16
		wantErr bool
	}{
		{"80", 80, false},
		{"8080", 8080, false},
		{"65535", 65535, false},
		{"0", 0, true},
		{"-1", 0, true},
		{"70000", 0, true},
		{"", 0, true},
		{"abc", 0, true},
	}
	for _, tc := range cases {
		got, err := parsePort(tc.raw)
		if (err != nil) != tc.wantErr {
			t.Fatalf("parsePort(%q) error = %v; wantErr=%v", tc.raw, err, tc.wantErr)
		}
		if !tc.wantErr && got != tc.want {
			t.Fatalf("parsePort(%q) = %d; want %d", tc.raw, got, tc.want)
		}
	}
}

func TestParsePortMapping(t *testing.T) {
	m, err := parsePortMapping(" 8080:80 ")
	if err != nil {
		t.Fatal(err)
	}
	if m.host != 8080 || m.container != 80 {
		t.Fatalf("mapping = %+v; want host 8080 container 80", m)
	}
	for _, bad := range []string{"", "8080", ":", "8080:0", "0:80", "8080:abc", "x:y", "8080:65536"} {
		if _, err := parsePortMapping(bad); err == nil {
			t.Fatalf("parsePortMapping(%q) succeeded; want error", bad)
		}
	}
}

func TestPortListValueAccumulates(t *testing.T) {
	var v portListValue
	if err := v.Set("8080:80"); err != nil {
		t.Fatal(err)
	}
	if err := v.Set("2222:22"); err != nil {
		t.Fatal(err)
	}
	if len(v.mappings) != 2 {
		t.Fatalf("accumulated %d mappings; want 2", len(v.mappings))
	}
	if v.String() != "8080:80,2222:22" {
		t.Fatalf("String() = %q; want 8080:80,2222:22", v.String())
	}
	if err := v.Set("bad"); err == nil {
		t.Fatal("Set accepted an invalid mapping")
	}
}

func TestParsePublishFlag(t *testing.T) {
	mappings, err := parsePublishFlag("8080:80,2222:22")
	if err != nil {
		t.Fatal(err)
	}
	if len(mappings) != 2 || mappings[0].container != 80 || mappings[1].container != 22 {
		t.Fatalf("mappings = %+v", mappings)
	}
	// Empty and whitespace-only flags yield no mappings (a run without ports).
	if got, err := parsePublishFlag(""); err != nil || len(got) != 0 {
		t.Fatalf("empty flag = %+v, %v; want empty", got, err)
	}
	if got, err := parsePublishFlag("  , "); err != nil || len(got) != 0 {
		t.Fatalf("blank flag = %+v, %v; want empty", got, err)
	}
	if _, err := parsePublishFlag("8080:80,bad"); err == nil {
		t.Fatal("parsePublishFlag accepted a malformed item")
	}
}

// TestPublisherTargetDedup verifies that multiple -p mappings sharing a
// container port dedupe the authorized target list but keep every host->container
// route, so the allocator authorizes each container port exactly once.
func TestPublisherTargetDedup(t *testing.T) {
	mappings := []portMapping{{host: 18000, container: 80}, {host: 18080, container: 8080}, {host: 18001, container: 80}}
	p, err := newRunPublisher(t.Context(), &application{}, mappings)
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	if len(p.targets) != 2 {
		t.Fatalf("deduped targets = %d; want 2 (container ports 80 and 8080)", len(p.targets))
	}
	// Listeners were bound one per mapping: three distinct host listeners even
	// though container port 80 is shared.
	if len(p.listeners) != 3 {
		t.Fatalf("bound %d listeners; want 3", len(p.listeners))
	}
	// containerFor resolves the container port for each listener by host port.
	var hostToContainer = map[uint16]uint16{}
	for _, m := range mappings {
		hostToContainer[m.host] = m.container
	}
	for host, container := range hostToContainer {
		local := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: int(host)}
		got, ok := p.containerFor(local)
		if !ok || got != container {
			t.Fatalf("containerFor(host %d) = %d, %v; want %d", host, got, ok, container)
		}
	}
	if _, ok := p.containerFor(&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9999}); ok {
		t.Fatal("containerFor resolved an unbound host port")
	}
}

// TestPublisherDupBindFails verifies a repeated host port fails fast before any
// forwarding starts.
func TestPublisherDupBindFails(t *testing.T) {
	mappings := []portMapping{{host: 18010, container: 80}, {host: 18010, container: 8080}}
	if _, err := newRunPublisher(t.Context(), &application{}, mappings); err == nil {
		t.Fatal("duplicate host port bind succeeded")
	}
}

// TestPublisherSetActiveClosesStalePair is the F22-06 rebinding guard: pointing
// the publisher at a new execution closes the old execution's pair so its
// streams end, without touching the listeners, and a later connection
// re-establishes against the replacement.
func TestPublisherSetActiveClosesStalePair(t *testing.T) {
	p, err := newRunPublisher(t.Context(), &application{}, []portMapping{{host: 18020, container: 80}})
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()

	// A stub pair conn records Close; the publisher only inspects executionID
	// and calls Close on the stale pair during rebinding.
	stale := &fakePairConn{}
	p.pair = &tunnelPair{executionID: "execution-attempt-1", ports: map[uint16]bool{80: true}, conn: stale}

	p.SetActive("execution-attempt-2")
	if !stale.closed {
		t.Fatal("SetActive did not close the stale pair conn")
	}
	if p.pair != nil {
		t.Fatal("SetActive did not drop the stale pair cache")
	}
	// The listener is still bound: a fresh connection can be accepted.
	conn, err := net.Dial("tcp", p.listeners[0].Addr().String())
	if err != nil {
		t.Fatalf("listener not bound after rebinding: %v", err)
	}
	conn.Close()
	// Same-execution SetActive must not disturb an established pair.
	p.pair = &tunnelPair{executionID: "execution-attempt-2", ports: map[uint16]bool{80: true}, conn: stale}
	stale.closed = false
	p.SetActive("execution-attempt-2")
	if stale.closed || p.pair == nil {
		t.Fatal("SetActive for the same execution disturbed the pair")
	}
}

// fakePairConn is a minimal tunnel.Conn that records whether it was closed.
type fakePairConn struct{ closed bool }

func (f *fakePairConn) Read(p []byte) (int, error)  { return 0, errors.New("closed") }
func (f *fakePairConn) Write(p []byte) (int, error) { return 0, errors.New("closed") }
func (f *fakePairConn) Close() error                { f.closed = true; return nil }
func (f *fakePairConn) PeerKey() []byte             { return nil }
func (f *fakePairConn) CloseRead() error            { return nil }
func (f *fakePairConn) CloseWrite() error           { return nil }

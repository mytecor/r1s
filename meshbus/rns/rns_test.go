package rns

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/Quad4-Software/Reticulum-Go/pkg/common"
	"github.com/Quad4-Software/Reticulum-Go/pkg/interfaces"
	"github.com/Quad4-Software/Reticulum-Go/pkg/sharedinstance"
	rnstransport "github.com/Quad4-Software/Reticulum-Go/pkg/transport"
	"github.com/mytecor/r1s/meshbus"
)

// testRealmKey is a fixed realm key shared by same-realm test endpoints.
func testRealmKey() []byte { return bytes.Repeat([]byte{0x51}, 32) }

// foreignRealmKey is a different realm key for cross-realm rejection tests.
func foreignRealmKey() []byte { return bytes.Repeat([]byte{0x77}, 32) }

func freeUDPPort(t *testing.T) int {
	t.Helper()
	listener, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.LocalAddr().(*net.UDPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}

func freeTCPPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}

// standaloneConfig returns an isolated UDP-loopback Reticulum config on the
// given listener/target ports, used to keep tests off the shared instance.
func standaloneConfig(storage string, listenPort, targetPort int) *common.ReticulumConfig {
	config := common.DefaultConfig()
	config.EnableTransport = false
	config.ConfigPath = storage
	config.Interfaces = map[string]*common.InterfaceConfig{
		"test": {
			Type:       "UDPInterface",
			Enabled:    true,
			Address:    fmt.Sprintf("127.0.0.1:%d", listenPort),
			TargetHost: fmt.Sprintf("127.0.0.1:%d", targetPort),
		},
	}
	return config
}

// newTestEndpoint builds one generic endpoint on its own UDP loopback pair.
// advertise=false makes the endpoint passive (it never announces itself).
func newTestEndpoint(t *testing.T, storage string, listenPort, targetPort int, key []byte, advertise bool, handler meshbus.Handler) *Endpoint {
	t.Helper()
	if handler == nil {
		handler = func(context.Context, meshbus.ReceivedMessage) error { return nil }
	}
	var codec PresenceCodec = GenericPresenceCodec{}
	if !advertise {
		codec = passiveCodec{}
	}
	endpoint, err := New(Config{
		Reticulum:      standaloneConfig(storage, listenPort, targetPort),
		IdentitySource: filepath.Join(storage, "identity"),
		RealmKey:       key,
		Codec:          codec,
		NetworkWait:    8 * time.Second,
		Handler:        handler,
	})
	if err != nil {
		t.Fatal(err)
	}
	return endpoint
}

// newPair builds two generic endpoints on one UDP loopback pair in the same
// realm. The first endpoint advertises; the second is passive. It returns the
// endpoints plus a channel receiving every authenticated message the passive
// side sees.
func newPair(t *testing.T, key []byte) (*Endpoint, *Endpoint, chan meshbus.ReceivedMessage) {
	t.Helper()
	portA := freeUDPPort(t)
	portB := freeUDPPort(t)
	for portB == portA {
		portB = freeUDPPort(t)
	}
	root := t.TempDir()
	received := make(chan meshbus.ReceivedMessage, 8)
	advertiser := newTestEndpoint(t, filepath.Join(root, "advertiser"), portA, portB, key, true, func(_ context.Context, message meshbus.ReceivedMessage) error {
		received <- message
		return nil
	})
	passive := newTestEndpoint(t, filepath.Join(root, "passive"), portB, portA, key, false, nil)
	return advertiser, passive, received
}

// passiveCodec carries no app data, keeping an endpoint passive: it presents
// as a discoverable realm member but never announces itself as a service.
type passiveCodec struct{ GenericPresenceCodec }

func (passiveCodec) Build(realmID, identityHash []byte) ([]byte, error) { return nil, nil }

func (passiveCodec) Parse(appData []byte, expectedRealmID []byte) (map[string]string, error) {
	return map[string]string{}, nil
}

// startPair starts the passive (`second`) endpoint before the advertiser
// (`first`). The advertiser announces once at Start and only re-announces on
// the (long) periodic interval, so the passive peer must already be listening
// for the announce not to be lost.
func startPair(t *testing.T, first, second *Endpoint) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	for _, endpoint := range []*Endpoint{second, first} {
		if err := endpoint.Start(ctx); err != nil {
			cancel()
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = endpoint.Close() })
	}
	return cancel
}

func waitForPeer(t *testing.T, endpoint *Endpoint, wantIdentity string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, peer := range endpoint.Peers() {
			if peer.ID.String() == wantIdentity {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("peer %s was not discovered within 5s (peers=%v)", wantIdentity, endpoint.Peers())
}

func TestGenericRealmNodesDiscoverEachOther(t *testing.T) {
	advertiser, passive, _ := newPair(t, testRealmKey())
	cancel := startPair(t, advertiser, passive)
	defer cancel()

	waitForPeer(t, passive, advertiser.Name())
	found := passive.Peers()[0]
	if !bytes.Equal(found.ID.Bytes(), advertiser.identity.Hash()) {
		t.Fatalf("discovered identity = %x, want advertiser %x", found.ID.Bytes(), advertiser.identity.Hash())
	}
	if found.Route != advertiser.Destination() {
		t.Fatalf("discovered route = %s, want %s", found.Route, advertiser.Destination())
	}
}

func TestForeignRealmIsRejectedOnSharedPair(t *testing.T) {
	// Two endpoints on the same UDP pair but in different realms must not
	// become usable peers of one another.
	portA := freeUDPPort(t)
	portB := freeUDPPort(t)
	for portB == portA {
		portB = freeUDPPort(t)
	}
	root := t.TempDir()
	receivedA := make(chan meshbus.ReceivedMessage, 8)
	receivedB := make(chan meshbus.ReceivedMessage, 8)
	nodeA := newTestEndpoint(t, filepath.Join(root, "a"), portA, portB, testRealmKey(), true,
		func(_ context.Context, message meshbus.ReceivedMessage) error { receivedA <- message; return nil })
	nodeB := newTestEndpoint(t, filepath.Join(root, "b"), portB, portA, foreignRealmKey(), true,
		func(_ context.Context, message meshbus.ReceivedMessage) error { receivedB <- message; return nil })

	cancel := startPair(t, nodeA, nodeB)
	defer cancel()

	// Neither node may learn the other in its peer directory.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(nodeA.Peers()) > 0 || len(nodeB.Peers()) > 0 {
			t.Fatalf("cross-realm nodes became peers: A=%+v B=%+v", nodeA.Peers(), nodeB.Peers())
		}
		time.Sleep(50 * time.Millisecond)
	}

	// A direct send across realms must fail authentication and never be handled.
	sendCtx, stop := context.WithTimeout(context.Background(), 6*time.Second)
	defer stop()
	if err := nodeA.SendMessage(sendCtx, nodeB.Destination(), []byte("foreign")); fmt.Sprint(err) == "" {
		// If Send did not error synchronously, prove no delivery reached nodeB.
		select {
		case <-receivedB:
			t.Fatal("foreign-realm message was delivered")
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func TestDiscoveredPeerReceivesDirectBytesWithoutR1s(t *testing.T) {
	advertiser, passive, received := newPair(t, testRealmKey())
	cancel := startPair(t, advertiser, passive)
	defer cancel()
	waitForPeer(t, passive, advertiser.Name())

	sendCtx, stop := context.WithTimeout(context.Background(), 8*time.Second)
	defer stop()
	route := passive.Peers()[0].Route
	if err := passive.SendMessage(sendCtx, route, []byte("hello-meshbus")); err != nil {
		t.Fatal(err)
	}
	select {
	case message := <-received:
		if !bytes.Equal(message.Sender().Bytes(), passive.identity.Hash()) {
			t.Fatalf("sender = %x, want %x", message.Sender().Bytes(), passive.identity.Hash())
		}
		if string(message.Payload()) != "hello-meshbus" {
			t.Fatalf("payload = %q", message.Payload())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("direct meshbus bytes were not delivered")
	}
}

func TestEndpointsExchangeDirectBytesThroughSharedInstance(t *testing.T) {
	port := freeTCPPort(t)
	serverConfig := common.NewReticulumConfig()
	serverConfig.EnableTransport = true
	serverConfig.InMemoryStorage = true
	serverTransport := rnstransport.NewTransport(serverConfig)
	if err := serverTransport.Start(); err != nil {
		t.Fatal(err)
	}
	if err := serverTransport.InitializePathRequestHandler(); err != nil {
		_ = serverTransport.Close()
		t.Fatal(err)
	}
	server, err := interfaces.NewLocalServerInterface(port, "", false, func(client *interfaces.LocalClientInterface) {
		if registerErr := serverTransport.RegisterInterface(client.GetName(), &serializedLocalClient{LocalClientInterface: client}); registerErr != nil {
			_ = client.Stop()
		}
	}, nil)
	if err != nil {
		_ = serverTransport.Close()
		t.Fatal(err)
	}
	if err := server.Start(); err != nil {
		_ = serverTransport.Close()
		t.Fatal(err)
	}
	if err := serverTransport.RegisterInterface(server.GetName(), server); err != nil {
		_ = server.Stop()
		_ = serverTransport.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = server.Stop()
		_ = serverTransport.Close()
	})

	connector := func(transport *rnstransport.Transport) (*sharedinstance.Instance, error) {
		return connectSharedInstanceAt(transport, port, "", false)
	}
	received := make(chan meshbus.ReceivedMessage, 1)
	client, err := New(Config{
		connectShared: connector, EphemeralIdentity: true, RealmKey: testRealmKey(),
		Codec: passiveCodec{}, Handler: func(context.Context, meshbus.ReceivedMessage) error { return nil }, NetworkWait: 8 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	allocator, err := New(Config{
		connectShared: connector, EphemeralIdentity: true, RealmKey: testRealmKey(),
		Handler: func(_ context.Context, message meshbus.ReceivedMessage) error { received <- message; return nil }, NetworkWait: 8 * time.Second,
	})
	if err != nil {
		_ = client.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, endpoint := range []*Endpoint{client, allocator} {
		if err := endpoint.Start(ctx); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = endpoint.Close() })
	}
	if client.stack.shared.Mode != sharedinstance.ModeClient || allocator.stack.shared.Mode != sharedinstance.ModeClient {
		t.Fatalf("endpoint shared modes = %v, %v; want ModeClient", client.stack.shared.Mode, allocator.stack.shared.Mode)
	}

	waitForPeer(t, client, allocator.Name())
	sendContext, stop := context.WithTimeout(context.Background(), 8*time.Second)
	defer stop()
	if err := client.SendMessage(sendContext, client.Peers()[0].Route, []byte("shared-instance")); err != nil {
		t.Fatal(err)
	}
	select {
	case message := <-received:
		if string(message.Payload()) != "shared-instance" || message.Sender().String() != client.Name() {
			t.Fatalf("message sender=%s payload=%q", message.Sender().String(), message.Payload())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("direct message was not delivered through shared instance")
	}
}

func TestReconnectAfterLinkLoss(t *testing.T) {
	advertiser, passive, received := newPair(t, testRealmKey())
	cancel := startPair(t, advertiser, passive)
	defer cancel()
	waitForPeer(t, passive, advertiser.Name())

	sendCtx, stop := context.WithTimeout(context.Background(), 8*time.Second)
	defer stop()
	route := passive.Peers()[0].Route
	if err := passive.SendMessage(sendCtx, route, []byte("first")); err != nil {
		t.Fatal(err)
	}
	select {
	case message := <-received:
		if string(message.Payload()) != "first" {
			t.Fatalf("first payload = %q", message.Payload())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first direct message not delivered")
	}

	// Tear the outbound link and path so a later send must reconnect.
	destinationHash, key, err := parseDestination(route)
	if err != nil {
		t.Fatal(err)
	}
	_, _, active := passive.connections.resolve(destinationHash, key)
	if active == nil {
		t.Fatal("outbound session was not cached")
	}
	active.link.Teardown()
	passive.stack.transport.ExpirePath(destinationHash)

	if err := passive.SendMessage(sendCtx, route, []byte("second")); err != nil {
		t.Fatal(err)
	}
	select {
	case message := <-received:
		if string(message.Payload()) != "second" {
			t.Fatalf("second payload = %q", message.Payload())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("direct message after reconnect was not delivered")
	}
}

func TestPeerSnapshotsFeedBusFanout(t *testing.T) {
	advertiser, passive, _ := newPair(t, testRealmKey())
	cancel := startPair(t, advertiser, passive)
	defer cancel()
	waitForPeer(t, passive, advertiser.Name())

	routes := passive.Routes()
	if len(routes) != 1 {
		t.Fatalf("routes = %v, want exactly one discovered route", routes)
	}
	if routes[0] != advertiser.Destination() {
		t.Fatalf("route = %s, want %s", routes[0], advertiser.Destination())
	}
	// A full snapshot returns the bounded peer records.
	if peers := passive.Peers(); len(peers) != 1 {
		t.Fatalf("peers = %v, want exactly one", peers)
	}
}

func TestConfigValidation(t *testing.T) {
	nop := func(context.Context, meshbus.ReceivedMessage) error { return nil }
	// Ephemeral identity and identity source are mutually exclusive.
	if _, err := New(Config{IdentitySource: "identity", EphemeralIdentity: true, RealmKey: testRealmKey(), Handler: nop}); err == nil {
		t.Fatal("identity source and ephemeral identity accepted together")
	}
	// A negative announce interval is rejected.
	if _, err := New(Config{EphemeralIdentity: true, RealmKey: testRealmKey(), AnnounceInterval: -time.Minute, Handler: nop}); err == nil {
		t.Fatal("negative announce interval accepted")
	}
	// A missing handler is rejected.
	if _, err := New(Config{EphemeralIdentity: true, RealmKey: testRealmKey()}); err == nil {
		t.Fatal("missing handler accepted")
	}
}

type discoveryRecord struct {
	route   string
	appData []byte
}

func TestOnDiscoverReceivesRawAppData(t *testing.T) {
	portA := freeUDPPort(t)
	portB := freeUDPPort(t)
	for portB == portA {
		portB = freeUDPPort(t)
	}
	root := t.TempDir()
	discovered := make(chan discoveryRecord, 1)
	codec := GenericPresenceCodec{}
	nop := func(context.Context, meshbus.ReceivedMessage) error { return nil }
	advertiser, err := New(Config{
		Reticulum:         standaloneConfig(filepath.Join(root, "a"), portA, portB),
		EphemeralIdentity: true,
		RealmKey:          testRealmKey(),
		Codec:             codec,
		Handler:           nop,
		NetworkWait:       8 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	passive, err := New(Config{
		Reticulum:         standaloneConfig(filepath.Join(root, "b"), portB, portA),
		EphemeralIdentity: true,
		RealmKey:          testRealmKey(),
		Codec:             codec,
		Handler:           nop,
		OnDiscover: func(peer meshbus.PeerID, route string, hops uint8, metadata map[string]string, appData []byte) {
			select {
			case discovered <- discoveryRecord{route: route, appData: appData}:
			default:
			}
		},
		NetworkWait: 8 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	cancel := startPair(t, advertiser, passive)
	defer cancel()

	select {
	case record := <-discovered:
		if record.route != advertiser.Destination() {
			t.Fatalf("onDiscover route = %s, want %s", record.route, advertiser.Destination())
		}
		presence, realmID, err := parsePresence(record.appData)
		if err != nil {
			t.Fatalf("parse presence: %v", err)
		}
		if !bytes.Equal(realmID, advertiser.RealmID()) {
			t.Fatalf("presence realm = %x, want %x", realmID, advertiser.RealmID())
		}
		if presence.Protocol != "meshbus.v1" {
			t.Fatalf("presence protocol = %q", presence.Protocol)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("onDiscover was not called")
	}
}

func TestDestinationHelper(t *testing.T) {
	want := bytes.Repeat([]byte{0xab}, 16)
	got, key, err := parseDestination("  ABABABABABABABABABABABABABABABAB  ")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) || key != "abababababababababababababababab" {
		t.Fatalf("destination = %x, key = %q", got, key)
	}
	if _, _, err := parseDestination("short"); err == nil {
		t.Fatal("invalid destination accepted")
	}
}

func TestEphemeralEndpointsGetFreshInMemoryIdentities(t *testing.T) {
	nop := func(context.Context, meshbus.ReceivedMessage) error { return nil }
	first, err := New(Config{EphemeralIdentity: true, RealmKey: testRealmKey(), Handler: nop})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := New(Config{EphemeralIdentity: true, RealmKey: testRealmKey(), Handler: nop})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if first.Name() == second.Name() {
		t.Fatalf("fresh ephemeral endpoints reused identity %s", first.Name())
	}
}

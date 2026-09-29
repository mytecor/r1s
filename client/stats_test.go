package client

import (
	"context"
	"testing"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	coreclient "github.com/mytecor/r1s/internal/client"
	"github.com/mytecor/r1s/internal/transport/rns"
)

func TestClientStats(t *testing.T) {
	c := &Client{}
	stats := c.Stats()
	if len(stats.Requests) != 0 || len(stats.Executions) != 0 {
		t.Fatalf("expected empty stats on uninitialized client, got %+v", stats)
	}
}

func TestClientStatsAfterRun(t *testing.T) {
	core, err := coreclient.New(coreclient.Config{Identity: []byte("client")})
	if err != nil {
		t.Fatal(err)
	}
	c := &Client{
		core: core, identity: []byte("client"), waiters: make(map[string][]chan *r1sv1.Envelope),
		stateChanged: make(chan struct{}, 1),
	}
	endpoint := &fakeControllerEndpoint{client: c, discoveries: make(chan rns.Service, 1)}
	c.endpoint = endpoint
	endpoint.discoveries <- rns.Service{
		Destination: "allocator-destination",
		Identity:    "616c6c6f6361746f72",
		Descriptor:  rns.Descriptor{Capacity: map[string]uint32{"default": 1}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	_, err = c.Run(ctx, &r1sv1.ExecutionRequest{
		Workload:      &r1sv1.Workload{Image: "example.test/image@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		Policy:        &r1sv1.ExecutionPolicy{},
		ResourceClass: "default",
	}, RunOptions{OfferWait: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}

	stats := c.Stats()
	if len(stats.Requests) != 1 {
		t.Fatalf("expected 1 request snapshot, got %d", len(stats.Requests))
	}
	if len(stats.Executions) != 1 {
		t.Fatalf("expected 1 execution snapshot, got %d", len(stats.Executions))
	}
	if stats.Requests[0].OfferCount != 1 {
		t.Errorf("request offer count = %d, want 1", stats.Requests[0].OfferCount)
	}
	if stats.Executions[0].State.GetPhase() != r1sv1.ExecutionPhase_EXECUTION_PHASE_COMPLETED {
		t.Errorf("execution phase = %v, want COMPLETED", stats.Executions[0].State.GetPhase())
	}
}

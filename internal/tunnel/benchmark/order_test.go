package benchmark

import (
	"context"
	"testing"
	"time"
)

func TestOrderYggThenRNS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	y, err := newYggConnector()
	if err != nil {
		t.Fatalf("ygg new: %v", err)
	}
	cy, ay, err := y.Open(ctx)
	if err != nil {
		t.Fatalf("ygg open: %v", err)
	}
	cy.Close()
	ay.Close()
	y.Close()
	t.Log("ygg done")

	r, err := newRNSConnector()
	if err != nil {
		t.Fatalf("rns new: %v", err)
	}
	cr, ar, err := r.Open(ctx)
	if err != nil {
		t.Fatalf("rns open after ygg: %v", err)
	}
	cr.Close()
	ar.Close()
	r.Close()
	t.Log("rns done")
}

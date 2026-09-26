package client

import (
	"context"
	"sync"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
)

// StartOfferReleases retries in-memory offer cleanup without blocking assignment or state handling. The run controller retries
// the stable pending set, including late offers. Shutdown gives ACKs a bounded window;
// an unreachable allocator still has its original lease expiry.
func (o *Client) StartOfferReleases(parent context.Context, send func(context.Context, string, *r1sv1.Envelope) error) func() []PendingRelease {
	ctx, cancel := context.WithCancel(parent)
	drainRequest := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		attempted := make(map[string]time.Time)
		draining := false
		drain := (<-chan struct{})(drainRequest)
		for {
			pending := o.PendingReleases()
			if draining && len(pending) == 0 {
				return
			}
			var batch sync.WaitGroup
			launched := 0
			for _, release := range pending {
				id := release.Envelope.GetMessageId()
				if time.Since(attempted[id]) < time.Second {
					continue
				}
				attempted[id] = time.Now()
				batch.Add(1)
				go func() {
					defer batch.Done()
					sendContext, stop := context.WithTimeout(ctx, 500*time.Millisecond)
					defer stop()
					_ = send(sendContext, release.Destination, release.Envelope)
				}()
				launched++
				if launched == 4 {
					break
				}
			}
			batch.Wait()
			select {
			case <-ctx.Done():
				return
			case <-drain:
				draining = true
				drain = nil
			case <-ticker.C:
			}
		}
	}()
	return func() []PendingRelease {
		close(drainRequest)
		timer := time.NewTimer(2 * time.Second)
		defer timer.Stop()
		select {
		case <-done:
		case <-timer.C:
		}
		cancel()
		<-done
		return o.PendingReleases()
	}
}

package localserver

import (
	"errors"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/client"
)

// Watch streams durable execution-state transitions in revision order. It
// replays every retained transition after the requested position (the durable
// journal), then follows new transitions through the journal as they commit.
// A slow consumer that falls behind the retained journal is told to
// re-synchronize from a fresh position instead of silently missing revisions.
func (s *Server) Watch(in *r1sv1.LocalWatchRequest, stream r1sv1.LocalClient_WatchServer) error {
	after := in.GetAfter()
	ctx := stream.Context()

	// The journal is authoritative: never deliver an event that did not durably
	// commit. The live observer only wakes this loop; the journal is re-read
	// from the durable position so nothing is dropped for slow consumers.
	wake := make(chan struct{}, 1)
	cancel := s.backend.SubscribeWatch(ctx, func(client.WatchEvent) {
		select {
		case wake <- struct{}{}:
		default:
		}
	})
	defer cancel()

	for {
		events, contiguous := s.backend.WatchAfter(ctx, after)
		if !contiguous {
			// The watcher's durable position fell off the retained journal.
			// Tell it to re-synchronize from the current position.
			after = s.backend.WatchSeq(ctx)
			if err := stream.Send(&r1sv1.LocalWatchEvent{Sequence: after, Resync: true}); err != nil {
				return err
			}
			// Replay from the fresh position so the watcher rebuilds state.
			if events, contiguous = s.backend.WatchAfter(ctx, after); !contiguous {
				return errors.New("watch lost its durable position without a recoverable one")
			}
		}
		for _, event := range events {
			if err := stream.Send(&r1sv1.LocalWatchEvent{Sequence: event.Sequence, ExecutionId: event.ExecutionID, State: event.State}); err != nil {
				return err
			}
			after = event.Sequence
		}
		if len(events) == 0 && !contiguous {
			return errors.New("watch lost its durable position without a recoverable one")
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wake:
		}
	}
}

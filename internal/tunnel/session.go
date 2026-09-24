package tunnel

// Session is the single live tunnel session (mesh connection) for one
// execution, opened at accept and bound to the execution lifecycle. It
// carries the authenticated peer key reported by the edge and the
// client-supplied container port list the edge may splice streams to. Each
// opening stream references exactly one container port; every port came from
// the client-supplied list the allocator bound into the minted grant
// (validated only for well-formedness).
type Session struct {
	ExecutionID string
	PeerKey     []byte
	Targets     []Target
	Endpoint    Endpoint
	done        chan struct{}
}

// ResolveTarget returns the target for the given container port, or false when
// the port was not pre-authorized, so the edge can reject the stream with
// ReasonUnauthorized before any payload byte moves.
func (s *Session) ResolveTarget(port uint16) (Target, bool) {
	select {
	case <-s.Done():
		return Target{}, false
	default:
	}
	for _, target := range s.Targets {
		if target.Port == port {
			return target, true
		}
	}
	return Target{}, false
}

// Done closes when the registry revokes or releases this session.
func (s *Session) Done() <-chan struct{} { return s.done }

// Release ends only the specified session, never a newer replacement.
func (r *Registry) Release(session *Session) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if rec := r.records[session.ExecutionID]; rec != nil && rec.session == session {
		close(session.done)
		rec.session = nil
	}
}

// Active verifies that a caller holds the actual registry-issued session.
func (r *Registry) Active(session *Session) bool {
	if session == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	rec := r.records[session.ExecutionID]
	return rec != nil && rec.session == session
}

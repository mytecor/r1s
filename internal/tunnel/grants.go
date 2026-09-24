package tunnel

import "time"

// Grant is a minted, single-use, execution-scoped access grant.
type Grant struct {
	ExecutionID string
	ID          string
	ExpiresAt   time.Time
}

// grantState is the mutable grant record: identity and single-use consumption.
type grantState struct {
	id        string
	expiresAt time.Time
	consumed  bool
}

// record is the grant-and-session state for one execution.
type record struct {
	executionID string
	peerKey     []byte
	targets     []Target
	endpoint    Endpoint
	grant       *grantState
	session     *Session
}

// cloneTargets deep-copies a target slot list so the caller's backing slice is
// never aliased by a long-lived record or session.
func cloneTargets(targets []Target) []Target {
	if targets == nil {
		return nil
	}
	cloned := make([]Target, len(targets))
	copy(cloned, targets)
	return cloned
}

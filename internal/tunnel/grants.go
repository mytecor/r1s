package tunnel

// record is the mutable tunnel state for one execution: the owner-bound edge
// peer key and target list, the allocator endpoint advertisement, and the
// single live session. It replaces the retired grant token (F22-06): there is
// no grant ID, no TTL, and no single-use consumption. The binding is
// established by the authenticated owner's ExecutionTunnelOpen and lives for
// the execution's lifetime; an allocator restart drops it (the run re-opens).
type record struct {
	executionID string
	peerKey     []byte
	targets     []Target
	endpoint    Endpoint
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

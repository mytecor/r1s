// Package protocol validates the versioned wire contract before domain state is mutated.
package protocol

// LeaseExpiredDetail is the stable terminal-state detail that marks an
// execution evicted because its client-held lease expired without renewal. It
// is deliberately distinct from any client-supplied cancellation reason so
// clients can tell lease loss from cancellation and, for example, re-request
// lost work.
const LeaseExpiredDetail = "execution lease expired"

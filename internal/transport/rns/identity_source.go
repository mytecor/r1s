package rns

import meshrns "github.com/mytecor/r1s/meshbus/rns"

// Identity helpers remain available at the r1s adapter boundary for command
// wiring, while their implementation is owned by meshbus/rns.
var (
	IsInlineIdentitySource = meshrns.IsInlineIdentitySource
	IdentitySeed           = meshrns.IdentitySeed
)

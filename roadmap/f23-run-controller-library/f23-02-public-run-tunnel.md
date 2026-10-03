# F23-02 — Public run-owned tunnel data plane

**Status:** ✅ Complete (2026-10-03)

## Outcome

External Go services can create a public `client.RunTunnel`, update its active execution from
`EventAttemptAssigned`, and dial reliable bidirectional streams to fixed container ports without
importing `internal/tunnel` or `internal/tunnel/yggdrasil`.

The tunnel belongs to the logical run. Its embedded Yggdrasil client edge and authenticated pair
are initialized on the first dial. Concurrent streams reuse that pair. Selecting a new execution
closes the old pair and its streams, while the replacement remains lazy until the next dial. The
existing authenticated `Client.OpenTunnel` control request and allocator-side target validation
remain the authorization boundary.

`r1s run -p` is now only the CLI adapter that binds loopback TCP listeners and relays each accepted
connection through `RunTunnel.Dial`; it no longer derives peer keys, creates overlay nodes, writes
routing preambles, or caches execution-specific pairs.

## Acceptance

- Valid targets create a tunnel without starting an overlay edge; zero or missing targets fail.
- Dial before an active attempt and dial to an undeclared port return classified errors.
- First and concurrent dials lazily establish and share one authenticated pair.
- Attempt replacement closes the previous pair and streams; later dials target the replacement.
- Close is idempotent, releases the pair and edge, and prevents later dials.
- CLI publish tests use the public tunnel surface, ordinary runs allocate no tunnel resources, and
  `make check` passes.

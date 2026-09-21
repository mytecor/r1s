# F21-01 — Private tunnel RNS transport stack

**Status:** ⏳ Planned

## Outcome

A minimal, self-contained tunnel transport in a new `rns` subpackage of
[`internal/tunnel`](../../internal/tunnel)
that runs as a **separate** Reticulum data plane over the system `yggdrasil` daemon — the client
and allocator edges of the tunnel open their own private RNS instance with exactly one Ygg-backed
Backbone/TCP interface, and the main control-plane RNS is never involved in tunnel data.

The package deliberately does **not** reuse [`internal/transport/rns`](../../internal/transport/rns)
(whose `Endpoint` is tuned for the control plane: announces, allocator discovery, protobuf
Envelope, cluster HMAC challenge, connection registry). It is a fresh, minimal implementation.

## Scope

- `stack.go` — construct a Reticulum-Go `Transport` that is independent of the control-plane
  transport: `EnableTransport = false`, no interface borrowed from the control RNS, only one
  `Backbone`/`TCP` interface bound to the allocator's Ygg IPv6 address and tunnel listener port.
  The interface is closed under the shared-cluster IFAC derived from the cluster secret
  (`network_name = "r1s-tunnel"`, `passphrase = HKDF(cluster-key, "r1s/tunnel-ifac/v1")`).
- `identity.go` — re-use the existing persistent RNS identity (client and `r1sd`) for the tunnel
  Link; **do not** mint a separate edge/Ygg key. `Link.Identify()` on the established Link is the
  identity source of truth.
- `dial.go` / `listen.go` — client-side `Dial` that opens a Backbone/TCP connection to the
  advertised `[ygg-ipv6]:port`, establishes the RNS Link and calls `Link.Identify()`; allocator
  `Listen` that accepts inbound Links and awaits identification.
- `session.go` / `stream.go` — map `Link`/`Channel`/`Buffer` to the narrowed
  `tunnel.Conn` (`io.ReadWriteCloser` + `CloseWrite()`); implement the one-Link-per-stream model so
  one local TCP connection is one tunnel stream. Use only stock `Channel`/`Buffer` primitives; no
  custom mux, framing, stream IDs, segmentation, or per-stream flow control.
- Keep the constructor signatures such that the allocator and client edges can be swapped into the
  F21-03/F21-04 integration points without inventing transport-specific protocol types in the core.

## Acceptance

- Two `internal/tunnel/rns` stacks connect over a local Backbone/TCP pair (unit-level, no real Ygg
  required for the logic path): Link established, `Link.Identify()` returns the other side's
  persistent identity.
- The tunnel transport has exactly its one Ygg Backbone/TCP interface and `EnableTransport == false`; a
  test asserts no control-plane RNS interface is attached.
- Payloads larger than the RNS MTU round-trip through `Buffer`; a half-close (`CloseWrite`) on one
  side delivers EOF to the far reader.
- No `internal/transport/rns` types are imported by `internal/tunnel/rns` for tunnel data.
- `go build ./...`, `go vet ./...`, `go test -race ./...` pass for the new package.

<div align="center">
  <img src="./assets/logo.svg" alt="r1s logo" height="96">

  [![Release](https://img.shields.io/github/v/release/mytecor/r1s?sort=semver&style=for-the-badge&label=Release&color=151515)](https://github.com/mytecor/r1s/releases/latest)[![CI](https://img.shields.io/github/actions/workflow/status/mytecor/r1s/ci.yml?branch=main&style=for-the-badge&label=CI&color=151515)](https://github.com/mytecor/r1s/actions/workflows/ci.yml)![Go](https://img.shields.io/badge/Go-1.27.1-151515?logo=go&style=for-the-badge)![containerd](https://img.shields.io/badge/containerd-151515?logo=containerd&style=for-the-badge)![Protobuf](https://img.shields.io/badge/Protobuf-151515?logo=protobuf&style=for-the-badge)

  **r1s** *("ris", Scandinavian for "rice")* is a decentralized OCI workload execution fabric over the Reticulum Network Stack (RNS).
</div>

## How it works

The `r1s` client publishes workload demand, collects offers from independent `r1sd` allocators, and
selects where to run each workload. There is no global API server, scheduler, registry, or shared
state.

1. A client broadcasts an execution request — optionally with exact-match placement constraints.
2. Allocators with available capacity (and, when constrained, only those whose advertised
   capabilities match) return time-limited offers.
3. The client selects one offer and releases the others.
4. The selected allocator starts the OCI workload and reports its state.

Workloads continue through client disconnects and allocator restarts. Every RNS link authenticates
the peer identity and proves possession of the cluster key before carrying control messages.
Container logs remain local to the allocator and are transferred only after an explicit,
authenticated `logs` request.

RNS is the resilient, low-bandwidth control plane. OCI images remain ordinary digest-pinned registry
references fetched by containerd.

Each `request` creates one immutable execution. Durable, client-owned desired state for one
execution is `request --keep-alive`: the recorded intent is renewed for the service lifetime, and
a lost lease re-requests the recorded workload with its allocator pinning. A manifest-level
`r1s deploy` layer is deferred (see [BACKLOG.md](./roadmap/BACKLOG.md)); if built, it would
compose `request`, `inspect`, `cancel`, and the local `Watch` API without adding deployment state
to allocators or the wire protocol.

See [ARCHITECTURE.md](./ARCHITECTURE.md) for protocol, authority, lifecycle, persistence, and adapter
boundaries.

## Install

Download an archive for Linux, macOS, or Windows from the
[releases page](https://github.com/mytecor/r1s/releases).
Copy binaries to a directory on `PATH`:

```sh
mkdir -p "$HOME/.local/bin"
cp r1s r1sd "$HOME/.local/bin/"
```

Release binaries are unsigned. On macOS, remove the quarantine attribute from browser downloads:

```sh
xattr -d com.apple.quarantine "$HOME/.local/bin/r1s" "$HOME/.local/bin/r1sd"
```

## Build from source

```sh
git clone https://github.com/mytecor/r1s
cd r1s
GOBIN="$HOME/.local/bin" go install ./cmd/r1s ./cmd/r1sd
```

`r1sd` requires a reachable containerd daemon, but does not require root itself. Run it as any user
that can access the containerd socket and write its configured identity, state, and log paths.
Both binaries require an already-running Reticulum shared instance (`reticulum-go` or Python
`rnsd`) and attach to its platform-default local endpoint. They fail closed when the shared
instance is unavailable; r1s never starts a private RNS stack or takes ownership of the shared
listener.

Execution tunnels require Linux and a local containerd daemon in the same PID namespace as
`r1sd`. The tunnel connects to loopback inside the selected container's network namespace;
it never forwards client-selected ports to the allocator host. The allocator needs permission
to read the task's namespace and enter it (`CAP_SYS_ADMIN`), plus `CAP_NET_ADMIN` to bring up
loopback in a fresh container namespace. Missing permissions fail the stream explicitly;
normal workload execution does not require enabling tunnels.

RNS remains the discovery, identity, and control plane. Tunnel application bytes use the dedicated
embedded Yggdrasil adapter; the F21 private-RNS data-plane experiment was rejected by its recorded
benchmark and is being rolled back without changing the tunnel UX or container-isolation boundary.

`--identity` accepts an existing or new identity file path, or a private RNS identity in the same
formats as Reticulum-Go's identity importer: 128-character hex, Base32, or Base64. Existing files take
priority. An inline identity is not persisted; its default state and log paths are placed under
`~/.config/r1s` and named with the public identity hash. Use `--state` and the allocator's `--logs`
flag to override them.

## Quick start

Create the cluster once on the first participant. Either binary can create it; this example uses a
client:

```sh
r1s cluster init
# Cluster ID: <public identifier>
# Join token: r1s1:<secret>
```

If an allocator is the first participant instead, run `r1sd cluster init`. Save the join token:
`init` prints it only once. Do not run `init` independently on other participants, because that
creates a different cluster.

Join every additional client with the same token to persist membership:

```sh
r1s cluster join 'r1s1:<secret>'
```

Join every allocator that did not create the cluster to persist membership, then start it:

```sh
r1sd cluster join 'r1s1:<secret>'
r1sd \
  --identity /var/lib/r1s/identity \
  --capacity default=2
```

Both binaries store membership in `~/.config/r1s/cluster` by default, resolved for the OS account
running the process. For workload commands and `r1sd`, the global `--cluster` flag accepts either
an inline join token or a state file path. The value is first loaded as a file; if that file does
not exist, the same value is parsed as a join token:

```sh
r1sd --cluster 'r1s1:<secret>' \
  --identity '<private RNS identity hex, Base32, Base64, or path>'

r1s --cluster /custom/path/to/cluster \
  --identity "$HOME/.config/r1s/identity" \
  list
```

An inline token is not written to disk and must be supplied on every invocation. With
`cluster init`, `cluster join`, and `cluster show`, `--cluster` remains the destination state file
path.

The shared token establishes cluster membership; the shared RNS daemon owns the interfaces and
routing that reach the other participants. An allocator using an admission allowlist must also
include each permitted client's RNS identity.

Submit a digest-pinned OCI image. Allocators are discovered through RNS announces:

```sh
r1s \
  --identity "$HOME/.config/r1s/identity" \
  request \
  '{"workload":{"image":"registry.example/image@sha256:..."},"policy":{"resultRetention":"86400s"},"resourceClass":"default"}'
```

The command returns durable request and execution IDs after assignment. The execution is then
bounded by a client-held lease (10 minutes by default). Add `--keep-alive` (optionally `--lease`)
to keep it running: direct mode blocks, renews the lease, and re-requests the recorded workload if
the lease is ever lost; `--socket` mode records the duty durably in the service, whose renewal
loop holds it while `r1s serve` runs. A re-request preserves the allocator pinning (`--allocator`)
recorded with the intent, and the intent itself is durable: an interrupted direct-mode keep-alive
request leaves it recorded, and a later `r1s serve` with the same client state resumes holding it.
An execution whose lease expires without renewal is evicted
locally; its terminal metadata stays retrievable within `result_retention`. Use the execution ID
with `inspect`, `cancel`, `result`, or `logs`; use `list` to show saved requests. Run `r1s --help`
or a subcommand with `--help` for all options.

## Local client API

For applications that want a persistent client instead of rebuilding RNS, identity, and durable
state per command, run the local service once and point workflows at it:

```sh
r1s serve \
  --identity "$HOME/.config/r1s/identity" &

r1s --socket "$HOME/.config/r1s/client.sock" \
  request '{"workload":{"image":"registry.example/image@sha256:..."},"resourceClass":"default"}'
r1s --socket "$HOME/.config/r1s/client.sock" list
r1s --socket "$HOME/.config/r1s/client.sock" inspect <execution-id>
```

`r1s serve` keeps one client identity, state store, and RNS endpoint alive for the service lifetime
and serves a versioned local gRPC API over a Unix socket (default mode `0600`, `--socket-mode` to
change, default socket at `~/.config/r1s/client.sock`). It also streams durable execution-state
changes through `Watch` so applications need not poll. The service is the only continuous
lease-renewal holder: every lease-holding intent recorded by `request --keep-alive` is replayed from durable
state on each tick, and a lost lease re-requests the recorded workload. The service is a local
frontend for one client identity — it is not a cluster-wide API server and is never reachable over
RNS.

Direct mode remains the default and is still the way to run `serve` and `cluster`. Passing
`--socket <path>` routes `request`, `list`, `inspect`, `result`, `cancel`, and `logs`
through the local service; an unreachable explicit socket is an error, never a silent fallback to a
new client identity.

Without `--socket`, the CLI transparently discovers a running local service: when the default
socket (`~/.config/r1s/client.sock`) is already listening, workflow commands route through it
instead of building a fresh RNS endpoint for the invocation. If no service is running, the command
falls back to direct mode as before. Explicit `--socket` remains authoritative and never silently
falls back.

## Documentation

- [ARCHITECTURE.md](./ARCHITECTURE.md) — component boundaries, authority, and lifecycle.
- [ROADMAP.md](./ROADMAP.md) — features, current status, and open work.
- [CONTRIBUTING.md](./CONTRIBUTING.md) — development and verification workflow.

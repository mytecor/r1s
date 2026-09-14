#!/usr/bin/env python3
"""Python-reference RNS peer for the r1s live interoperability harness.

Runs one RNS node using the upstream Python reference implementation (the
`RNS` module) that announces an r1s allocator-aspect destination with a bounded
capability descriptor — the same announce the Go r1sd Endpoint publishes — and
accepts incoming links.

It is the reference counterpart to `internal/transport/rns/endpoint.go` and is
driven by `interop_python_test.go` under `RUN_LIVE_INTEROP=1`. It requires the
`RNS` module to be importable by the interpreter that launches it (set
`PYTHON_INTEROP` to a pipx `rns` venv or any interpreter with `RNS` installed).

Stdout protocol (line-based, consumed by the Go harness):

    READY
    <32-hex destination hash>
    LINK_UP <32-hex link id>            (per accepted link)
    DATA <hex>                          (raw plaintext received over a link)

The peer re-announces periodically, matching how a real allocator keeps its
service discoverable, so the Go harness's announce-driven discovery is reliable.
"""

import argparse
import json
import os
import sys
import time

# Allow `RETICULUM_PATH` to point at an RNS install, mirroring the reference
# Reticulum-Go interop scripts.
_reticulum_path = os.environ.get("RETICULUM_PATH")
if _reticulum_path:
    sys.path.insert(0, os.path.abspath(_reticulum_path))

import RNS  # noqa: E402  (upstream Python Reticulum reference)

APP_NAME = "r1s"
ASPECT = "allocator"
PROTOCOL = "r1s.v1"
MAX_DESCRIPTOR = 256  # must match descriptor.maxDescriptorBytes


class Peer:
    def __init__(self, configdir, capacity, announce, no_ratchet, dump):
        RNS.Reticulum(configdir=configdir)
        self.identity = RNS.Identity()
        self.destination = RNS.Destination(
            self.identity,
            RNS.Destination.IN,
            RNS.Destination.SINGLE,
            APP_NAME,
            ASPECT,
        )
        self.destination.accepts_links(True)
        if no_ratchet:
            self.destination.ratchets = None
        descriptor = json.dumps(
            {"protocol": PROTOCOL, "capacity": capacity}, separators=(",", ":")
        ).encode("utf-8")
        if len(descriptor) > MAX_DESCRIPTOR:
            raise ValueError("descriptor exceeds the %d-byte bound" % MAX_DESCRIPTOR)
        self.destination.set_default_app_data(descriptor)
        self.announce = announce
        self.dump = dump
        self.destination.set_link_established_callback(self.on_link)

    def on_link(self, link):
        link.set_link_closed_callback(lambda _link: None)
        print("LINK_UP " + link.hexhash, flush=True)
        if self.dump:
            link.set_packet_callback(self.on_packet)

    def on_packet(self, plaintext, _packet):
        print("DATA " + plaintext.hex(), flush=True)

    def run(self):
        print("READY", flush=True)
        print(self.destination.hexhash, flush=True)
        while True:
            if self.announce:
                self.destination.announce()
            time.sleep(2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--configdir", required=True)
    parser.add_argument("--capacity", default='{"default": 2}')
    parser.add_argument("--announce", action="store_true")
    parser.add_argument("--no-ratchet", action="store_true")
    parser.add_argument("--dump", action="store_true")
    args = parser.parse_args()
    Peer(
        args.configdir,
        json.loads(args.capacity),
        args.announce,
        args.no_ratchet,
        args.dump,
    ).run()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(0)

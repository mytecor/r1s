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
    CHANNEL_MSG <len> <hex>             (per r1s envelope over a Channel)
    DATA <hex>                          (raw plaintext received over a link)

In the default (dump) mode the peer accepts links and reports raw plaintext
packets. With `--channel` it instead opens an RNS Channel on each accepted
link, registers the r1s envelope message type, prints every received envelope
as `CHANNEL_MSG`, and echoes the exact bytes back over the same Channel so the
Go harness can verify bidirectional reliable Channel delivery end to end.

RNS diagnostics are routed to stderr (via the log-callback destination) so the
line-based stdout protocol stays clean regardless of verbosity.

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
from RNS.Channel import MessageBase as ChannelMessageBase  # noqa: E402

APP_NAME = "r1s"
ASPECT = "allocator"
PROTOCOL = "r1s.v1"
MAX_DESCRIPTOR = 256  # must match descriptor.maxDescriptorBytes
ENVELOPE_MSGTYPE = 0x0101  # must match internal/transport/rns wire.go


class EnvelopeMessage(ChannelMessageBase):
    """Carries one serialized r1s envelope over a Channel.

    MSGTYPE and wire layout mirror the Go endpoint so the two implementations
    interoperate byte-for-byte: a 6-byte Channel header (MSGTYPE, sequence,
    length) followed by the raw envelope bytes.
    """

    MSGTYPE = ENVELOPE_MSGTYPE

    def __init__(self, data=None):
        self.data = bytes(data or b"")

    def pack(self):
        return self.data

    def unpack(self, raw):
        self.data = bytes(raw)


class Peer:
    def __init__(self, configdir, capacity, announce, no_ratchet, dump, channel, verbosity=0):
        # Route RNS diagnostics to stderr so the stdout protocol stream stays
        # clean line-based output (READY/hash/LINK_UP/CHANNEL_MSG) regardless
        # of log level.
        def to_stderr(line):
            sys.stderr.write(line + "\n")
            sys.stderr.flush()

        RNS.logdest = RNS.LOG_CALLBACK
        RNS.logcall = to_stderr
        RNS.Reticulum(configdir=configdir, verbosity=int(verbosity), logdest=RNS.LOG_CALLBACK)
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
        self.channel = channel
        self.dump = dump
        self.destination.set_link_established_callback(self.on_link)

    def on_link(self, link):
        link.set_link_closed_callback(lambda _link: None)
        link_hexhash = (
            getattr(link, "hexhash", None)
            or (getattr(link, "hash", None) or getattr(link, "link_id", b"")).hex()
        )
        print("LINK_UP " + link_hexhash, flush=True)
        # Diagnostic: mirror link events to stderr so they survive the gated
        # harness's stdout protocol stream.
        sys.stderr.write("LINK_UP " + link_hexhash + "\n")
        sys.stderr.flush()
        if self.channel:
            ch = link.get_channel()
            ch.register_message_type(EnvelopeMessage)

            def on_message(message):
                if isinstance(message, EnvelopeMessage):
                    print(
                        "CHANNEL_MSG " + str(len(message.data)) + " " + message.data.hex(),
                        flush=True,
                    )
                    sys.stderr.write(
                        "CHANNEL_MSG " + str(len(message.data)) + "\n"
                    )
                    sys.stderr.flush()
                    # Echo the exact envelope bytes back over the same Channel
                    # so the Go endpoint can verify bidirectional Channel
                    # integrity (and that reliable delivery was achieved).
                    ch.send(EnvelopeMessage(message.data))
                    return True
                return False

            ch.add_message_handler(on_message)
        elif self.dump:
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
    parser.add_argument("--channel", action="store_true")
    parser.add_argument("--verbosity", default="0")
    args = parser.parse_args()
    Peer(
        args.configdir,
        json.loads(args.capacity),
        args.announce,
        args.no_ratchet,
        args.dump,
        args.channel,
        args.verbosity,
    ).run()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(0)

# meshbus wire contracts

All integer fields are unsigned big-endian unless stated otherwise. A receiver must reject unknown
versions, malformed lengths, empty identities, and values outside its configured bounds before
application delivery. None of the formats below serializes an authoritative sender: sender
authority always comes from the authenticated transport session.

## Realm v1

A realm key is exactly 32 bytes.

The public realm ID is:

```text
SHA-256("meshbus-realm-id-v1" || realm_key)
```

A membership proof is:

```text
HMAC-SHA256(
  realm_key,
  "meshbus-realm-auth-v1" || nonce || challenger_identity || responder_identity,
)
```

The nonce is exactly 32 bytes. Challenger and responder identities must be non-empty and equal in
length. Their order is significant.

## Reticulum Channel messages

The RNS adapter reserves Channel message type `0x0100` for realm authentication and `0x0101` for
opaque direct messages.

An authentication message payload is:

```text
kind:u8 | nonce:32 | proof:0-or-32
```

Kind `1` is a challenge and has no proof. Kind `2` is a response and has a 32-byte Realm v1 proof.
Opaque direct-message payloads are non-empty and bounded by the negotiated Channel MDU.

## Presence `meshbus.v1`

RNS announce application data is a UTF-8 JSON object of at most 256 bytes:

```json
{"p":"meshbus.v1","realm":"<64 lowercase hex characters>","meta":{"key":"value"}}
```

`meta` is optional advisory application data and contains at most 16 entries. The complete encoded
descriptor, rather than an individual metadata field, is the authoritative size bound. A foreign
realm is rejected before it enters peer discovery.

## Event `MBE` v1

An event frame has this exact layout:

```text
offset  size  field
0       4     magic: 0x4d 0x42 0x45 0x01 ("MBE" + version 1)
4       16    event ID
20      8     publication time, Unix milliseconds
28      4     TTL, milliseconds
32      2     topic byte length
34      2     content-type byte length
36      4     payload byte length
40      ...   topic bytes, content-type bytes, payload bytes
```

The event ID is non-zero. Topic and payload are non-empty. Topics are at most 128 bytes and contain
ASCII letters, digits, `.`, `_`, or `-`, without empty dot-separated segments. Content type is at
most 128 printable ASCII bytes. Default payload and TTL bounds are 64 KiB and one hour; a receiver
may configure smaller bounds.

Expiry is the earlier of `published_at + TTL` and `received_at + TTL`, so a future publisher clock
cannot extend the receiver's retention window.

## Compatibility

The Go API remains pre-v1. The wire markers and domain separators documented here are stable:
incompatible changes require a new domain separator, presence protocol value, event magic version,
or Channel message type. Existing meanings are never silently reassigned.

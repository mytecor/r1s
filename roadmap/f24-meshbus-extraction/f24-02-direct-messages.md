# F24-02 — Extract authenticated direct messages

**Status:** ✅ Complete

## Outcome

The public [`meshbus`](../../meshbus) package defines an opaque direct-message contract with an
immutable `PeerID`, isolated payload bytes, a handler, and a sender interface. It imports no r1s
protocol or Reticulum-Go packages.

The RNS session layer now performs only peer identification, realm authentication, Channel
delivery, and bounded pre-authentication buffering. Once authenticated, it emits a meshbus
`ReceivedMessage`; it no longer unmarshals or validates r1s Protobuf messages.

A separate r1s envelope adapter owns Protobuf encoding and validation. On receive it always
replaces `Envelope.sender` with the meshbus message's transport-authenticated peer before protocol
validation. On send it serializes the envelope and delegates the bytes to `SendMessage`. The
existing RNS Channel message type and wire bytes remain unchanged.

## Authority

- Only the identity authenticated by the RNS Link becomes `ReceivedMessage.Sender`.
- A sender copied into opaque payload bytes has no authority.
- Empty identities and empty direct messages are rejected.
- `PeerID.Bytes` and `ReceivedMessage.Payload` return copies, preventing handlers from mutating the
  stored authority or message.
- r1s authorization remains above meshbus and continues to validate commands against the injected
  envelope sender.

## Acceptance

- Contract tests prove that sender and payload inputs and outputs do not alias caller memory.
- Unauthenticated Channel data never reaches the direct-message handler.
- Authenticated opaque bytes reach the handler with the Link identity.
- Forged serialized envelope senders are replaced before r1s validation.
- Malformed or invalid r1s envelopes never reach allocator or client handlers.
- Existing UDP, shared-instance, reconnect, foreign-realm, and Python interoperability tests pass.
- `make check` passes.

## Notes

The Reticulum-specific endpoint is still hosted under r1s while extraction is staged. F24-03 can
build pub/sub over the public direct-message contract without changing r1s request/offer/assign
semantics. Moving the adapter to a separately versioned module remains the packaging decision
recorded in [BACKLOG.md](../BACKLOG.md).

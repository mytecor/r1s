# F1-01 — Define the control protocol

**Status:** Complete

## Outcome

Define the `r1s.v1` Protobuf package for the envelope, OCI workload, finite execution policy,
offer, assignment, cancellation, and execution state messages.

## Acceptance

- The new `api/proto/r1s/v1/control.proto` compiles with `protoc-gen-go`.
- Generated code builds as part of `go test ./...`.
- Protocol validation rejects missing identity, message, and required payload fields.

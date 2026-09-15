#!/usr/bin/env bash
# Reproducible storage-scaling measurement for the allocator state store.
#
# The allocator persists one whole JSON snapshot in a single bbolt transaction
# after every accepted transition. This script runs the F11-02 benchmarks at a
# fixed benchtime so the reported per-op costs are comparable across machines,
# and records a header identifying the host, Go version, and commit so results
# can be reproduced later.
#
# Usage:
#   scripts/storage-bench.sh [benchtime]
#
# benchtime defaults to "3x" (three iterations per history bucket), which keeps
# the whole run under a couple of minutes on an Apple Silicon laptop while still
# averaging away cold-start noise. Use "10x" or "50x" for tighter estimates.

set -euo pipefail

cd "$(dirname "$0")/.."

BENCHTIME="${1:-3x}"

echo "r1s storage-scaling benchmark"
echo "host: $(uname -s -m)"
echo "commit: $(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
echo "go: $(go version)"
echo "benchtime: -benchtime=$BENCHTIME"
echo

go test -run '^$' -bench '.' -benchtime="$BENCHTIME" -benchmem \
  ./internal/allocator/ 2>&1 \
  | grep -E '^(Benchmark|goos|goarch|pkg|PASS|FAIL|ok)' \
  || { echo "benchmark failed" >&2; exit 1; }

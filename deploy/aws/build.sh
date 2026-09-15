#!/usr/bin/env bash
# Cross-compile the Lambda order worker.
#
# One binary serves every version: the version is chosen per function by the
# ORDER_VERSION environment variable, so all five Lambdas ship identical code
# and differ only in configuration.
set -euo pipefail

cd "$(dirname "$0")/../.."

OUT=${OUT:-dist/lambda}
ARCH=${ARCH:-arm64}   # Graviton: cheaper and quicker to start than x86_64.

mkdir -p "$OUT"

echo "Building the Lambda worker for linux/${ARCH}…"
CGO_ENABLED=0 GOOS=linux GOARCH="$ARCH" \
  go build -trimpath -ldflags="-s -w" -o "$OUT/bootstrap" ./cmd/lambdaworker

# Lambda's provided runtime looks for an executable named exactly "bootstrap".
(cd "$OUT" && rm -f worker.zip && zip -q worker.zip bootstrap)

echo "Built $OUT/worker.zip ($(du -h "$OUT/worker.zip" | cut -f1))"

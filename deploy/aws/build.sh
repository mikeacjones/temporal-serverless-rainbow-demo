#!/usr/bin/env bash
# Cross-compile the Lambda workers: the order worker and the control plane.
#
# One order binary serves every version: the version is chosen per function by
# the ORDER_VERSION environment variable, so all five order Lambdas ship
# identical code and differ only in configuration.
#
# The control plane is a second binary because it is a different worker on a
# different task queue, registered as its own Worker Deployment.
set -euo pipefail

cd "$(dirname "$0")/../.."

OUT=${OUT:-dist/lambda}
ARCH=${ARCH:-arm64}   # Graviton: cheaper and quicker to start than x86_64.

mkdir -p "$OUT"

# Lambda's provided runtime looks for an executable named exactly "bootstrap",
# so each zip contains one file under that name.
build() {
  local pkg="$1" zip="$2" label="$3"
  echo "Building the $label for linux/${ARCH}…"
  CGO_ENABLED=0 GOOS=linux GOARCH="$ARCH" \
    go build -trimpath -ldflags="-s -w" -o "$OUT/bootstrap" "$pkg"
  (cd "$OUT" && rm -f "$zip" && zip -q "$zip" bootstrap && rm -f bootstrap)
  echo "  built $OUT/$zip ($(du -h "$OUT/$zip" | cut -f1))"
}

build ./cmd/lambdaworker   worker.zip  "order worker"
build ./cmd/controllambda  control.zip "control plane"

#!/usr/bin/env bash
# Start a local Temporal dev server with the search attributes this demo needs.
#
# The order Workflow publishes OrderVersion / OrderStep / OrderHealth about
# itself so the dashboard can read thousands of orders in bulk instead of
# Querying each one.
#
# --metrics-port exposes the server's Prometheus endpoint, which is the only
# place the real sync match rate is reported.
set -euo pipefail

exec temporal server start-dev \
  --ip 0.0.0.0 \
  --metrics-port 9233 \
  --search-attribute OrderVersion=Keyword \
  --search-attribute OrderStep=Keyword \
  --search-attribute OrderHealth=Keyword \
  "$@"

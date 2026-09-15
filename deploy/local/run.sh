#!/usr/bin/env bash
# Run the whole demo on the host, without containers.
#
# Useful when iterating on code: rebuild and restart one piece without waiting
# for an image. For an actual demo, prefer `make up` — containers keep the five
# worker versions and their environments from drifting apart.
#
# Everything is started in the background and killed together on Ctrl-C.
set -euo pipefail

cd "$(dirname "$0")/../.."

BIN=./bin
BACKEND_PORT="${BACKEND_PORT:-8088}"
DASHBOARD_PORT="${DASHBOARD_PORT:-3000}"
LOGS="${LOGS:-.run-logs}"

export TEMPORAL_ADDRESS="${TEMPORAL_ADDRESS:-localhost:7233}"
export TEMPORAL_NAMESPACE="${TEMPORAL_NAMESPACE:-default}"
export TEMPORAL_DEPLOYMENT_NAME="${TEMPORAL_DEPLOYMENT_NAME:-rainbow-orders}"
export ORDER_PROFILE="${ORDER_PROFILE:-demo}"
export LOG_FORMAT="${LOG_FORMAT:-text}"

mkdir -p "$LOGS"

pids=()
cleanup() {
  echo
  echo "stopping…"
  for pid in "${pids[@]:-}"; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

start() {
  local name=$1; shift
  "$@" >"$LOGS/$name.log" 2>&1 &
  pids+=($!)
  echo "  $name  (log: $LOGS/$name.log)"
}

# Reuse a dev server if one is already up, so restarting the app does not throw
# away the order history you were just looking at.
if temporal operator cluster health --address "$TEMPORAL_ADDRESS" >/dev/null 2>&1; then
  echo "Temporal: reusing the server already running at $TEMPORAL_ADDRESS"
else
  echo "Temporal:"
  start temporal ./deploy/local/dev-server.sh --log-level error
  for _ in $(seq 1 40); do
    temporal operator cluster health --address "$TEMPORAL_ADDRESS" >/dev/null 2>&1 && break
    sleep 1
  done
fi

if [[ ! -x "$BIN/worker" ]]; then
  echo "Building binaries…"
  mkdir -p "$BIN"
  go build -o "$BIN/" ./cmd/...
fi

echo "Order workers (all five versions run at once):"
for version in v1 v2 v3 v4 v5; do
  ORDER_VERSION=$version start "order-$version" "$BIN/worker"
done

echo "Control plane:"
start control "$BIN/controlworker"

echo "Backend:"
PORT="$BACKEND_PORT" start backend "$BIN/backend"

echo "Dashboard:"
start dashboard python3 -m http.server "$DASHBOARD_PORT" --directory frontend

cat <<BANNER

  Dashboard    http://localhost:$DASHBOARD_PORT/?api=http://localhost:$BACKEND_PORT
  Temporal UI  http://localhost:8233
  Backend API  http://localhost:$BACKEND_PORT

  The dashboard needs the ?api= parameter in host mode, because it is served
  from a different port than the backend. In containers nginx proxies both from
  one origin and the parameter is unnecessary.

  Ctrl-C to stop everything.

BANNER

wait

#!/usr/bin/env bash
# Run one HTTP benchmark server under wrk.
#
# Usage:
#   ./run.sh asyncio_std
#   ./run.sh tealetio_sync --proactor selector
#   PORT=9090 WRK_THREADS=2 WRK_CONNECTIONS=128 WRK_DURATION=10s ./run.sh tealetio_async
#
# Environment:
#   PORT            listen port (default 8080)
#   HOST            target host for wrk (default 127.0.0.1)
#   WRK_THREADS     wrk -t (default 4)
#   WRK_CONNECTIONS wrk -c (default 256)
#   WRK_DURATION    wrk -d (default 30s)
#   WRK_WARMUP      warmup duration before measured run (default 5s)
#   WRK_RUNS        repeated measured runs (default 3)
#   SERVER_ARGS              extra args passed to the server (e.g. --proactor uring)
#   TEALETIO_WAKEUP_MANAGER  event (default) or token for uring proactor

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${ROOT}/../../.." && pwd)"
SERVER="${1:?server name required (e.g. asyncio_std, tealetio_sync)}"
shift || true

PORT="${PORT:-8080}"
HOST="${HOST:-127.0.0.1}"
WRK_THREADS="${WRK_THREADS:-4}"
WRK_CONNECTIONS="${WRK_CONNECTIONS:-256}"
WRK_DURATION="${WRK_DURATION:-30s}"
WRK_WARMUP="${WRK_WARMUP:-5s}"
WRK_RUNS="${WRK_RUNS:-3}"
SERVER_ARGS="${SERVER_ARGS:-}"

SERVER_SCRIPT="${ROOT}/servers/${SERVER}.py"
if [[ ! -f "${SERVER_SCRIPT}" ]]; then
  echo "unknown server: ${SERVER} (expected ${SERVER_SCRIPT})" >&2
  exit 1
fi

if ! command -v wrk >/dev/null 2>&1; then
  echo "wrk is not installed (sudo apt install wrk)" >&2
  exit 1
fi

cd "${REPO_ROOT}"
env TEALETIO_WAKEUP_MANAGER="${TEALETIO_WAKEUP_MANAGER:-}" \
  uv run --active --package tealetio python "${SERVER_SCRIPT}" --port "${PORT}" ${SERVER_ARGS} "$@" &
SERVER_PID=$!

cleanup() {
  if kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

echo "waiting for ${HOST}:${PORT} (pid ${SERVER_PID})..."
ready=0
for _ in $(seq 1 100); do
  if curl -fsS -m 2 -o /dev/null "http://${HOST}:${PORT}/" 2>/dev/null; then
    ready=1
    break
  fi
  if ! kill -0 "${SERVER_PID}" 2>/dev/null; then
    echo "server exited before becoming ready" >&2
    wait "${SERVER_PID}" || true
    exit 1
  fi
  sleep 0.1
done
if [[ "${ready}" -eq 0 ]]; then
  echo "server did not become ready within 10s" >&2
  exit 1
fi

URL="http://${HOST}:${PORT}/"
echo "warmup: wrk -t${WRK_THREADS} -c${WRK_CONNECTIONS} -d${WRK_WARMUP} ${URL}"
wrk -t"${WRK_THREADS}" -c"${WRK_CONNECTIONS}" -d"${WRK_WARMUP}" "${URL}" >/dev/null

for run in $(seq 1 "${WRK_RUNS}"); do
  echo "=== run ${run}/${WRK_RUNS}: ${SERVER} ==="
  wrk -t"${WRK_THREADS}" -c"${WRK_CONNECTIONS}" -d"${WRK_DURATION}" --latency "${URL}"
done
#!/usr/bin/env bash
# Compare uring throughput with event vs token wakeup managers.
#
# Usage:
#   packages/tealetio/bench/compare_wrk_wakeup.sh
#   WRK_THREADS=4 WRK_CONNECTIONS=512 WRK_DURATION=15s packages/tealetio/bench/compare_wrk_wakeup.sh
#
# Environment (forwarded to each run.sh invocation):
#   PORT            base port (default 8080); token run uses PORT+1
#   HOST, WRK_THREADS, WRK_CONNECTIONS, WRK_DURATION, WRK_WARMUP, WRK_RUNS

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_PORT="${PORT:-8080}"
TOKEN_PORT=$((BASE_PORT + 1))

run_case() {
  local label="$1"
  local manager="$2"
  local port="$3"
  echo
  echo "################################################################"
  echo "# ${label}  TEALETIO_WAKEUP_MANAGER=${manager}  PORT=${port}"
  echo "################################################################"
  PORT="${port}" TEALETIO_WAKEUP_MANAGER="${manager}" SERVER_ARGS="--proactor uring" \
    "${ROOT}/run.sh" tealetio_sync
}

run_case "uring / event wakeup (default)" event "${BASE_PORT}"
run_case "uring / token handoff wakeup" token "${TOKEN_PORT}"
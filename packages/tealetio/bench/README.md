# tealetio HTTP benchmarks

Opt-in throughput/latency comparisons between a minimal asyncio HTTP server and
tealetio variants. Not part of default pytest CI.

## Prerequisites

```bash
sudo apt install wrk curl
```

From the workspace root, sync tealetio as usual (`uv sync --active --dev`).

## Servers

| Script | Description |
|--------|-------------|
| `servers/asyncio_std.py` | stdlib `asyncio` loop + `asyncio.start_server` (baseline) |
| `servers/tealetio_sync.py` | `SyncProactorScheduler` + `start_server` sync streams |
| `servers/tealetio_async.py` | `start_server(async_=True)` + async stream handlers |
| `servers/tealetio_asyncio_loop.py` | `TealetProactorEventLoop` hosting asyncio `start_server` |

Tealetio servers accept `--proactor default|selector|uring`.

Each server returns the same prebuilt `text/html` response from `common.py`.

## Quick start

```bash
# baseline
packages/tealetio/bench/run.sh asyncio_std

# tealetio native sync (default proactor: uring when available)
packages/tealetio/bench/run.sh tealetio_sync

# selector proactor
SERVER_ARGS="--proactor selector" packages/tealetio/bench/run.sh tealetio_sync

# asyncio app on TealetProactorEventLoop
packages/tealetio/bench/run.sh tealetio_asyncio_loop
```

Tune wrk via environment variables:

```bash
WRK_THREADS=4 WRK_CONNECTIONS=512 WRK_DURATION=30s WRK_RUNS=5 \
  packages/tealetio/bench/run.sh tealetio_sync
```

## Manual server + wrk

```bash
uv run --active --package tealetio python packages/tealetio/bench/servers/asyncio_std.py --port 8080

wrk -t4 -c256 -d30s --latency http://127.0.0.1:8080/
```

## Notes

- Run from a quiet machine; WSL2 numbers vary with host load.
- Compare using identical `WRK_*` settings and the same `PORT`.
- Tealetio servers run under `tealetio.run()` so the main tealet is a proper
  scheduler `Task` (required for uring IO waits).
- Phase 2 ideas: selector vs uring tables, async tealetio on
  `AsyncProactorScheduler`.
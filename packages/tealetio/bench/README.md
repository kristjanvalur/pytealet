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

Tealetio servers accept `--proactor default|selector|uring|uring-sync`.
`tealetio_sync` also accepts `--ring-entries N` (io_uring SQ depth; default 8)
and `--completion-threads N`.

Each server returns the same prebuilt `text/html` response from `common.py`.

## Quick start

```bash
# baseline
packages/tealetio/bench/run.sh asyncio_std

# tealetio native sync (default proactor: uring when available)
packages/tealetio/bench/run.sh tealetio_sync

# selector proactor
SERVER_ARGS="--proactor selector" packages/tealetio/bench/run.sh tealetio_sync

# uring with a server-sized ring (default is 8 SQ entries)
SERVER_ARGS="--proactor uring-sync --ring-entries 512" \
  packages/tealetio/bench/run.sh tealetio_sync

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

## Experiment knobs

`ProactorIOManager` reads these at construction (cached; default is eager send
+ uring `close_socket_nowait`):

| Variable | Values | Effect |
|----------|--------|--------|
| `TEALETIO_EAGER_IO` | `1` / `0` | master switch for the send first try |
| `TEALETIO_EAGER_SEND` | `1` / `0` | one non-blocking `send` before `proactor.send` |
| `TEALETIO_SOCK_CLOSE` | `nowait` / `stdlib` | uring nowait close vs `socket.close()` |

`TEALETIO_EAGER_SEND` inherits `TEALETIO_EAGER_IO` when unset (default on).
Accept and recv always go to the proactor.

Uring IO-manager eager-send on vs off (send volume and large; accept/recv once)::

    uv run --active --package tealetio python packages/tealetio/bench/micro_eager_compare.py

Uring send first-leg hint (`IoExpect.READY` vs `BLOCK`, no eager `send`)::

    uv run --active --package tealetio python packages/tealetio/bench/micro_send_expect.py
    uv run --active --package tealetio python -m pytest \
      packages/tealetio/tests/test_proactor.py::TestUringProactor::test_native_send_expect_ready_vs_block_timing -s

## Notes

- Run from a quiet machine; WSL2 numbers vary with host load.
- Compare using identical `WRK_*` settings and the same `PORT`.
- Tealetio servers run under `tealetio.run()` so the main tealet is a proper
  scheduler `Task` (required for uring IO waits).
- The bench response is `Connection: close`, so wrk measures accept/recv/send/close,
  not keep-alive request loops.
- Default `UringProactor` SQ depth is 8; use `--ring-entries 512` (or similar)
  when comparing batched submit.
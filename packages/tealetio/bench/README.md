# tealetio HTTP benchmarks

Opt-in throughput/latency comparisons between a minimal asyncio HTTP server and
tealetio variants. Not part of default pytest CI. A separate workflow reports
two pinned cases to Bencher.

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
`tealetio_sync` also accepts `--ring-entries N` (io_uring SQ depth; default 256),
`--ring-cq-entries N`, `--completion-threads N`, `--single-issuer`
(`IORING_SETUP_SINGLE_ISSUER`), and `--defer-taskrun`
(`IORING_SETUP_DEFER_TASKRUN`; implies single-issuer, inline wait only).
Default CQ is 1024, sized for wrk `-c256` keep-alive plus send_all CQEs.

Each server returns the same prebuilt `text/html` response from `common.py`.

## Quick start

```bash
# baseline
packages/tealetio/bench/run.sh asyncio_std

# tealetio native sync (default proactor: uring when available)
packages/tealetio/bench/run.sh tealetio_sync

# selector proactor
SERVER_ARGS="--proactor selector" packages/tealetio/bench/run.sh tealetio_sync

# explicit SQ 256 / CQ 1024 (the UringProactor default; send_all + recv-multishot)
SERVER_ARGS="--proactor uring-sync --ring-entries 256 --ring-cq-entries 1024" \
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

`ProactorIOManager` reads these at construction (cached; default is uring
`close_socket_nowait`):

| Variable | Values | Effect |
|----------|--------|--------|
| `TEALETIO_SOCK_CLOSE` | `nowait` / `stdlib` | uring nowait close vs `socket.close()` |

Send always goes to the proactor (`send_all` / `send_nowait`). There is no
manager-side stdlib `send`.

sendall+close vs `sock_send_close`::

    uv run --active --package tealetio python packages/tealetio/bench/micro_send_close.py --uring

Uring send first-leg hint (`IoExpect.READY` vs `BLOCK`)::

    uv run --active --package tealetio python packages/tealetio/bench/micro_send_expect.py
    uv run --active --package tealetio python -m pytest \
      packages/tealetio/tests/test_proactor.py::TestUringProactor::test_native_send_expect_ready_vs_block_timing -s

## CI

`.github/workflows/tealetio-bench.yml` runs on pull requests and on pushes to
`main` that touch tealet, tealetio, or uring-api. It uploads two medians to
the Bencher project `pytealet-tealetio` and Bencher comments on the pull
request. The comment shows the new requests/sec and p99, and the change
against earlier runs of the base branch on the same `ubuntu-24.04` runner.

Bencher draws that change only when a threshold exists. Each measure uses a
percentage test over at most the last 16 runs, and a single earlier run is
enough. Throughput alerts when requests/sec fall below half the recent mean.
p99 alerts when it more than doubles. A faster run, or a lower p99, does not
alert. Those bounds are deliberately wide while the shared runner's noise is
still unknown. The p99 is uploaded as Bencher's `latency` measure, in
nanoseconds. Bencher scales that measure and prints it as milliseconds. A
measure name Bencher does not know is labelled "units". An alert is a comment, not a failed job. A server that never
accepts, or a case with no usable wrk window, still fails the job.

| Benchmark | Interpreter | Server |
| --- | --- | --- |
| `http-sync-3.15` | CPython 3.15 | `--proactor uring-sync` |
| `http-threaded-3.15t` | CPython 3.15 free-threaded | `--proactor uring --completion-threads 2` |

Both servers use the proactor default ring (SQ depth 256). wrk uses 4 threads
and 256 connections. Each case discards a 5s warmup, then
keeps up to three 10s windows. A window with socket errors, a non-2xx
response, or a p99 of 1s or more is left out.

The first run creates an unclaimed project. Claim it from the link in that
job log, then store a Bencher API key as the `BENCHER_API_KEY` repository
secret. A claimed project rejects anonymous uploads, so the secret has to be
in place after the claim. History on `main` starts with the first push after
this workflow lands. A pull request copies that history as its start point,
which is the baseline the percentage test compares against.
Fork pull requests skip the job: the secret is not available there, and the
bench is for branches in this repository.

The runner is a shared GitHub machine. Read a single comment as a direction
against earlier runs on `ubuntu-24.04`, and use the window range in the job
summary before treating one sample as a shift.

## Notes

- Run from a quiet machine; WSL2 numbers vary with host load.
- Compare using identical `WRK_*` settings and the same `PORT`.
- Tealetio servers run under `tealetio.run()` so the main tealet is a proper
  scheduler `Task` (required for uring IO waits).
- The bench body is `Connection: close` plus `Content-Length`. wrk times from
  a finished TCP connect until that body has arrived. The handshake is outside
  the sample, and wrk does not wait for the server FIN, so a slow close is not
  this number. Time spent in the kernel accept queue is.
- Long wrk tails are a client artefact. A window whose p99 jumps to about
  1–2 s, or whose socket timeouts pile up near `-c` while the wall clock runs
  about one timeout past `-d`, is an outlier. Leave it out of the timing. A
  usable window keeps p99 in the tens of milliseconds and prints no socket
  errors.
- Default `UringProactor` SQ depth is 8; use `--ring-entries 512` (or similar)
  when comparing batched submit.

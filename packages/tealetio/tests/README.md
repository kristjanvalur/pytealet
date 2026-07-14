# tealetio test suite

This directory contains tests for the `tealetio` workspace package. Production
source under `src/tealetio/` is linted by Ruff; test code is excluded from
format/check passes but should still follow the same style when edited.

## Layout

### Shared helpers

- `conftest.py`: autouse scheduler TLS reset, parametrized task-factory fixtures,
  and the `requires_native_uring_recv_multishot` marker wiring.
- `helpers.py`: small scheduler construction helper for tests that need an
  explicit `set_scheduler()` call.
- `io_fakes.py`: shared scheduler stand-ins for direct `ProactorIOManager` unit
  tests.
- `uring_fakes.py`: shared io_uring ring fakes, capability patching helpers, and
  deferred/backpressure ring subclasses used by proactor and streams tests.

### Runtime and primitives

- `test_scheduler.py`: scheduler driving API, timers, spawn/wait, channels, DNS
  hooks, and socket-helper integration.
- `test_scheduler_crash_repros.py`: focused regression tests for rare scheduler
  crashes (kept separate for easy bisect/repro).
- `test_runtime_runner.py`: `Runner`, `AsyncRunner`, and `run()` lifecycle.
- `test_futures.py`: `Future`, linking, shields, and related task primitives.
- `test_locks.py`, `test_queues.py`, `test_channel.py`: synchronisation types.
- `test_public_api.py`: top-level `tealetio` export surface.

### IO and proactor

- `test_proactor.py`: proactor protocol, selector and io_uring backends,
  recv-iter internals, and proactor-backed schedulers. This is the largest file;
  it mirrors the size of `src/tealetio/proactor.py`.
- `test_io_manager.py`: `ProactorIOManager` and selector/proactor IO facades.
- `test_streams.py`: stream reader/writer/server helpers and IO-backend
  requirements.
- `test_asyncio_event_loop.py`: asyncio event-loop bridge smoke tests.
- `test_dns.py`: blocking DNS resolution via the proactor scheduler.

### Files

- `test_proactor_files.py`: `parse_open_mode()` flag mapping and `ProactorFile`
  handle behaviour (seek/read/write against a memory proactor fake).

## Markers

- `requires_native_uring_recv_multishot`: skipped automatically when native
  io_uring multishot receive is unavailable or does not settle on cancel.
  Defined and enforced in `conftest.py`.

## Running tests

From the workspace root with an active venv:

```bash
uv sync --active --locked --dev --package tealetio
uv run --active --package tealetio python -m pytest packages/tealetio/tests/ -v
```

Targeted suites:

```bash
uv run --active --package tealetio python -m pytest packages/tealetio/tests/test_proactor.py -v
uv run --active --package tealetio python -m pytest packages/tealetio/tests/test_scheduler.py -v
uv run --active --package tealetio python -m pytest packages/tealetio/tests/test_streams.py -v
```

Native io_uring multishot tests only run when the host supports them; skipped
tests report the reason from `conftest.py`.
# uring-asyncio

An asyncio event loop that submits socket IO through
[`uring-api`](../uring_api/README.md) instead of epoll.

Use this when you want stock `async def` code to run on io_uring without
hosting asyncio inside tealetio. The first slice is an IOCP-shaped proactor on
top of `asyncio.proactor_events.BaseProactorEventLoop`: it is meant to be
correct and small, not yet a uvloop competitor.

## Requirements

- Linux with a working `io_uring` (`uring_api.is_available()`)
- Python 3.10+
- `uring-api` (workspace member)

## Quick start

```python
import asyncio
from uring_asyncio import UringProactorEventLoop, run


async def main() -> None:
    reader, writer = await asyncio.open_connection("example.com", 80)
    writer.close()
    await writer.wait_closed()


# Python 3.12+
asyncio.run(main(), loop_factory=UringProactorEventLoop)

# any supported Python
run(main())
```

If io_uring cannot be created, constructing the loop raises
`UringUnavailableError` rather than falling back to a selector loop.

## What works in this slice

- `loop.sock_recv` / `sock_recv_into` / `sock_sendall` / `sock_connect` / `sock_accept`
- `asyncio.start_server` / `open_connection` (including SSL via CPython `sslproto`)
- UDP `sock_recvfrom` / `sock_sendto`
- `call_soon_threadsafe` via the stdlib proactor self-pipe

Sends use `uring-api` `send_all` so `sock_sendall` still means “the whole
buffer”, matching asyncio’s single-Future send contract.

## Not yet

- `add_reader` / `add_writer` (same gap as Windows `ProactorEventLoop`)
- subprocesses
- native sendfile (asyncio’s copy fallback still runs)
- multishot accept/recv and provided-buffer transports

Those belong in a later custom loop, not in this adapter. The intended
design, invariants, and `uring-api` boundary are in [ROADMAP.md](ROADMAP.md).

## Tests

```bash
uv sync --active --locked --dev --package uring-asyncio
timeout 30 uv run --active --package uring-asyncio python -m pytest packages/uring_asyncio/tests/ -v
```

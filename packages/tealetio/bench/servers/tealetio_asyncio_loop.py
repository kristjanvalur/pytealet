#!/usr/bin/env python3
"""Minimal asyncio HTTP server hosted on TealetProactorEventLoop."""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path
from typing import Callable

_BENCH_DIR = Path(__file__).resolve().parent.parent
if str(_BENCH_DIR) not in sys.path:
    sys.path.insert(0, str(_BENCH_DIR))

from common import RESPONSE, add_tealetio_args, drain_request_async  # noqa: E402

from tealetio import run
from tealetio.asyncio import TealetProactorEventLoop
from tealetio.proactor import SelectorProactor, SyncProactorScheduler, UringProactor


def _scheduler_factory(name: str) -> Callable[[], SyncProactorScheduler]:
    if name == "selector":
        return lambda: SyncProactorScheduler(SelectorProactor)
    if name == "uring":
        return lambda: SyncProactorScheduler(lambda: UringProactor())
    return SyncProactorScheduler


async def _handle_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    await drain_request_async(reader)
    writer.write(RESPONSE)
    await writer.drain()
    writer.close()


async def _serve_async(host: str, port: int, backlog: int) -> None:
    server = await asyncio.start_server(_handle_client, host, port, backlog=backlog)
    async with server:
        await server.serve_forever()


def _run_asyncio_loop(host: str, port: int, backlog: int) -> None:
    loop = TealetProactorEventLoop()
    try:
        asyncio.set_event_loop(loop)
        loop.run_until_complete(_serve_async(host, port, backlog))
    finally:
        asyncio.set_event_loop(None)
        loop.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    add_tealetio_args(parser)
    args = parser.parse_args()

    factory = _scheduler_factory(args.proactor)

    def exercise() -> None:
        _run_asyncio_loop(args.host, args.port, args.backlog)

    try:
        run(exercise, scheduler_factory=factory)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

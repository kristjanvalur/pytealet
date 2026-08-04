#!/usr/bin/env python3
"""Minimal HTTP server using tealetio async streams (run_coro) and SyncProactorScheduler."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Callable

_BENCH_DIR = Path(__file__).resolve().parent.parent
if str(_BENCH_DIR) not in sys.path:
    sys.path.insert(0, str(_BENCH_DIR))

from common import RESPONSE, add_tealetio_args, drain_request_async  # noqa: E402

from tealetio import run
from tealetio.proactor import SelectorProactor, SyncProactorScheduler, UringProactor
from tealetio.scheduler import _current_scheduler
from tealetio.streams import AsyncStreamReader, AsyncStreamWriter, start_server


def _scheduler_factory(name: str) -> Callable[[], SyncProactorScheduler]:
    if name == "selector":
        return lambda: SyncProactorScheduler(SelectorProactor)
    if name == "uring":
        return lambda: SyncProactorScheduler(lambda: UringProactor())
    return SyncProactorScheduler


async def _client_handler(reader: AsyncStreamReader, writer: AsyncStreamWriter) -> None:
    await drain_request_async(reader)
    writer.write(RESPONSE)
    await writer.drain()
    writer.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    add_tealetio_args(parser)
    args = parser.parse_args()

    factory = _scheduler_factory(args.proactor)

    def exercise() -> None:
        scheduler = _current_scheduler()
        if scheduler is None:
            raise RuntimeError("bench server requires an active scheduler")
        server = start_server(
            _client_handler,
            addr=(args.host, args.port),
            backlog=args.backlog,
            reuse_address=args.reuse_address,
            reuse_port=args.reuse_port,
            async_=True,
            scheduler=scheduler,
        )
        server.serve_forever()

    try:
        run(exercise, scheduler_factory=factory)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

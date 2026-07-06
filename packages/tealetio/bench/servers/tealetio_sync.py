#!/usr/bin/env python3
"""Minimal HTTP server using tealetio sync streams and SyncProactorScheduler."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Callable

_BENCH_DIR = Path(__file__).resolve().parent.parent
if str(_BENCH_DIR) not in sys.path:
    sys.path.insert(0, str(_BENCH_DIR))

from common import RESPONSE, add_tealetio_args, drain_request  # noqa: E402
from diag import start_watchdog  # noqa: E402

from tealetio import run
from tealetio.proactor import SelectorProactor, SyncProactorScheduler, UringProactor
from tealetio.scheduler import _current_scheduler
from tealetio.stream_diag import enabled as diag_enabled, event as diag_event
from tealetio.streams import StreamReader, StreamWriter, start_server


def _scheduler_factory(name: str) -> Callable[[], SyncProactorScheduler]:
    if name == "selector":
        return lambda: SyncProactorScheduler(SelectorProactor)
    if name == "uring":
        return lambda: SyncProactorScheduler(lambda: UringProactor())
    return SyncProactorScheduler


def _client_handler(reader: StreamReader, writer: StreamWriter) -> None:
    fd = writer.get_extra_info("socket").fileno()
    if diag_enabled():
        diag_event("bench_drain_begin", fd=fd)
    drain_request(reader)
    if diag_enabled():
        diag_event("bench_write_begin", fd=fd)
    writer.write(RESPONSE)
    writer.drain()
    if diag_enabled():
        diag_event("bench_close_begin", fd=fd)
    writer.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    add_tealetio_args(parser)
    parser.add_argument(
        "--diag",
        action="store_true",
        help="enable TEALETIO_STREAM_DIAG and a periodic stall watchdog",
    )
    args = parser.parse_args()
    if args.diag:
        import os

        os.environ["TEALETIO_STREAM_DIAG"] = "1"
        os.environ["TEALETIO_URING_ACCEPT_LOG"] = "1"

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
            scheduler=scheduler,
        )
        if args.diag:
            start_watchdog(scheduler, server)
            diag_event("bench_listen", host=args.host, port=args.port, proactor=args.proactor)
        server.serve_forever()

    try:
        run(exercise, scheduler_factory=factory)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

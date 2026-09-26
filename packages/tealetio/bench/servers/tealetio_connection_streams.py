#!/usr/bin/env python3
"""HTTP server using streams built on the connection server.

Same handler as ``tealetio_sync``, but accept goes through
``start_connection_stream_server``: the delivery thread posts the oneshot
recv and calls ``open_streams``. ``recv_many`` is armed only if a later read
needs another chunk.
"""

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
from tealetio.connections import start_connection_stream_server
from tealetio.proactor import (
    SelectorProactor,
    SyncProactorScheduler,
    SyncUringProactor,
    UringProactor,
)
from tealetio.scheduler import _current_scheduler
from tealetio.stream_diag import enabled as diag_enabled, event as diag_event
from tealetio.streams import StreamReader, StreamWriter


def _scheduler_factory(
    name: str,
    *,
    completion_threads: int | None = None,
) -> Callable[[], SyncProactorScheduler]:
    if name == "selector":
        return lambda: SyncProactorScheduler(SelectorProactor)
    if name in ("uring", "uring-sync"):
        if completion_threads is not None:
            threads = completion_threads
        elif name == "uring-sync":
            threads = 0
        else:
            threads = 2

        def factory() -> SyncProactorScheduler:
            if threads == 0:
                return SyncProactorScheduler(SyncUringProactor)
            return SyncProactorScheduler(lambda: UringProactor(completion_threads=threads))

        return factory
    return SyncProactorScheduler


def _make_client_handler() -> Callable[[StreamReader, StreamWriter], None]:
    def client_handler(reader: StreamReader, writer: StreamWriter) -> None:
        sock = writer.get_extra_info("socket")
        fd = sock.fileno()
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

    return client_handler


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    add_tealetio_args(parser)
    parser.add_argument(
        "--diag",
        action="store_true",
        help="enable TEALETIO_STREAM_DIAG and a periodic stall watchdog",
    )
    parser.add_argument(
        "--completion-threads",
        type=int,
        default=None,
        metavar="N",
        help="UringProactor completion workers (0 = inline ring.wait; default 2 for --proactor uring)",
    )
    args = parser.parse_args()
    if args.diag:
        import os

        os.environ["TEALETIO_STREAM_DIAG"] = "1"
        os.environ["TEALETIO_URING_ACCEPT_LOG"] = "1"
    if args.completion_threads is not None and args.completion_threads < 0:
        parser.error("--completion-threads must be non-negative")
    if args.completion_threads is not None and args.proactor == "selector":
        parser.error("--completion-threads only applies to uring proactors")

    factory = _scheduler_factory(args.proactor, completion_threads=args.completion_threads)

    def exercise() -> None:
        scheduler = _current_scheduler()
        if scheduler is None:
            raise RuntimeError("bench server requires an active scheduler")
        server = start_connection_stream_server(
            _make_client_handler(),
            addr=(args.host, args.port),
            backlog=args.backlog,
            reuse_address=args.reuse_address,
            reuse_port=args.reuse_port,
            scheduler=scheduler,
        )
        if args.diag:
            start_watchdog(scheduler, server)  # type: ignore[arg-type]
            diag_event("bench_listen", host=args.host, port=args.port, proactor=args.proactor)
        server.serve_forever()

    try:
        run(exercise, scheduler_factory=factory)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Minimal HTTP server using tealetio Connection (no streams, no handler tealet)."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable
from pathlib import Path

_BENCH_DIR = Path(__file__).resolve().parent.parent
if str(_BENCH_DIR) not in sys.path:
    sys.path.insert(0, str(_BENCH_DIR))

from common import RESPONSE, add_tealetio_args, http_headers_complete  # noqa: E402

from tealetio import run
from tealetio.connections import Connection, start_connection_server
from tealetio.proactor import (
    SelectorProactor,
    SyncProactorScheduler,
    SyncUringProactor,
    UringProactor,
)
from tealetio.scheduler import _current_scheduler


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


def _on_conn(conn: Connection) -> None:
    def on_data(_conn: Connection, data: memoryview | None, exc: BaseException | None) -> None:
        if exc is not None or data is None or not http_headers_complete(data):
            _conn.close()
            return
        _conn.send_close_nowait(RESPONSE)

    conn.set_recv_callback(on_data)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    add_tealetio_args(parser)
    parser.add_argument(
        "--completion-threads",
        type=int,
        default=None,
        metavar="N",
        help="UringProactor completion workers (0 = inline ring.wait; default 2 for --proactor uring)",
    )
    args = parser.parse_args()
    if args.completion_threads is not None and args.completion_threads < 0:
        parser.error("--completion-threads must be non-negative")
    if args.completion_threads is not None and args.proactor == "selector":
        parser.error("--completion-threads only applies to uring proactors")

    factory = _scheduler_factory(args.proactor, completion_threads=args.completion_threads)

    def exercise() -> None:
        scheduler = _current_scheduler()
        if scheduler is None:
            raise RuntimeError("bench server requires an active scheduler")
        server = start_connection_server(
            _on_conn,
            addr=(args.host, args.port),
            backlog=args.backlog,
            reuse_address=args.reuse_address,
            reuse_port=args.reuse_port,
            scheduler=scheduler,
        )
        server.serve_forever()

    try:
        run(exercise, scheduler_factory=factory)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

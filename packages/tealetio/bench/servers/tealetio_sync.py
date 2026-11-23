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

from tealetio import run
from tealetio.proactor import (
    SelectorProactor,
    SyncProactorScheduler,
    SyncUringProactor,
    UringProactor,
)
from tealetio.scheduler import _current_scheduler
from tealetio.streams import StreamReader, StreamWriter, start_server


def _scheduler_factory(
    name: str,
    *,
    completion_threads: int | None = None,
    sqpoll: bool = False,
) -> Callable[[], SyncProactorScheduler]:
    if name == "selector":
        return lambda: SyncProactorScheduler(SelectorProactor)
    if name in ("uring", "uring-sync"):
        # uring-sync defaults to 0 workers; uring defaults to UringProactor's 2
        if completion_threads is not None:
            threads = completion_threads
        elif name == "uring-sync":
            threads = 0
        else:
            threads = 2

        def factory() -> SyncProactorScheduler:
            import uring_api

            flags = uring_api.IORING_SETUP_SQPOLL if sqpoll else 0
            if threads == 0:
                return SyncProactorScheduler(lambda: SyncUringProactor(flags=flags))
            return SyncProactorScheduler(lambda: UringProactor(completion_threads=threads, flags=flags))

        return factory
    return SyncProactorScheduler


def _make_client_handler() -> Callable[[StreamReader, StreamWriter], None]:
    """Serve one connection on a handler tealet (spawned after streams are ready)."""

    def client_handler(reader: StreamReader, writer: StreamWriter) -> None:
        drain_request(reader)
        writer.write(RESPONSE)
        writer.drain()
        writer.close()

    return client_handler


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
    parser.add_argument(
        "--sqpoll",
        action="store_true",
        help="IORING_SETUP_SQPOLL: kernel thread issues; userspace still publishes at the usual points",
    )
    args = parser.parse_args()
    if args.completion_threads is not None and args.completion_threads < 0:
        parser.error("--completion-threads must be non-negative")
    if args.completion_threads is not None and args.proactor == "selector":
        parser.error("--completion-threads only applies to uring proactors")
    if args.sqpoll and args.proactor not in ("uring", "uring-sync"):
        parser.error("--sqpoll requires --proactor uring or uring-sync")

    factory = _scheduler_factory(
        args.proactor,
        completion_threads=args.completion_threads,
        sqpoll=args.sqpoll,
    )

    def exercise() -> None:
        scheduler = _current_scheduler()
        if scheduler is None:
            raise RuntimeError("bench server requires an active scheduler")
        server = start_server(
            _make_client_handler(),
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

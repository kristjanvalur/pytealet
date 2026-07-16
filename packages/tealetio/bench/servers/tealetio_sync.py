#!/usr/bin/env python3
"""Minimal HTTP server using tealetio sync streams and SyncProactorScheduler."""

from __future__ import annotations

import argparse
import socket
import sys
from pathlib import Path
from typing import Callable

_BENCH_DIR = Path(__file__).resolve().parent.parent
if str(_BENCH_DIR) not in sys.path:
    sys.path.insert(0, str(_BENCH_DIR))

from common import RESPONSE, add_tealetio_args, drain_request  # noqa: E402
from diag import start_watchdog  # noqa: E402

_profile_seq = 0

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


def _next_req_num(profile: bool) -> int | None:
    global _profile_seq
    if not profile:
        return None
    _profile_seq += 1
    if _profile_seq == 1:
        return None
    return _profile_seq - 1


def _make_profile_stream_factory() -> Callable[..., tuple[StreamReader, StreamWriter]]:
    from profile_timing import stamp_stream_open
    from tealetio.streams.open import default_server_stream_factory

    base = default_server_stream_factory(async_=False)

    def factory(io: object, sock: socket.socket, *, limit: int = 2**16) -> tuple[StreamReader, StreamWriter]:
        stamp_stream_open(sock)
        return base(io, sock, limit=limit)  # type: ignore[arg-type]

    return factory


def _make_client_handler(backend: str, profile: bool) -> Callable[[StreamReader, StreamWriter], None]:
    def client_handler(reader: StreamReader, writer: StreamWriter) -> None:
        sock = writer.get_extra_info("socket")
        fd = sock.fileno()
        req_num = _next_req_num(profile)
        timer = None
        if req_num is not None:
            from profile_timing import PhaseTimer, drain_request_profile, pre_handler_ms

            timer = PhaseTimer(f"tealetio-{backend}", req_num)
            opened = pre_handler_ms(sock)
            extra = {"fd": fd}
            if opened is not None:
                extra["pre_handler_ms"] = opened
            timer.mark("handler_start", **extra)
        if timer is not None:
            drain_request_profile(reader, timer)
            timer.mark("drain")
            writer.write(RESPONSE)
            timer.mark("write")
            writer.drain()
            timer.mark("drain_out")
            writer.close()
            timer.mark("close")
            timer.finish()
            return
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
    parser.add_argument("--profile", action="store_true", help="emit per-request PROFILE lines to stderr")
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
            _make_client_handler(args.proactor, args.profile),
            addr=(args.host, args.port),
            backlog=args.backlog,
            reuse_address=args.reuse_address,
            reuse_port=args.reuse_port,
            stream_factory=_make_profile_stream_factory() if args.profile else None,
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

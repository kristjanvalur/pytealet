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

from tealetio import run
from tealetio.proactor import (
    SelectorProactor,
    SyncProactorScheduler,
    SyncUringProactor,
    UringProactor,
)
from tealetio.scheduler import _current_scheduler
from tealetio.stream_diag import enabled as diag_enabled, event as diag_event
from tealetio.streams import StreamReader, StreamWriter, start_server


def _uring_setup_flags(*, single_issuer: bool, defer_taskrun: bool) -> int:
    import uring_api

    flags = 0
    if single_issuer or defer_taskrun:
        flags |= uring_api.IORING_SETUP_SINGLE_ISSUER
    if defer_taskrun:
        flags |= uring_api.IORING_SETUP_DEFER_TASKRUN
    return flags


def _scheduler_factory(
    name: str,
    *,
    completion_threads: int | None = None,
    ring_entries: int = 8,
    ring_flags: int = 0,
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
            if threads == 0:
                return SyncProactorScheduler(lambda: SyncUringProactor(entries=ring_entries, flags=ring_flags))
            return SyncProactorScheduler(lambda: UringProactor(ring_entries, ring_flags, completion_threads=threads))

        return factory
    return SyncProactorScheduler


def _make_profile_stream_factory() -> Callable[..., tuple[StreamReader, StreamWriter]]:
    from profile_timing import stamp_stream_open
    from tealetio.streams.open import default_server_stream_factory

    base = default_server_stream_factory(async_=False)

    def factory(io: object, sock: socket.socket, *, limit: int = 2**16) -> tuple[StreamReader, StreamWriter]:
        stamp_stream_open(sock)
        return base(io, sock, limit=limit)  # type: ignore[arg-type]

    return factory


def _make_client_handler(backend: str, profile: bool) -> Callable[[StreamReader, StreamWriter], None]:
    """Serve one connection on a handler tealet (spawned after streams are ready)."""

    gate = None
    if profile:
        from profile_timing import HandlerProfileGate

        gate = HandlerProfileGate()

    def client_handler(reader: StreamReader, writer: StreamWriter) -> None:
        sock = writer.get_extra_info("socket")
        fd = sock.fileno()
        req_num = gate.next_req_num() if gate is not None else None
        timer = None
        if req_num is not None:
            from profile_timing import PhaseTimer, drain_request_profile, pre_handler_ms

            timer = PhaseTimer(f"tealetio-{backend}", req_num)
            opened = pre_handler_ms(sock)
            extra = {"fd": fd}
            if opened is not None:
                extra["pre_handler_ms"] = opened
            # handler tealet is running; pre_handler_ms is spawn lag before this line
            timer.mark("handler_start", **extra)
        if timer is not None:
            # body: read headers, write response, wait for send + socket teardown
            drain_request_profile(reader, timer)
            timer.mark("drain")
            writer.write(RESPONSE)
            timer.mark("write")
            writer.drain()
            timer.mark("drain_out")
            writer.close()
            timer.mark("close")
            # Split shutdown: flush (parks) vs sock_close (forget, should be ~0).
            # StreamServer also wait_closed() in finally (cheap once closed).
            writer.flush()
            timer.mark("flush")
            writer.wait_closed()
            timer.mark("sock_close")
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
    parser.add_argument(
        "--completion-threads",
        type=int,
        default=None,
        metavar="N",
        help="UringProactor completion workers (0 = inline ring.wait; default 2 for --proactor uring)",
    )
    parser.add_argument(
        "--ring-entries",
        type=int,
        default=8,
        metavar="N",
        help="io_uring SQ depth (default 8, matching UringProactor)",
    )
    parser.add_argument(
        "--single-issuer",
        action="store_true",
        help="IORING_SETUP_SINGLE_ISSUER (creator thread owns io_uring_enter)",
    )
    parser.add_argument(
        "--defer-taskrun",
        action="store_true",
        help="IORING_SETUP_DEFER_TASKRUN (implies --single-issuer; inline wait only)",
    )
    args = parser.parse_args()
    if args.diag:
        import os

        os.environ["TEALETIO_STREAM_DIAG"] = "1"
        os.environ["TEALETIO_URING_ACCEPT_LOG"] = "1"
    if args.completion_threads is not None and args.completion_threads < 0:
        parser.error("--completion-threads must be non-negative")
    if args.proactor == "selector" and (
        args.completion_threads is not None or args.single_issuer or args.defer_taskrun
    ):
        parser.error("uring ring options only apply to uring proactors")
    if args.ring_entries < 1:
        parser.error("--ring-entries must be >= 1")
    threads = args.completion_threads
    if threads is None:
        threads = 0 if args.proactor == "uring-sync" else 2
    if args.defer_taskrun and threads != 0:
        parser.error("--defer-taskrun requires inline wait (--proactor uring-sync or --completion-threads 0)")
    ring_flags = 0
    if args.proactor != "selector":
        ring_flags = _uring_setup_flags(single_issuer=args.single_issuer, defer_taskrun=args.defer_taskrun)
    print(
        f"bench: proactor={args.proactor} entries={args.ring_entries} flags=0x{ring_flags:x} "
        f"completion_threads={threads if args.proactor != 'selector' else '-'}",
        file=sys.stderr,
        flush=True,
    )

    factory = _scheduler_factory(
        args.proactor,
        completion_threads=args.completion_threads,
        ring_entries=args.ring_entries,
        ring_flags=ring_flags,
    )

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
    finally:
        if args.profile:
            from profile_timing import _HandlerAggregate

            _HandlerAggregate.dump(final=True)


if __name__ == "__main__":
    main()

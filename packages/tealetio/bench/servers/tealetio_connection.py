#!/usr/bin/env python3
"""Minimal HTTP server using tealetio Connection (no streams, no handler tealet)."""

from __future__ import annotations

import argparse
import sys
import threading
import time
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


class _PathTiming:
    """Accumulate per-connection worker vs marshal delays (nanoseconds)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.n = 0
        self.start_ns = 0
        self.accept_marshal_ns = 0
        self.recv_marshal_ns = 0
        self.on_data_ns = 0
        self.recv_cross_thread = 0
        self.poll_n = 0
        self.poll_ns = 0
        self.wait_n = 0
        self.wait_ns = 0
        self.drain_n = 0
        self.drain_ns = 0
        self.cb_n = 0

    def add(self, *, start: int, accept_marshal: int, recv_marshal: int, on_data: int, recv_cross: bool) -> None:
        with self._lock:
            self.n += 1
            self.start_ns += start
            self.accept_marshal_ns += accept_marshal
            self.recv_marshal_ns += recv_marshal
            self.on_data_ns += on_data
            self.recv_cross_thread += int(recv_cross)

    def snapshot(self) -> str:
        with self._lock:
            n = self.n
            if n == 0:
                return "timing n=0"
            loop = ""
            if self.poll_n or self.wait_n or self.drain_n:
                loop = (
                    f" poll={self.poll_n}/{self.poll_ns / 1e6:.1f}ms"
                    f" wait={self.wait_n}/{self.wait_ns / 1e6:.1f}ms"
                    f" drain={self.drain_n}/{self.drain_ns / 1e6:.1f}ms"
                    f" cbs={self.cb_n}"
                )
            return (
                f"timing n={n} start={self.start_ns / n / 1000:.1f}us "
                f"accept_marshal={self.accept_marshal_ns / n / 1000:.1f}us "
                f"recv_marshal={self.recv_marshal_ns / n / 1000:.1f}us "
                f"on_data={self.on_data_ns / n / 1000:.1f}us "
                f"recv_cross_thread={self.recv_cross_thread}/{n}"
                f"{loop}"
            )


def _install_timing(timing: _PathTiming) -> None:
    orig_start = Connection.start
    orig_on_recv = Connection._on_recv_raw

    @classmethod
    def start(cls, io, sock, *, pool):  # noqa: ANN001
        t0 = time.perf_counter_ns()
        conn = orig_start(io, sock, pool=pool)
        now = time.perf_counter_ns()
        conn._t_start_ns = now - t0  # type: ignore[attr-defined]
        conn._t_worker_done_ns = now  # type: ignore[attr-defined]
        return conn

    def on_recv_raw(self, nbytes, exception):  # noqa: ANN001
        n = 0 if nbytes is None else nbytes
        t_cqe = time.perf_counter_ns()
        owner = getattr(self._io._scheduler, "_owner_thread", None)
        cross = owner is not None and threading.get_ident() != owner

        def deliver() -> None:
            self._t_recv_marshal_ns = time.perf_counter_ns() - t_cqe  # type: ignore[attr-defined]
            self._t_recv_cross = cross  # type: ignore[attr-defined]
            self._complete_recv(n, exception)

        scheduler = self._io._scheduler
        if scheduler is None:
            deliver()
            return
        scheduler.call_soon_threadsafe(deliver, immediate=True)

    Connection.start = start  # type: ignore[method-assign]
    Connection._on_recv_raw = on_recv_raw  # type: ignore[method-assign]


def _install_loop_timing(scheduler: object, timing: _PathTiming) -> None:
    orig_poll = scheduler._poll_io  # type: ignore[attr-defined]
    orig_wait = scheduler._wait_thread  # type: ignore[attr-defined]
    orig_drain = scheduler._drain_ready_callbacks  # type: ignore[attr-defined]

    def poll() -> None:
        t0 = time.perf_counter_ns()
        orig_poll()
        with timing._lock:
            timing.poll_n += 1
            timing.poll_ns += time.perf_counter_ns() - t0

    def wait() -> None:
        t0 = time.perf_counter_ns()
        orig_wait()
        with timing._lock:
            timing.wait_n += 1
            timing.wait_ns += time.perf_counter_ns() - t0

    def drain() -> None:
        t0 = time.perf_counter_ns()
        q = len(scheduler._ready_callbacks)  # type: ignore[attr-defined]
        orig_drain()
        with timing._lock:
            timing.drain_n += 1
            timing.drain_ns += time.perf_counter_ns() - t0
            timing.cb_n += q

    scheduler._poll_io = poll  # type: ignore[attr-defined, method-assign]
    scheduler._wait_thread = wait  # type: ignore[attr-defined, method-assign]
    scheduler._drain_ready_callbacks = drain  # type: ignore[attr-defined, method-assign]


def _on_conn(conn: Connection, timing: _PathTiming | None = None) -> None:
    t_on_conn = time.perf_counter_ns() if timing is not None else 0
    accept_marshal = 0
    start_ns = 0
    if timing is not None:
        worker_done = getattr(conn, "_t_worker_done_ns", None)
        start_ns = int(getattr(conn, "_t_start_ns", 0))
        if worker_done is not None:
            accept_marshal = t_on_conn - worker_done

    def on_data(_conn: Connection, data: memoryview | None, exc: BaseException | None) -> None:
        if timing is not None:
            on_data_ns = time.perf_counter_ns() - t_on_conn
            recv_marshal = int(getattr(_conn, "_t_recv_marshal_ns", 0))
            recv_cross = bool(getattr(_conn, "_t_recv_cross", False))
            timing.add(
                start=start_ns,
                accept_marshal=accept_marshal,
                recv_marshal=recv_marshal,
                on_data=on_data_ns,
                recv_cross=recv_cross,
            )
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
    parser.add_argument(
        "--timing",
        action="store_true",
        help="print per-connection worker/marshal averages to stderr",
    )
    parser.add_argument(
        "--busy-poll",
        action="store_true",
        help="never block in wait_idle; always poll (busy-spin the driver)",
    )
    args = parser.parse_args()
    if args.completion_threads is not None and args.completion_threads < 0:
        parser.error("--completion-threads must be non-negative")
    if args.completion_threads is not None and args.proactor == "selector":
        parser.error("--completion-threads only applies to uring proactors")

    factory = _scheduler_factory(args.proactor, completion_threads=args.completion_threads)
    timing = _PathTiming() if args.timing else None
    if timing is not None:
        _install_timing(timing)

    def exercise() -> None:
        scheduler = _current_scheduler()
        if scheduler is None:
            raise RuntimeError("bench server requires an active scheduler")

        def on_conn(conn: Connection) -> None:
            _on_conn(conn, timing)

        if args.busy_poll:

            async def busy_idle() -> None:
                scheduler._poll_io()

            scheduler._idle_or_poll = busy_idle  # type: ignore[method-assign]

        if timing is not None:
            _install_loop_timing(scheduler, timing)

            def snapshot() -> None:
                print(timing.snapshot(), file=sys.stderr, flush=True)
                scheduler.call_later(2.0, snapshot)

            scheduler.call_later(2.0, snapshot)

        server = start_connection_server(
            on_conn,
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

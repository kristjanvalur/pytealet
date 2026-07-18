#!/usr/bin/env python3
"""Microbenchmark: local TCP connect after socket creation.

Optional experiment for the eager-create direction: time

1. **direct create + uring connect** — ``socket()`` (scheduler contract) then
   ``ProactorIOManager.sock_connect`` / ``proactor.connect`` only on the ring
2. **chained uring create+connect** — today's ``sock_create(..., connect_to=)``
   which attaches ``create_socket`` then ``connect`` on an ``IOWaitGroup``

Both paths use ``SyncUringProactor`` (inline reaper) so completion cost is on
the driver thread. A background listener accepts and closes each connection.

Usage::

    uv run --active --package tealetio python packages/tealetio/bench/micro_socket_connect.py
    uv run --active --package tealetio python packages/tealetio/bench/micro_socket_connect.py -n 5000
"""

from __future__ import annotations

import argparse
import socket
import statistics
import threading
import time
from collections.abc import Callable

import uring_api
from tealetio.proactor import SyncProactorScheduler, SyncUringProactor
from tealetio.scheduler import set_scheduler
from tealetio.socket_helpers import configure_scheduler_socket

_SOCK_NONBLOCK = getattr(socket, "SOCK_NONBLOCK", 0)
_SOCK_CLOEXEC = getattr(socket, "SOCK_CLOEXEC", 0)


def _ns() -> int:
    return time.perf_counter_ns()


def _percentile(sorted_ns: list[int], p: float) -> float:
    if not sorted_ns:
        return float("nan")
    if len(sorted_ns) == 1:
        return sorted_ns[0] / 1000.0
    rank = (len(sorted_ns) - 1) * p
    lo = int(rank)
    hi = min(lo + 1, len(sorted_ns) - 1)
    frac = rank - lo
    return (sorted_ns[lo] * (1.0 - frac) + sorted_ns[hi] * frac) / 1000.0


def _summarise(name: str, samples_ns: list[int]) -> None:
    ordered = sorted(samples_ns)
    mean_us = statistics.fmean(samples_ns) / 1000.0
    med_us = statistics.median(samples_ns) / 1000.0
    p90_us = _percentile(ordered, 0.90)
    p99_us = _percentile(ordered, 0.99)
    print(
        f"{name:44s}  n={len(samples_ns):5d}  "
        f"mean={mean_us:8.2f} us  med={med_us:8.2f} us  "
        f"p90={p90_us:8.2f} us  p99={p99_us:8.2f} us"
    )


def _make_listener() -> tuple[socket.socket, tuple[str, int]]:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1024)
    listener.settimeout(0.05)
    return listener, listener.getsockname()


def _accept_loop(listener: socket.socket, stop: threading.Event) -> None:
    # drain as hard as possible so client connect latency is not accept-starved
    while not stop.is_set():
        try:
            while not stop.is_set():
                conn, _addr = listener.accept()
                try:
                    conn.close()
                except OSError:
                    pass
        except TimeoutError:
            continue
        except OSError:
            if stop.is_set():
                return
            continue


def _direct_socket() -> socket.socket:
    """Scheduler-contract socket without going through the proactor."""

    if _SOCK_NONBLOCK and _SOCK_CLOEXEC:
        sock = socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM | _SOCK_NONBLOCK | _SOCK_CLOEXEC,
        )
        # NONBLOCK at create already sets sock_timeout=0; setblocking is redundant
        # for state, but keep configure for cloexec portability on odd platforms.
        return sock
    return configure_scheduler_socket(socket.socket(socket.AF_INET, socket.SOCK_STREAM))


def _bench_io_paths(
    address: tuple[str, int],
    iterations: int,
    warmup: int,
    entries: int,
) -> None:
    samples_direct: list[int] = []
    samples_chained: list[int] = []

    scheduler = SyncProactorScheduler(lambda: SyncUringProactor(entries=entries))
    set_scheduler(scheduler)
    io = scheduler.io
    total = warmup + iterations

    def run_direct() -> None:
        for i in range(total):
            t0 = _ns()
            sock = _direct_socket()
            try:
                io.sock_connect(sock, address).wait()
            finally:
                try:
                    sock.close()
                except OSError:
                    pass
            t1 = _ns()
            if i >= warmup:
                samples_direct.append(t1 - t0)

    def run_chained() -> None:
        for i in range(total):
            t0 = _ns()
            sock = io.sock_create(
                socket.AF_INET,
                socket.SOCK_STREAM,
                connect_to=address,
            ).wait()
            try:
                sock.close()
            except OSError:
                pass
            t1 = _ns()
            if i >= warmup:
                samples_chained.append(t1 - t0)

    # wait() requires the scheduler task context (CrossThreadEvent / current)
    scheduler.run_until_complete(run_direct)
    _summarise("direct socket() + sock_connect (uring)", samples_direct)

    scheduler.run_until_complete(run_chained)
    _summarise("sock_create(connect_to=) chained uring", samples_chained)

    scheduler.close()


def _bench_proactor_only(
    address: tuple[str, int],
    iterations: int,
    warmup: int,
    entries: int,
) -> None:
    """Lower-level: no IOWaiter/scheduler, only proactor Operations + wait."""

    proactor = SyncUringProactor(entries=entries)
    samples_direct: list[int] = []
    samples_chained: list[int] = []
    total = warmup + iterations

    def wait_op(op: object) -> None:
        deadline = proactor.get_time() + 5.0
        while not op.done():  # type: ignore[attr-defined]
            proactor.wait(deadline)
            if proactor.get_time() >= deadline and not op.done():  # type: ignore[attr-defined]
                raise TimeoutError("operation did not complete")

    for i in range(total):
        t0 = _ns()
        sock = _direct_socket()
        try:
            op = proactor.connect(sock, address)
            wait_op(op)
            op.result()  # type: ignore[attr-defined]
        finally:
            try:
                sock.close()
            except OSError:
                pass
        t1 = _ns()
        if i >= warmup:
            samples_direct.append(t1 - t0)

    for i in range(total):
        t0 = _ns()
        create_op = proactor.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        wait_op(create_op)
        sock = create_op.result()  # type: ignore[attr-defined]
        try:
            connect_op = proactor.connect(sock, address)
            wait_op(connect_op)
            connect_op.result()  # type: ignore[attr-defined]
        finally:
            try:
                sock.close()
            except OSError:
                pass
        t1 = _ns()
        if i >= warmup:
            samples_chained.append(t1 - t0)

    proactor.close()
    _summarise("proactor: direct socket + connect", samples_direct)
    _summarise("proactor: create_socket + connect", samples_chained)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-n", "--iterations", type=int, default=5_000, help="timed iterations per case")
    parser.add_argument("-w", "--warmup", type=int, default=200, help="warmup iterations (not timed)")
    parser.add_argument("--entries", type=int, default=64, help="io_uring SQ entries")
    parser.add_argument(
        "--proactor-only",
        action="store_true",
        help="also run lower-level proactor Operation loops (no scheduler/IOWaiter)",
    )
    args = parser.parse_args()

    if not uring_api.is_available():
        print("uring-api native extension unavailable")
        return 1

    caps = uring_api.probe(entries=args.entries)
    print(f"IORING_OP_SOCKET={caps.get('IORING_OP_SOCKET')} IORING_OP_CONNECT={caps.get('IORING_OP_CONNECT', 'n/a')}")
    print(f"iterations={args.iterations} warmup={args.warmup} entries={args.entries}")
    print()

    listener, address = _make_listener()
    stop = threading.Event()
    acceptor = threading.Thread(target=_accept_loop, args=(listener, stop), name="micro-accept", daemon=True)
    acceptor.start()
    try:
        print("--- SyncProactorScheduler + ProactorIOManager ---")
        _bench_io_paths(address, args.iterations, args.warmup, args.entries)
        if args.proactor_only:
            print()
            print("--- SyncUringProactor Operations only ---")
            _bench_proactor_only(address, args.iterations, args.warmup, args.entries)
    finally:
        stop.set()
        try:
            listener.close()
        except OSError:
            pass
        acceptor.join(1.0)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

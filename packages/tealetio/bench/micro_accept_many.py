#!/usr/bin/env python3
"""Microbenchmark: accept_many eager drain vs continuous-only.

Times draining a pre-queued backlog via ``io.accept_many`` /
``accept_many_streams``. Toggle eager with ``TEALETIO_EAGER_ACCEPT`` (default on)
or ``--no-eager`` / ``--compare``.

Uses ``SelectorProactor`` by default (oneshot re-arm for the continuous leg).
Pass ``--uring`` for ``SyncUringProactor``.

Usage::

    uv run --active --package tealetio python packages/tealetio/bench/micro_accept_many.py
    uv run --active --package tealetio python packages/tealetio/bench/micro_accept_many.py --compare
    uv run --active --package tealetio python packages/tealetio/bench/micro_accept_many.py -n 200 -b 32
"""

from __future__ import annotations

import argparse
import os
import socket
import statistics
import time
from typing import Any

from tealetio.operations import is_io_cancellation
from tealetio.proactor import SelectorProactor, SyncProactorScheduler, SyncUringProactor
from tealetio.scheduler import set_scheduler


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
        f"{name:48s}  n={len(samples_ns):5d}  "
        f"mean={mean_us:8.2f} us  med={med_us:8.2f} us  "
        f"p90={p90_us:8.2f} us  p99={p99_us:8.2f} us"
    )


def _make_scheduler(*, uring: bool) -> SyncProactorScheduler:
    if uring:
        return SyncProactorScheduler(lambda: SyncUringProactor())
    return SyncProactorScheduler(lambda: SelectorProactor())


def _fill_backlog(addr: tuple[str, int], n: int) -> list[socket.socket]:
    clients: list[socket.socket] = []
    for _ in range(n):
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect(addr)
        clients.append(client)
    return clients


def _cancel_waiter(io: Any, waiter: Any) -> None:
    operation = getattr(waiter, "operation", None)
    if operation is not None and not operation.done():
        io.proactor.cancel(operation)


def _wait_ignore_cancel(waiter: Any) -> None:
    try:
        waiter.wait()
    except OSError as exc:
        if not is_io_cancellation(exc):
            raise


def _bench_accept_many(
    *,
    iterations: int,
    backlog: int,
    streams: bool,
    uring: bool,
    eager: bool,
) -> list[int]:
    os.environ["TEALETIO_EAGER_ACCEPT"] = "1" if eager else "0"
    samples: list[int] = []
    scheduler = _make_scheduler(uring=uring)
    set_scheduler(scheduler)
    try:

        def exercise() -> None:
            nonlocal samples
            io = scheduler.io
            listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("127.0.0.1", 0))
            listener.listen(max(backlog * 2, 128))
            listener.setblocking(False)
            addr = listener.getsockname()
            try:
                for _ in range(iterations):
                    clients = _fill_backlog(addr, backlog)
                    try:
                        got = 0

                        def on_accept(delivery: object) -> None:
                            nonlocal got
                            if streams:
                                _reader, writer = delivery  # type: ignore[misc]
                                writer.close()
                            else:
                                conn, _initial = delivery  # type: ignore[misc]
                                conn.close()
                            got += 1

                        t0 = _ns()
                        # oneshot backends finish after each accept; re-arm until backlog.
                        # multishot continuous never "dones" until cancel — do not wait on
                        # the waiter once got == backlog (that hangs on uring).
                        waiter = None
                        while got < backlog:
                            if streams:
                                waiter = io.accept_many_streams(listener, on_accept)
                            else:
                                waiter = io.accept_many(listener, on_accept)
                            if got >= backlog:
                                break
                            if not waiter.poll():
                                while got < backlog and not waiter.poll():
                                    scheduler.proactor.wait(0.0)
                                # oneshot: one accept completes the operation; wait it out
                                if got < backlog and not waiter.poll():
                                    _wait_ignore_cancel(waiter)
                        assert got == backlog, f"got {got} expected {backlog}"
                        if waiter is not None and not waiter.poll():
                            _cancel_waiter(io, waiter)
                            _wait_ignore_cancel(waiter)
                        samples.append(_ns() - t0)
                    finally:
                        for client in clients:
                            client.close()
            finally:
                listener.close()

        scheduler.run_until_complete(scheduler.spawn(exercise))
    finally:
        scheduler.close()
        set_scheduler(None)
    return samples


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-n", "--iterations", type=int, default=100, help="samples per path")
    parser.add_argument("-b", "--backlog", type=int, default=16, help="connections per sample")
    parser.add_argument("--warmup", type=int, default=5)
    parser.add_argument(
        "--uring",
        action="store_true",
        help="use SyncUringProactor (default: SelectorProactor)",
    )
    parser.add_argument(
        "--no-eager",
        action="store_true",
        help="disable eager drain (TEALETIO_EAGER_ACCEPT=0); ignored with --compare",
    )
    parser.add_argument(
        "--compare",
        action="store_true",
        help="run eager and continuous-only back-to-back",
    )
    args = parser.parse_args()

    backend = "uring" if args.uring else "selector"
    print(f"backend={backend} iterations={args.iterations} backlog={args.backlog} warmup={args.warmup}")

    if args.compare:
        mode_list: list[tuple[str, bool]] = [("eager", True), ("continuous-only", False)]
    else:
        eager = not args.no_eager
        mode_list = [("eager" if eager else "continuous-only", eager)]

    for streams, base_label in ((False, "accept_many"), (True, "accept_many_streams")):
        for mode_name, eager in mode_list:
            label = f"{base_label} {mode_name}"
            _bench_accept_many(
                iterations=args.warmup,
                backlog=args.backlog,
                streams=streams,
                uring=args.uring,
                eager=eager,
            )
            samples = _bench_accept_many(
                iterations=args.iterations,
                backlog=args.backlog,
                streams=streams,
                uring=args.uring,
                eager=eager,
            )
            _summarise(label, samples)


if __name__ == "__main__":
    main()

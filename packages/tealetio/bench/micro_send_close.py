#!/usr/bin/env python3
"""Compare sendall+close vs sock_send_close (eager send on/off).

Classic: ``sock_sendall().wait()`` then ``sock_close``.
``sock_send_close``: fire-and-forget sendall+close via ``send_close_nowait``;
we also time until the peer sees EOF.

Usage::

    uv run --active --package tealetio python packages/tealetio/bench/micro_send_close.py --uring
    TEALETIO_EAGER_SEND=0 uv run --active --package tealetio python \\
      packages/tealetio/bench/micro_send_close.py --uring
"""

from __future__ import annotations

import argparse
import os
import socket
import statistics
import time

from tealetio.proactor import SelectorProactor, SyncProactorScheduler, SyncUringProactor
from tealetio.scheduler import set_scheduler


def _ns() -> int:
    return time.perf_counter_ns()


def _summarise(name: str, samples_ns: list[int]) -> None:
    mean_us = statistics.fmean(samples_ns) / 1000.0
    med_us = statistics.median(samples_ns) / 1000.0
    print(f"{name:44s}  n={len(samples_ns):4d}  mean={mean_us:8.2f} us  med={med_us:8.2f} us")


def _make_scheduler(*, uring: bool) -> SyncProactorScheduler:
    if uring:
        return SyncProactorScheduler(lambda: SyncUringProactor())
    return SyncProactorScheduler(lambda: SelectorProactor())


def _bench_classic(*, iterations: int, payload: bytes, uring: bool) -> tuple[list[int], list[int]]:
    submit: list[int] = []
    until_eof: list[int] = []
    scheduler = _make_scheduler(uring=uring)
    set_scheduler(scheduler)
    try:

        def exercise() -> None:
            io = scheduler.io
            for _ in range(iterations):
                reader, writer = socket.socketpair()
                reader.setblocking(False)
                writer.setblocking(False)
                try:
                    t0 = _ns()
                    io.sock_sendall(writer, payload).wait()
                    io.sock_close(writer)
                    submit.append(_ns() - t0)
                    deadline = time.monotonic() + 2.0
                    got = bytearray()
                    while time.monotonic() < deadline:
                        try:
                            chunk = reader.recv(65536)
                        except BlockingIOError:
                            scheduler.proactor.wait(0.0)
                            continue
                        if not chunk:
                            break
                        got.extend(chunk)
                    until_eof.append(_ns() - t0)
                    assert bytes(got) == payload
                finally:
                    reader.close()
                    if writer.fileno() != -1:
                        writer.close()

        scheduler.run_until_complete(scheduler.spawn(exercise))
    finally:
        scheduler.close()
        set_scheduler(None)
    return submit, until_eof


def _bench_send_close(*, iterations: int, payload: bytes, uring: bool) -> tuple[list[int], list[int]]:
    submit: list[int] = []
    until_eof: list[int] = []
    scheduler = _make_scheduler(uring=uring)
    set_scheduler(scheduler)
    try:

        def exercise() -> None:
            io = scheduler.io
            for _ in range(iterations):
                reader, writer = socket.socketpair()
                reader.setblocking(False)
                writer.setblocking(False)
                try:
                    t0 = _ns()
                    io.sock_send_close(writer, payload)
                    submit.append(_ns() - t0)
                    deadline = time.monotonic() + 2.0
                    got = bytearray()
                    while time.monotonic() < deadline:
                        try:
                            chunk = reader.recv(65536)
                        except BlockingIOError:
                            scheduler.proactor.wait(0.0)
                            continue
                        if not chunk:
                            break
                        got.extend(chunk)
                    until_eof.append(_ns() - t0)
                    assert bytes(got) == payload
                finally:
                    reader.close()
                    if writer.fileno() != -1:
                        writer.close()

        scheduler.run_until_complete(scheduler.spawn(exercise))
    finally:
        scheduler.close()
        set_scheduler(None)
    return submit, until_eof


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-n", "--iterations", type=int, default=80)
    parser.add_argument("-p", "--payload", type=int, default=685)
    parser.add_argument("--warmup", type=int, default=8)
    parser.add_argument("--uring", action="store_true")
    args = parser.parse_args()
    payload = b"x" * args.payload
    backend = "uring" if args.uring else "selector"
    eager = os.environ.get("TEALETIO_EAGER_SEND", "1")
    print(f"backend={backend} TEALETIO_EAGER_SEND={eager} payload={args.payload} n={args.iterations}")
    for label, fn in (("classic sendall+close", _bench_classic), ("sock_send_close", _bench_send_close)):
        fn(iterations=args.warmup, payload=payload, uring=args.uring)
        submit, eof = fn(iterations=args.iterations, payload=payload, uring=args.uring)
        _summarise(f"{label}  submit", submit)
        _summarise(f"{label}  until peer EOF", eof)


if __name__ == "__main__":
    main()

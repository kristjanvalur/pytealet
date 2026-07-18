#!/usr/bin/env python3
"""Microbenchmark: socket creation paths used by tealetio.

Compares:

1. ``socket.socket`` + ``setblocking(False)`` + ``set_inheritable(False)``
   (``configure_scheduler_socket`` / sync proactor path)
2. ``socket.socket`` with ``SOCK_NONBLOCK | SOCK_CLOEXEC`` in the type flags
   (single ``socket()`` syscall when the platform supports it)
3. Raw ``uring_api.Ring.submit_socket`` + ``wait`` + ``socket_from_uring_fd``
4. ``SyncUringProactor.create_socket`` + ``wait`` (full Operation path, inline reaper)

Usage::

    uv run --active --package tealetio python packages/tealetio/bench/micro_socket_create.py
    uv run --active --package tealetio python packages/tealetio/bench/micro_socket_create.py -n 20000
"""

from __future__ import annotations

import argparse
import os
import socket
import statistics
import time
from collections.abc import Callable

import uring_api
from tealetio.proactor import SyncUringProactor
from tealetio.socket_helpers import configure_scheduler_socket, socket_from_uring_fd

_SOCK_NONBLOCK = getattr(socket, "SOCK_NONBLOCK", 0)
_SOCK_CLOEXEC = getattr(socket, "SOCK_CLOEXEC", 0)
_URING_TYPE = socket.SOCK_STREAM | _SOCK_NONBLOCK | _SOCK_CLOEXEC


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
        f"{name:40s}  n={len(samples_ns):6d}  "
        f"mean={mean_us:8.2f} us  med={med_us:8.2f} us  "
        f"p90={p90_us:8.2f} us  p99={p99_us:8.2f} us"
    )


def _bench(name: str, iterations: int, warmup: int, body: Callable[[], socket.socket]) -> None:
    samples: list[int] = []
    for i in range(warmup + iterations):
        t0 = _ns()
        sock = body()
        t1 = _ns()
        sock.close()
        if i >= warmup:
            samples.append(t1 - t0)
    _summarise(name, samples)


def _sync_socket_then_flags() -> socket.socket:
    return configure_scheduler_socket(socket.socket(socket.AF_INET, socket.SOCK_STREAM))


def _sync_socket_type_flags() -> socket.socket:
    if not (_SOCK_NONBLOCK and _SOCK_CLOEXEC):
        raise RuntimeError("SOCK_NONBLOCK/SOCK_CLOEXEC not available")
    # kernel already non-blocking; still sync the Python wrapper like socket_from_uring_fd
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM | _SOCK_NONBLOCK | _SOCK_CLOEXEC)
    sock.setblocking(False)
    return sock


def _make_uring_raw(entries: int) -> Callable[[], socket.socket]:
    ring = uring_api.Ring(entries)

    def body() -> socket.socket:
        pending = ring.submit_socket(socket.AF_INET, _URING_TYPE, 0, 0)
        batch = ring.wait(1.0)
        assert batch is not None and len(batch) == 1
        completion = batch[0]
        assert completion is pending
        if completion.res < 0:
            raise OSError(-completion.res, os.strerror(-completion.res))
        return socket_from_uring_fd(completion.res)

    body._ring = ring  # type: ignore[attr-defined]
    return body


def _make_proactor_create(entries: int) -> Callable[[], socket.socket]:
    proactor = SyncUringProactor(entries=entries)

    def body() -> socket.socket:
        op = proactor.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        if not op.done():
            proactor.wait(proactor.get_time() + 1.0)
        return op.result()

    body._proactor = proactor  # type: ignore[attr-defined]
    return body


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-n", "--iterations", type=int, default=10_000, help="timed iterations per case")
    parser.add_argument("-w", "--warmup", type=int, default=500, help="warmup iterations (not timed)")
    parser.add_argument("--entries", type=int, default=64, help="io_uring SQ entries")
    args = parser.parse_args()

    if not uring_api.is_available():
        print("uring-api native extension unavailable; cannot run uring cases")
        return 1

    caps = uring_api.probe(entries=args.entries)
    print(f"kernel probe IORING_OP_SOCKET={caps.get('IORING_OP_SOCKET')}")
    print(f"SOCK_NONBLOCK={_SOCK_NONBLOCK:#x} SOCK_CLOEXEC={_SOCK_CLOEXEC:#x}")
    print(f"iterations={args.iterations} warmup={args.warmup} entries={args.entries}")
    print()

    _bench("socket()+setblocking+set_inheritable", args.iterations, args.warmup, _sync_socket_then_flags)

    if _SOCK_NONBLOCK and _SOCK_CLOEXEC:
        _bench("socket(NONBLOCK|CLOEXEC)+setblocking", args.iterations, args.warmup, _sync_socket_type_flags)
    else:
        print(f"{'socket(NONBLOCK|CLOEXEC)':40s}  skipped (flags unavailable)")

    if not caps.get("IORING_OP_SOCKET"):
        print(f"{'uring submit_socket+wait':40s}  skipped (IORING_OP_SOCKET unavailable)")
        print(f"{'SyncUringProactor.create_socket':40s}  skipped (IORING_OP_SOCKET unavailable)")
        return 0

    raw = _make_uring_raw(args.entries)
    try:
        _bench("uring submit_socket+wait+wrap", args.iterations, args.warmup, raw)
    finally:
        raw._ring.close()  # type: ignore[attr-defined]

    proactor_body = _make_proactor_create(args.entries)
    try:
        _bench("SyncUringProactor.create_socket+wait", args.iterations, args.warmup, proactor_body)
    finally:
        proactor_body._proactor.close()  # type: ignore[attr-defined]

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

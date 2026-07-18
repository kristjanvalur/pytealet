#!/usr/bin/env python3
"""Microbenchmark: io.sock_recvall (eager drain + continuous fallback).

Pre-fills a socket with a fixed payload, then times ``scheduler.io.sock_recvall``
until EOF. Uses ``SelectorProactor`` by default; pass ``--uring`` for
``SyncUringProactor``.

Usage::

    uv run --active --package tealetio python packages/tealetio/bench/micro_recv_many.py
    uv run --active --package tealetio python packages/tealetio/bench/micro_recv_many.py --uring
"""

from __future__ import annotations

import argparse
import socket
import statistics
import time

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


def _bench_recvall(
    *,
    iterations: int,
    payload_bytes: int,
    chunk_size: int,
    uring: bool,
) -> list[int]:
    samples: list[int] = []
    payload = b"x" * payload_bytes
    scheduler = _make_scheduler(uring=uring)
    set_scheduler(scheduler)
    try:

        def exercise() -> None:
            nonlocal samples
            io = scheduler.io
            # uring BufGroup requires power-of-two buffer_count
            need = max(8, payload_bytes // chunk_size + 4)
            buffer_count = 1
            while buffer_count < need:
                buffer_count *= 2
            pool = io.create_recv_buffer_pool(chunk_size, buffer_count)
            for _ in range(iterations):
                reader, writer = socket.socketpair()
                reader.setblocking(False)
                writer.setblocking(False)
                try:
                    writer.sendall(payload)
                    writer.shutdown(socket.SHUT_WR)
                    t0 = _ns()
                    data = io.sock_recvall(reader, buffer_pool=pool)
                    samples.append(_ns() - t0)
                    assert data == payload, f"got {len(data)} expected {payload_bytes}"
                finally:
                    reader.close()
                    writer.close()

        scheduler.run_until_complete(scheduler.spawn(exercise))
    finally:
        scheduler.close()
        set_scheduler(None)
    return samples


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-n", "--iterations", type=int, default=200, help="samples")
    parser.add_argument("-p", "--payload", type=int, default=64 * 1024, help="bytes pre-written per sample")
    parser.add_argument("-c", "--chunk-size", type=int, default=4096, help="recv buffer pool buffer_size")
    parser.add_argument("--warmup", type=int, default=10)
    parser.add_argument(
        "--uring",
        action="store_true",
        help="use SyncUringProactor (default: SelectorProactor)",
    )
    args = parser.parse_args()

    backend = "uring" if args.uring else "selector"
    print(
        f"backend={backend} iterations={args.iterations} payload={args.payload} "
        f"chunk_size={args.chunk_size} warmup={args.warmup}"
    )

    _bench_recvall(
        iterations=args.warmup,
        payload_bytes=args.payload,
        chunk_size=args.chunk_size,
        uring=args.uring,
    )
    samples = _bench_recvall(
        iterations=args.iterations,
        payload_bytes=args.payload,
        chunk_size=args.chunk_size,
        uring=args.uring,
    )
    _summarise("sock_recvall", samples)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Microbenchmark: sock_sendall and SendBuffer write/drain/flush.

``SendBuffer.write()`` submits each leg through ``scheduler.io.sock_sendall``.
When the socket accepts the buffer immediately, the leg completes as
``IOWaiterSync`` and ``_on_leg_complete`` chains the next chunk without a
proactor park. That keeps ``pending_bytes`` down, so ``drain()`` often returns
without blocking (it only parks above ``high_water``). ``flush()`` still waits
for the queue to empty, but each leg is cheaper when eager.

Usage::

    uv run --active --package tealetio python packages/tealetio/bench/micro_sendall.py
    uv run --active --package tealetio python packages/tealetio/bench/micro_sendall.py --uring
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


def _bench_sock_sendall(
    *,
    iterations: int,
    payload_bytes: int,
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
            for _ in range(iterations):
                reader, writer = socket.socketpair()
                reader.setblocking(False)
                writer.setblocking(False)
                try:
                    t0 = _ns()
                    io.sock_sendall(writer, payload).wait()
                    samples.append(_ns() - t0)
                    # drain peer so the next pair is not back-pressured by leftovers
                    while True:
                        try:
                            chunk = reader.recv(65536)
                        except BlockingIOError:
                            break
                        if not chunk:
                            break
                finally:
                    reader.close()
                    writer.close()

        scheduler.run_until_complete(scheduler.spawn(exercise))
    finally:
        scheduler.close()
        set_scheduler(None)
    return samples


def _bench_send_buffer_flush(
    *,
    iterations: int,
    chunk_bytes: int,
    chunks: int,
    uring: bool,
) -> list[int]:
    """Time write(N chunks) + drain/flush on ``SendBuffer`` (stream writer path)."""

    samples: list[int] = []
    chunk = b"y" * chunk_bytes
    total = chunk_bytes * chunks
    scheduler = _make_scheduler(uring=uring)
    set_scheduler(scheduler)
    try:

        def exercise() -> None:
            nonlocal samples
            io = scheduler.io
            for _ in range(iterations):
                reader, writer = socket.socketpair()
                reader.setblocking(False)
                writer.setblocking(False)
                send_buffer = io._open_send_buffer(writer)
                received = 0

                def pump_reader() -> None:
                    nonlocal received
                    while received < total:
                        try:
                            data = reader.recv(65536)
                        except BlockingIOError:
                            scheduler.proactor.wait(0.0)
                            continue
                        if not data:
                            break
                        received += len(data)

                reader_task = scheduler.spawn(pump_reader)
                try:
                    t0 = _ns()
                    for _i in range(chunks):
                        send_buffer.write(chunk)
                        # same pattern as stream writers: write then drain pressure
                        send_buffer.drain()
                    send_buffer.flush()
                    samples.append(_ns() - t0)
                    reader_task.wait()
                    assert received == total, f"got {received} expected {total}"
                finally:
                    send_buffer.close()
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
    parser.add_argument(
        "-p",
        "--payload",
        type=int,
        default=4096,
        help="bytes per sock_sendall sample (must fit socket buffer without a reader)",
    )
    parser.add_argument("-c", "--chunk-size", type=int, default=1024, help="SendBuffer write size")
    parser.add_argument("--chunks", type=int, default=32, help="SendBuffer writes per sample")
    parser.add_argument("--warmup", type=int, default=10)
    parser.add_argument(
        "--uring",
        action="store_true",
        help="use SyncUringProactor (default: SelectorProactor)",
    )
    parser.add_argument(
        "--path",
        choices=("sendall", "buffer", "both"),
        default="both",
        help="which paths to measure",
    )
    args = parser.parse_args()

    backend = "uring" if args.uring else "selector"
    print(
        f"backend={backend} iterations={args.iterations} payload={args.payload} "
        f"chunk_size={args.chunk_size} chunks={args.chunks} warmup={args.warmup}"
    )

    if args.path in ("sendall", "both"):
        _bench_sock_sendall(
            iterations=args.warmup,
            payload_bytes=args.payload,
            uring=args.uring,
        )
        samples = _bench_sock_sendall(
            iterations=args.iterations,
            payload_bytes=args.payload,
            uring=args.uring,
        )
        _summarise("sock_sendall", samples)

    if args.path in ("buffer", "both"):
        _bench_send_buffer_flush(
            iterations=args.warmup,
            chunk_bytes=args.chunk_size,
            chunks=args.chunks,
            uring=args.uring,
        )
        samples = _bench_send_buffer_flush(
            iterations=args.iterations,
            chunk_bytes=args.chunk_size,
            chunks=args.chunks,
            uring=args.uring,
        )
        _summarise("SendBuffer write+drain+flush", samples)


if __name__ == "__main__":
    main()

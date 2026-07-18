#!/usr/bin/env python3
"""Microbenchmark: UringOperation freelist with fake io_uring.

Compares one-shot ``recv`` scaffolding when:

* freelist disabled (``op_pool_max=0``)
* freelist + explicit ``recycle_operation`` after use
* freelist enabled but refs held without recycle (no reuse)

The fake ring completes CQEs inline on submit. History lists that pin
``user_data`` are cleared so the waitable can actually be recycled.

Usage::

    uv run --active --package tealetio python \\
        packages/tealetio/bench/micro_uring_op_pool.py
    uv run --active --package tealetio python \\
        packages/tealetio/bench/micro_uring_op_pool.py -n 100000 --pool 256
"""

from __future__ import annotations

import argparse
import socket
import statistics
import sys
import time
from pathlib import Path

_TESTS = Path(__file__).resolve().parents[1] / "tests"
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

from uring_fakes import _FakeUringRing  # noqa: E402

from tealetio.proactor import UringProactor  # noqa: E402


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
        f"{name:40s}  n={len(samples_ns):7d}  "
        f"mean={mean_us:8.3f} us  med={med_us:8.3f} us  "
        f"p90={p90_us:8.3f} us  p99={p99_us:8.3f} us"
    )


def _run_batch(
    *,
    op_pool_max: int,
    iterations: int,
    warmup: int,
    mode: str,
) -> tuple[list[int], dict[str, int]]:
    proactor = UringProactor(ring_factory=_FakeUringRing, op_pool_max=op_pool_max)
    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        writer.send(b"x" * 64)
        samples: list[int] = []
        ring = proactor.ring
        held: list[object] = []
        for i in range(warmup + iterations):
            t0 = _ns()
            op = proactor.recv(reader, 8)
            if not op.done():
                proactor.wait(proactor.get_time() + 1.0)
            _ = op.result()
            # Drop fake history pins (real ring releases Completions after delivery).
            ring.submitted_recv.clear()
            if mode == "explicit":
                proactor.recycle_operation(op)
            elif mode == "hold":
                held.append(op)
            else:
                raise ValueError(mode)
            t1 = _ns()
            if i >= warmup:
                samples.append(t1 - t0)
        return samples, proactor.op_pool_stats
    finally:
        reader.close()
        writer.close()
        proactor.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-n", "--iterations", type=int, default=80_000)
    parser.add_argument("--warmup", type=int, default=3_000)
    parser.add_argument("--pool", type=int, default=256, help="op_pool_max when freelist enabled")
    args = parser.parse_args()

    print(f"fake-io recv loop  iterations={args.iterations} warmup={args.warmup}")
    print()

    cases = (
        ("no pool", 0, "explicit"),
        ("pool + explicit recycle", args.pool, "explicit"),
        ("pool + hold refs (no recycle)", args.pool, "hold"),
    )
    for label, pool_max, mode in cases:
        samples, stats = _run_batch(
            op_pool_max=pool_max,
            iterations=args.iterations,
            warmup=args.warmup,
            mode=mode if pool_max else "explicit",
        )
        # For no-pool case, recycle is a no-op (max=0) but we still call it.
        hit_den = stats["hits"] + stats["misses"]
        hit_rate = (stats["hits"] / hit_den) if hit_den else 0.0
        _summarise(label, samples)
        print(
            f"{'':40s}  hits={stats['hits']} misses={stats['misses']} "
            f"hit_rate={hit_rate:5.1%} releases={stats['releases']} "
            f"drops={stats['drops']} size={stats['size']}"
        )
        print()


if __name__ == "__main__":
    main()

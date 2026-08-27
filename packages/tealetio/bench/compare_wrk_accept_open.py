#!/usr/bin/env python3
"""Compare wrk throughput for accept stream-open on worker vs issuer.

Default ``TEALETIO_ACCEPT_OPEN_STREAMS=worker`` opens streams (buf group +
``recv_many``) on the accept delivery thread. ``owner`` marshals the accepted
socket and opens on the scheduler/issuer thread.

Usage::

    uv run --active --package tealetio python packages/tealetio/bench/compare_wrk_accept_open.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

_BENCH_DIR = Path(__file__).resolve().parent
if str(_BENCH_DIR) not in sys.path:
    sys.path.insert(0, str(_BENCH_DIR))

from compare_wrk_ring import Case, _run_case  # noqa: E402


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8140)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--connections", type=int, default=256)
    parser.add_argument("--warmup", default="5s")
    parser.add_argument("--duration", default="15s")
    parser.add_argument("--runs", type=int, default=2)
    parser.add_argument("--entries", type=int, default=8)
    args = parser.parse_args()

    connections = max(args.connections, args.threads)
    cases = [
        ("workers worker-open", "uring", "worker"),
        ("workers owner-open", "uring", "owner"),
        ("sync worker-open", "uring-sync", "worker"),
        ("sync owner-open", "uring-sync", "owner"),
    ]
    print(
        f"wrk: threads={args.threads} connections={connections} entries={args.entries} "
        f"warmup={args.warmup} duration={args.duration} runs={args.runs}"
    )
    print(f"{'label':<22}  {'req/s avg':>10}  {'req/s min':>10}  {'req/s max':>10}  {'p50 us':>8}")
    for index, (label, proactor, open_where) in enumerate(cases):
        os.environ["TEALETIO_ACCEPT_OPEN_STREAMS"] = open_where
        case = Case(
            label,
            proactor,
            args.entries,
            completion_threads=2 if proactor == "uring" else 0,
        )
        stats = _run_case(
            case,
            args.host,
            args.port + index,
            threads=args.threads,
            connections=connections,
            warmup=args.warmup,
            duration=args.duration,
            runs=args.runs,
        )
        print(
            f"{label:<22}  {stats['req_sec_avg']:10.1f}  {stats['req_sec_min']:10.1f}  "
            f"{stats['req_sec_max']:10.1f}  {stats['p50_us_avg']:8.1f}",
            flush=True,
        )


if __name__ == "__main__":
    main()

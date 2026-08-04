#!/usr/bin/env python3
"""Compare uring wrk throughput for event vs token wakeup managers."""

from __future__ import annotations

import argparse
import os
import re
import socket
import subprocess
import time
from pathlib import Path

_BENCH_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _BENCH_DIR.parent.parent.parent

REQ_SEC_RE = re.compile(r"Requests/sec:\s+([\d.]+)")
REQ_TOTAL_RE = re.compile(r"(\d+) requests in")
LAT_P50_RE = re.compile(r"50%\s+([\d.]+)(\w+)")


def _wait_listen(host: str, port: int, proc: subprocess.Popen[bytes], timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError("server exited before becoming ready")
        try:
            with socket.create_connection((host, port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise TimeoutError(f"server not ready on {host}:{port}")


def _start_server(host: str, port: int, manager: str) -> subprocess.Popen[bytes]:
    script = _BENCH_DIR / "servers" / "tealetio_sync.py"
    env = os.environ.copy()
    env["TEALETIO_WAKEUP_MANAGER"] = manager
    return subprocess.Popen(
        [
            "uv",
            "run",
            "--active",
            "--package",
            "tealetio",
            "python",
            str(script),
            "--host",
            host,
            "--port",
            str(port),
            "--proactor",
            "uring",
        ],
        cwd=_REPO_ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=env,
    )


def _run_wrk(host: str, port: int, *, threads: int, connections: int, duration: str) -> dict[str, float | int]:
    url = f"http://{host}:{port}/"
    out = subprocess.check_output(
        ["wrk", f"-t{threads}", f"-c{connections}", f"-d{duration}", "--latency", url],
        text=True,
        stderr=subprocess.STDOUT,
    )
    req_sec = float(REQ_SEC_RE.search(out).group(1))  # type: ignore[union-attr]
    total = int(REQ_TOTAL_RE.search(out).group(1))  # type: ignore[union-attr]
    lat_match = LAT_P50_RE.search(out)
    lat_us = 0.0
    if lat_match:
        value = float(lat_match.group(1))
        unit = lat_match.group(2)
        if unit == "ms":
            lat_us = value * 1000.0
        elif unit == "us":
            lat_us = value
        elif unit == "s":
            lat_us = value * 1_000_000.0
    return {"req_sec": req_sec, "total": total, "p50_us": lat_us}


def _run_case(
    manager: str,
    host: str,
    port: int,
    *,
    threads: int,
    connections: int,
    warmup: str,
    duration: str,
    runs: int,
) -> dict[str, float]:
    proc = _start_server(host, port, manager)
    try:
        _wait_listen(host, port, proc)
        _run_wrk(host, port, threads=threads, connections=connections, duration=warmup)
        samples = [
            _run_wrk(host, port, threads=threads, connections=connections, duration=duration)
            for _ in range(runs)
        ]
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=3)

    req_rates = [sample["req_sec"] for sample in samples]
    p50s = [sample["p50_us"] for sample in samples if sample["p50_us"]]
    return {
        "req_sec_avg": sum(req_rates) / len(req_rates),
        "req_sec_min": min(req_rates),
        "req_sec_max": max(req_rates),
        "p50_us_avg": sum(p50s) / len(p50s) if p50s else 0.0,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8100)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--connections", type=int, nargs="+", default=[4, 8, 32, 128])
    parser.add_argument("--warmup", default="3s")
    parser.add_argument("--duration", default="10s")
    parser.add_argument("--runs", type=int, default=3)
    args = parser.parse_args()

    print(
        f"wrk: threads={args.threads} warmup={args.warmup} duration={args.duration} runs={args.runs}"
    )
    print(f"{'connections':>11}  {'manager':>5}  {'req/s avg':>10}  {'req/s min':>10}  {'req/s max':>10}  {'p50 us':>8}")
    for index, connections in enumerate(args.connections):
        port = args.port + index
        effective_connections = max(connections, args.threads)
        for manager in ("event", "token"):
            stats = _run_case(
                manager,
                args.host,
                port,
                threads=args.threads,
                connections=effective_connections,
                warmup=args.warmup,
                duration=args.duration,
                runs=args.runs,
            )
            print(
                f"{effective_connections:11d}  {manager:>5}  "
                f"{stats['req_sec_avg']:10.1f}  {stats['req_sec_min']:10.1f}  {stats['req_sec_max']:10.1f}  "
                f"{stats['p50_us_avg']:8.1f}"
            )


if __name__ == "__main__":
    main()
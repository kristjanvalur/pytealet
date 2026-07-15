#!/usr/bin/env python3
"""Single-request latency breakdown: 1 warmup curl + 2 measured curls per backend."""

from __future__ import annotations

import argparse
import json
import re
import socket
import subprocess
import time
from pathlib import Path
from typing import Any

_BENCH_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _BENCH_DIR.parent.parent.parent

CURL_FMT = (
    "req=%{http_code} "
    "dns=%{time_namelookup} connect=%{time_connect} "
    "ttfb=%{time_starttransfer} total=%{time_total} "
    "bytes=%{size_download}"
)

PROFILE_RE = re.compile(r"^PROFILE (.+)$")


def _wait_listen(host: str, port: int, proc: subprocess.Popen[bytes], timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError("profile server exited before becoming ready")
        try:
            with socket.create_connection((host, port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise TimeoutError(f"server not ready on {host}:{port}")


def _curl(url: str) -> dict[str, str]:
    out = subprocess.check_output(
        ["curl", "-fsS", "-o", "/dev/null", "-w", CURL_FMT, url],
        text=True,
    )
    return dict(item.split("=", 1) for item in out.strip().split())


def _start_server(name: str, host: str, port: int, extra_args: list[str]) -> subprocess.Popen[bytes]:
    script = _BENCH_DIR / "servers" / f"{name}.py"
    cmd = [
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
        "--profile",
        *extra_args,
    ]
    return subprocess.Popen(
        cmd,
        cwd=_REPO_ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )


def _collect_profiles(proc: subprocess.Popen[bytes]) -> list[dict[str, Any]]:
    profiles: list[dict[str, Any]] = []
    assert proc.stderr is not None
    for raw in proc.stderr:
        line = raw.decode(errors="replace").rstrip("\n")
        match = PROFILE_RE.match(line)
        if match:
            profiles.append(json.loads(match.group(1)))
    return profiles


def _phase_table(profile: dict[str, Any]) -> str:
    pre = next(
        (ph.get("pre_handler_ms") for ph in profile["phases"] if ph["phase"] == "handler_start"),
        None,
    )
    head = f"  server total={profile['total_ms']:.2f}ms readlines={profile['readline_calls']}"
    if pre is not None:
        head += f" pre_handler={pre:.2f}ms"
    lines = [head]
    for phase in profile["phases"]:
        extra = ""
        if phase.get("pre_handler_ms") is not None:
            extra = f" pre_handler={phase['pre_handler_ms']:.2f}ms"
        lines.append(f"    {phase['phase']:16s}  +{phase['delta_ms']:7.2f}ms  @{phase['since_start_ms']:7.2f}ms{extra}")
    lines.append(f"    readline sum     {profile['readline_wait_ms']:7.2f}ms  ({profile['readline_bytes']} bytes)")
    return "\n".join(lines)


def _run_concurrent(url: str, count: int) -> list[float]:
    import concurrent.futures

    def one() -> float:
        row = _curl(url)
        return float(row["total"]) * 1000.0

    with concurrent.futures.ThreadPoolExecutor(max_workers=count) as pool:
        return list(pool.map(lambda _: one(), range(count)))


def _run_case(label: str, server: str, host: str, port: int, extra_args: list[str], *, concurrent: int) -> None:
    url = f"http://{host}:{port}/"
    proc = _start_server(server, host, port, extra_args)
    try:
        _wait_listen(host, port, proc)
        print(f"\n=== {label} ===")
        curl_rows: list[dict[str, str]] = []
        for n in range(3):
            row = _curl(url)
            row["run"] = str(n)
            curl_rows.append(row)
            kind = "warmup" if n == 0 else "measure"
            print(
                f"  curl {kind}: total={float(row['total']) * 1000:.2f}ms "
                f"connect={float(row['connect']) * 1000:.2f}ms "
                f"ttfb={float(row['ttfb']) * 1000:.2f}ms"
            )
        if concurrent > 1:
            totals = _run_concurrent(url, concurrent)
            print(
                f"  concurrent x{concurrent}: "
                f"min={min(totals):.2f}ms max={max(totals):.2f}ms avg={sum(totals) / len(totals):.2f}ms"
            )
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=3)
        profiles = _collect_profiles(proc)

    measured = [p for p in profiles if p["req"] in (1, 2)]
    for profile in measured:
        print(f"  server req={profile['req']} ({label}):")
        print(_phase_table(profile))

    if len(measured) == 2:
        avg_total = sum(p["total_ms"] for p in measured) / 2
        avg_drain = (
            sum(next((ph["delta_ms"] for ph in p["phases"] if ph["phase"] == "drain"), 0.0) for p in measured) / 2
        )
        avg_write = (
            sum(next((ph["delta_ms"] for ph in p["phases"] if ph["phase"] == "write"), 0.0) for p in measured) / 2
        )
        avg_pre = [
            ph.get("pre_handler_ms")
            for p in measured
            for ph in p["phases"]
            if ph["phase"] == "handler_start" and ph.get("pre_handler_ms") is not None
        ]
        pre_s = f" pre_handler={sum(avg_pre) / len(avg_pre):.2f}ms" if avg_pre else ""
        print(f"  avg measured: server={avg_total:.2f}ms drain={avg_drain:.2f}ms write={avg_write:.2f}ms{pre_s}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8090)
    parser.add_argument(
        "--cases",
        nargs="*",
        default=("asyncio", "tealetio-selector", "tealetio-uring"),
        choices=("asyncio", "tealetio-selector", "tealetio-uring", "tealetio-default"),
    )
    parser.add_argument(
        "--concurrent",
        type=int,
        default=32,
        help="after sequential curls, fire this many parallel curls (0 disables)",
    )
    args = parser.parse_args()

    cases: dict[str, tuple[str, list[str]]] = {
        "asyncio": ("asyncio_std", []),
        "tealetio-default": ("tealetio_sync", []),
        "tealetio-selector": ("tealetio_sync", ["--proactor", "selector"]),
        "tealetio-uring": ("tealetio_sync", ["--proactor", "uring"]),
    }

    for index, case in enumerate(args.cases):
        server, extra = cases[case]
        port = args.port + index
        _run_case(case, server, args.host, port, extra, concurrent=args.concurrent)


if __name__ == "__main__":
    main()

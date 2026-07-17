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
ACCEPT_DIAG_RE = re.compile(r"\[stream-diag [\d.]+ [^\]]+\] (accept_\w+) #\d+ (.*)$")
BREAK_WAIT_TIMING_RE = re.compile(r"\[break-wait-timing\] (\w+) (.*)$")
EVENT_WAKEUP_TIMING_RE = re.compile(r"\[event-wakeup-timing\] (\w+) (.*)$")
ACCEPT_PATH_TIMING_RE = re.compile(r"\[accept-path-timing\] ready (.*)$")
RECV_ITER_TIMING_RE = re.compile(r"\[recv-iter-timing\] ready (.*)$")
WORKER_COMPLETION_TIMING_RE = re.compile(r"\[worker-completion-timing\] (.*)$")
HANDLER_TEALET_TIMING_RE = re.compile(r"\[handler-tealet-timing\] (.*)$")


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


def _start_server(
    name: str,
    host: str,
    port: int,
    extra_args: list[str],
    *,
    diag: bool,
    env_extra: dict[str, str] | None = None,
) -> subprocess.Popen[bytes]:
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
    if diag and name.startswith("tealetio"):
        cmd.append("--diag")
    import os

    env = os.environ.copy()
    if diag:
        env["TEALETIO_STREAM_DIAG"] = "1"
        env["TEALETIO_URING_ACCEPT_LOG"] = "1"
    if env_extra:
        env.update(env_extra)
    return subprocess.Popen(
        cmd,
        cwd=_REPO_ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        env=env,
    )


def _read_stderr_lines(proc: subprocess.Popen[bytes]) -> list[str]:
    assert proc.stderr is not None
    data = proc.stderr.read()
    if not data:
        return []
    return [line for line in data.decode(errors="replace").splitlines()]


def _summarize_break_wait_timing(lines: list[str]) -> None:
    waits: list[float] = []
    handoffs: list[float] = []
    sleeps: list[float] = []
    for line in lines:
        match = BREAK_WAIT_TIMING_RE.match(line)
        if not match:
            continue
        event, tail = match.group(1), match.group(2)
        fields = dict(item.split("=", 1) for item in tail.split() if "=" in item)
        if event == "wait_return" and fields.get("woke") == "1" and "since_signal_us" in fields:
            waits.append(float(fields["since_signal_us"]))
        if event == "handoff_done" and "since_signal_us" in fields:
            handoffs.append(float(fields["since_signal_us"]))
        if event == "sleep0_done" and "sleep_us" in fields:
            sleeps.append(float(fields["sleep_us"]))
    parts: list[str] = []
    if handoffs:
        parts.append(f"handoff avg={sum(handoffs) / len(handoffs):.1f}us n={len(handoffs)}")
    if waits:
        parts.append(f"wait_since_signal avg={sum(waits) / len(waits):.1f}us n={len(waits)}")
    if sleeps:
        parts.append(f"sleep0 avg={sum(sleeps) / len(sleeps):.1f}us n={len(sleeps)}")
    if parts:
        print("  break-wait timing: " + " ".join(parts))


def _summarize_recv_iter_timing(lines: list[str]) -> None:
    rows: list[dict[str, float]] = []
    for line in lines:
        match = RECV_ITER_TIMING_RE.search(line)
        if not match:
            continue
        fields = dict(item.split("=", 1) for item in match.group(1).split() if "=" in item)
        row = {key: float(value) for key, value in fields.items() if key != "fd"}
        if row:
            rows.append(row)

    if not rows:
        return

    def _avg(key: str) -> float | None:
        vals = [row[key] for row in rows if key in row]
        return sum(vals) / len(vals) if vals else None

    parts: list[str] = []
    for key, label in (
        ("total_us", "total"),
        ("setup_us", "setup"),
        ("marshal_cb_us", "marshal_cb"),
        ("recv_many_enter_us", "recv_many_enter"),
        ("recv_guard_us", "recv_guard"),
        ("recv_op_new_us", "recv_op_new"),
        ("recv_entry_us", "recv_entry"),
        ("submit_enter_us", "submit_enter"),
        ("ring_submit_us", "ring_submit"),
        ("submit_done_us", "submit_done"),
        ("recv_store_us", "recv_store"),
    ):
        avg = _avg(key)
        if avg is not None:
            parts.append(f"{label}={avg:.1f}us")
    if parts:
        print(f"  recv-iter timing avg ({len(rows)} buffers): " + " ".join(parts))


def _summarize_accept_path_timing(lines: list[str]) -> None:
    rows: list[dict[str, float]] = []
    for line in lines:
        match = ACCEPT_PATH_TIMING_RE.search(line)
        if not match:
            continue
        fields = dict(item.split("=", 1) for item in match.group(1).split() if "=" in item)
        row = {key: float(value) for key, value in fields.items() if key != "fd"}
        if row:
            rows.append(row)

    if not rows:
        return

    def _avg(key: str) -> float | None:
        vals = [row[key] for row in rows if key in row]
        return sum(vals) / len(vals) if vals else None

    parts: list[str] = []
    for key, label in (
        ("cqe_to_ready_us", "cqe→ready"),
        ("socket_wrap_us", "socket_wrap"),
        ("worker_enter_us", "worker_enter"),
        ("open_streams_us", "open_streams"),
        ("pooled_enter_us", "pooled_enter"),
        ("pool_enter_us", "pool_enter"),
        ("pool_create_us", "pool_create"),
        ("pool_shared_us", "pool_shared"),
        ("recv_iter_us", "recv_iter"),
        ("send_buf_us", "send_buf"),
        ("stream_objs_us", "stream_objs"),
    ):
        avg = _avg(key)
        if avg is not None:
            parts.append(f"{label}={avg:.1f}us")
    if parts:
        print(f"  accept-path timing avg ({len(rows)} accepts): " + " ".join(parts))


def _summarize_worker_completion_timing(lines: list[str]) -> None:
    """Print the last progress/final worker-completion summary line if present."""

    last: str | None = None
    for line in lines:
        match = WORKER_COMPLETION_TIMING_RE.search(line)
        if match:
            last = match.group(1)
    if last is not None:
        print(f"  worker-completion timing: {last}")


def _summarize_handler_tealet_timing(lines: list[str]) -> None:
    last: str | None = None
    for line in lines:
        match = HANDLER_TEALET_TIMING_RE.search(line)
        if match:
            last = match.group(1)
    if last is not None:
        print(f"  handler-tealet timing: {last}")


def _summarize_event_wakeup_timing(lines: list[str]) -> None:
    waits: list[float] = []
    for line in lines:
        match = EVENT_WAKEUP_TIMING_RE.match(line)
        if not match:
            continue
        event, tail = match.group(1), match.group(2)
        fields = dict(item.split("=", 1) for item in tail.split() if "=" in item)
        if event == "wait_return" and fields.get("woke") == "1" and "since_signal_us" in fields:
            waits.append(float(fields["since_signal_us"]))
    if waits:
        print(
            "  event-wakeup timing: "
            f"set_to_wait_return avg={sum(waits) / len(waits):.1f}us n={len(waits)}"
        )


def _summarize_accept_diag(lines: list[str]) -> None:
    rows: list[dict[str, float]] = []
    current: dict[str, float] = {}
    for line in lines:
        match = ACCEPT_DIAG_RE.match(line)
        if not match:
            continue
        name, tail = match.group(1), match.group(2)
        fields = dict(item.split("=", 1) for item in tail.split() if "=" in item)
        if name == "accept_worker":
            if current:
                rows.append(current)
            current = {}
            continue
        for key in ("open_streams_ms", "since_open_ms", "marshal_queue_ms"):
            if key in fields:
                current[key] = float(fields[key])
    if current:
        rows.append(current)

    if not rows:
        return

    def _avg(key: str) -> float | None:
        vals = [row[key] for row in rows if key in row]
        return sum(vals) / len(vals) if vals else None

    parts: list[str] = []
    for key, label in (
        ("open_streams_ms", "open_streams"),
        ("since_open_ms", "pre_marshal"),
        ("marshal_queue_ms", "marshal_queue"),
    ):
        avg = _avg(key)
        if avg is not None:
            parts.append(f"{label}={avg:.3f}ms")
    if parts:
        print(f"  accept delivery avg ({len(rows)} accepts): " + " ".join(parts))


def _collect_profiles(lines: list[str]) -> list[dict[str, Any]]:
    profiles: list[dict[str, Any]] = []
    for line in lines:
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


def _run_case(
    label: str,
    server: str,
    host: str,
    port: int,
    extra_args: list[str],
    *,
    concurrent: int,
    diag: bool,
    env_extra: dict[str, str] | None = None,
) -> None:
    url = f"http://{host}:{port}/"
    proc = _start_server(server, host, port, extra_args, diag=diag, env_extra=env_extra)
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
        stderr_lines = _read_stderr_lines(proc)
        _summarize_break_wait_timing(stderr_lines)
        _summarize_event_wakeup_timing(stderr_lines)
        _summarize_worker_completion_timing(stderr_lines)
        _summarize_handler_tealet_timing(stderr_lines)
        _summarize_accept_path_timing(stderr_lines)
        _summarize_recv_iter_timing(stderr_lines)
        if diag:
            _summarize_accept_diag(stderr_lines)
        profiles = _collect_profiles(stderr_lines)

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
    parser.add_argument(
        "--diag",
        action="store_true",
        help="enable TEALETIO_STREAM_DIAG accept-delivery breakdown on tealetio servers",
    )
    parser.add_argument(
        "--break-wait-timing",
        action="store_true",
        help="set TEALETIO_BREAK_WAIT_TIMING=1 on tealetio servers",
    )
    parser.add_argument(
        "--wakeup-manager",
        choices=("event", "token"),
        help="set TEALETIO_WAKEUP_MANAGER for uring proactor servers",
    )
    parser.add_argument(
        "--compare-wakeup",
        action="store_true",
        help="run tealetio-uring twice with event and token wakeup managers",
    )
    args = parser.parse_args()

    cases: dict[str, tuple[str, list[str]]] = {
        "asyncio": ("asyncio_std", []),
        "tealetio-default": ("tealetio_sync", []),
        "tealetio-selector": ("tealetio_sync", ["--proactor", "selector"]),
        "tealetio-uring": ("tealetio_sync", ["--proactor", "uring"]),
    }

    if args.compare_wakeup:
        selected_cases = ["tealetio-uring-event", "tealetio-uring-token"]
    else:
        selected_cases = list(args.cases)

    port = args.port
    for case in selected_cases:
        server, extra = cases.get(case, cases["tealetio-uring"])
        env_extra: dict[str, str] = {}
        if args.break_wait_timing or args.compare_wakeup:
            env_extra["TEALETIO_BREAK_WAIT_TIMING"] = "1"
        wakeup_manager = args.wakeup_manager
        label = case
        if case == "tealetio-uring-event":
            wakeup_manager = "event"
            label = "tealetio-uring (wakeup=event)"
        elif case == "tealetio-uring-token":
            wakeup_manager = "token"
            label = "tealetio-uring (wakeup=token)"
        if wakeup_manager is not None:
            env_extra["TEALETIO_WAKEUP_MANAGER"] = wakeup_manager
        _run_case(
            label,
            server,
            args.host,
            port,
            extra,
            concurrent=args.concurrent,
            diag=args.diag,
            env_extra=env_extra or None,
        )
        port += 1


if __name__ == "__main__":
    main()

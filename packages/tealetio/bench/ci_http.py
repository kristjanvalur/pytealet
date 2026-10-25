#!/usr/bin/env python3
"""Run one pinned tealetio HTTP case and append Bencher Metric Format JSON.

The workflow runs this twice: CPython 3.15 with inline ``uring-sync``, then
free-threaded 3.15t with two completion threads. The server uses the
proactor default ring. Each case discards a warmup
window, then keeps the wrk windows that look like a real sample. The uploaded
throughput is the median requests/sec. ``p99_ms`` is the median p99 in
milliseconds. ``lower_value`` and ``upper_value`` are the min and max of the
kept windows.

A window is dropped when wrk reports socket errors, a non-2xx response, no
completed requests, or a p99 of a second or more. Those tails are a client
artefact on this bench, not a server rate.
"""

from __future__ import annotations

import argparse
import json
import re
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

_BENCH_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _BENCH_DIR.parent.parent.parent

P99_OUTLIER_MS = 1000.0

REQ_SEC_RE = re.compile(r"Requests/sec:\s+([\d.]+)")
P99_RE = re.compile(r"99%\s+([\d.]+)(us|ms|s)")
SOCKET_ERR_RE = re.compile(r"Socket errors: connect (\d+), read (\d+), write (\d+), timeout (\d+)")
NON_2XX_RE = re.compile(r"Non-2xx or 3xx responses:\s+(\d+)")


@dataclass(frozen=True)
class Case:
    name: str
    free_threaded: bool
    proactor: str
    completion_threads: int | None = None


CASES = {
    "http-sync-3.15": Case("http-sync-3.15", free_threaded=False, proactor="uring-sync"),
    "http-threaded-3.15t": Case(
        "http-threaded-3.15t",
        free_threaded=True,
        proactor="uring",
        completion_threads=2,
    ),
}


def latency_ms(value: float, unit: str) -> float:
    if unit == "us":
        return value / 1000.0
    if unit == "ms":
        return value
    if unit == "s":
        return value * 1000.0
    raise ValueError(f"unknown wrk latency unit {unit!r}")


def parse_wrk_output(text: str) -> dict[str, float]:
    """Pull requests/sec, p99, and error counts out of a wrk report."""

    req_match = REQ_SEC_RE.search(text)
    p99_match = P99_RE.search(text)
    if req_match is None or p99_match is None:
        raise ValueError("wrk report is missing Requests/sec or the 99% latency line")
    socket_match = SOCKET_ERR_RE.search(text)
    socket_errors = 0.0
    if socket_match:
        socket_errors = float(sum(int(part) for part in socket_match.groups()))
    non_2xx_match = NON_2XX_RE.search(text)
    non_2xx = float(non_2xx_match.group(1)) if non_2xx_match else 0.0
    return {
        "req_sec": float(req_match.group(1)),
        "p99_ms": latency_ms(float(p99_match.group(1)), p99_match.group(2)),
        "socket_errors": socket_errors,
        "non_2xx": non_2xx,
    }


def reject_reason(sample: dict[str, float]) -> str | None:
    if sample["socket_errors"]:
        return f"socket errors ({int(sample['socket_errors'])})"
    if sample["non_2xx"]:
        return f"non-2xx responses ({int(sample['non_2xx'])})"
    if sample["req_sec"] <= 0:
        return "no completed requests"
    if sample["p99_ms"] >= P99_OUTLIER_MS:
        return f"p99 {sample['p99_ms']:.0f} ms"
    return None


def median(values: list[float]) -> float:
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2.0


def measures_for(samples: list[dict[str, float]]) -> dict[str, dict[str, float]]:
    rates = [sample["req_sec"] for sample in samples]
    p99s = [sample["p99_ms"] for sample in samples]
    return {
        "throughput": {"value": median(rates), "lower_value": min(rates), "upper_value": max(rates)},
        "p99_ms": {"value": median(p99s), "lower_value": min(p99s), "upper_value": max(p99s)},
    }


def format_summary(data: dict[str, dict[str, dict[str, float]]]) -> str:
    lines = [
        "## tealetio HTTP bench",
        "",
        "| benchmark | req/s | window range | p99 ms |",
        "| --- | ---: | ---: | ---: |",
    ]
    for name in sorted(data):
        throughput = data[name]["throughput"]
        p99 = data[name]["p99_ms"]
        lines.append(
            f"| `{name}` | {throughput['value']:.0f} | "
            f"{throughput['lower_value']:.0f}–{throughput['upper_value']:.0f} | {p99['value']:.1f} |"
        )
    lines.append("")
    lines.append(
        "req/s is the median of the kept wrk windows. The range is their min and max. "
        "p99 is the median 99th percentile, in milliseconds."
    )
    return "\n".join(lines) + "\n"


def _append_result(path: Path, name: str, measures: dict[str, dict[str, float]]) -> None:
    existing: dict[str, dict[str, dict[str, float]]] = {}
    if path.exists():
        existing = json.loads(path.read_text())
    existing[name] = measures
    path.write_text(json.dumps(existing, indent=2) + "\n")


def _interpreter_error(case: Case) -> str | None:
    if (sys.version_info.major, sys.version_info.minor) != (3, 15):
        return f"{case.name} must run on CPython 3.15, this is {sys.version.split()[0]}"
    gil_enabled = getattr(sys, "_is_gil_enabled", None)
    free_threaded = gil_enabled is not None and not gil_enabled()
    if free_threaded != case.free_threaded:
        kind = "free-threaded" if free_threaded else "GIL"
        want = "free-threaded" if case.free_threaded else "GIL"
        return f"{case.name} must run on {want} CPython 3.15, this build is {kind}"
    return None


def _wait_listen(host: str, port: int, proc: subprocess.Popen[bytes], timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"server exited ({proc.returncode}) before accepting connections")
        try:
            with socket.create_connection((host, port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise TimeoutError(f"server did not accept connections on {host}:{port}")


def _server_cmd(case: Case, host: str, port: int) -> list[str]:
    cmd = [
        sys.executable,
        str(_BENCH_DIR / "servers" / "tealetio_sync.py"),
        "--host",
        host,
        "--port",
        str(port),
        "--proactor",
        case.proactor,
    ]
    if case.completion_threads is not None:
        cmd.extend(["--completion-threads", str(case.completion_threads)])
    return cmd


def _run_wrk(host: str, port: int, *, threads: int, connections: int, duration: str) -> str:
    url = f"http://{host}:{port}/"
    proc = subprocess.run(
        ["wrk", f"-t{threads}", f"-c{connections}", f"-d{duration}", "--timeout", "10s", "--latency", url],
        check=False,
        capture_output=True,
        text=True,
    )
    text = proc.stdout
    if proc.stderr:
        text = f"{text}\n{proc.stderr}"
    if proc.returncode != 0 and "Requests/sec:" not in text:
        raise RuntimeError(f"wrk exited {proc.returncode}\n{text}")
    return text


def _print_log(log_path: Path) -> None:
    if not log_path.exists():
        return
    text = log_path.read_text(errors="replace")
    lines = text.splitlines()
    if len(lines) > 80:
        lines = ["... log truncated ...", *lines[-80:]]
    print(f"--- {log_path.name} ---")
    print("\n".join(lines))


def run_case(
    case: Case,
    out: Path,
    *,
    host: str,
    port: int,
    threads: int,
    connections: int,
    warmup: str,
    duration: str,
    runs: int,
) -> None:
    mismatch = _interpreter_error(case)
    if mismatch:
        raise SystemExit(mismatch)
    import uring_api

    if not uring_api.is_available():
        detail = getattr(uring_api, "_native_import_error", None)
        raise SystemExit(f"io_uring is not available ({detail})")

    log_path = out.with_name(f"{out.stem}.{case.name}.log")
    cmd = _server_cmd(case, host, port)
    print(" ".join(cmd), flush=True)
    version = " ".join(sys.version.splitlines())
    print(f"python {version}", flush=True)
    with log_path.open("w") as log:
        proc = subprocess.Popen(cmd, cwd=_REPO_ROOT, stdout=log, stderr=subprocess.STDOUT)
        try:
            _wait_listen(host, port, proc)
            print(f"warmup {warmup}", flush=True)
            _run_wrk(host, port, threads=threads, connections=connections, duration=warmup)
            kept: list[dict[str, float]] = []
            reasons: list[str] = []
            for index in range(runs):
                text = _run_wrk(host, port, threads=threads, connections=connections, duration=duration)
                print(text, flush=True)
                try:
                    sample = parse_wrk_output(text)
                except ValueError as exc:
                    reasons.append(f"window {index + 1}: {exc}")
                    continue
                reason = reject_reason(sample)
                if reason:
                    reasons.append(f"window {index + 1}: {reason}")
                    continue
                kept.append(sample)
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
    _print_log(log_path)
    for reason in reasons:
        print(f"dropped {reason}", flush=True)
    if not kept:
        raise SystemExit(f"{case.name}: no wrk window was usable")
    measures = measures_for(kept)
    _append_result(out, case.name, measures)
    rate = measures["throughput"]["value"]
    p99 = measures["p99_ms"]["value"]
    print(f"{case.name}: {rate:.0f} req/s, p99 {p99:.1f} ms, kept {len(kept)}/{runs}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", choices=sorted(CASES))
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--summarize", action="store_true", help="print a markdown table for the JSON file and exit")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--connections", type=int, default=256)
    parser.add_argument("--warmup", default="5s")
    parser.add_argument("--duration", default="10s")
    parser.add_argument("--runs", type=int, default=3)
    args = parser.parse_args()
    if args.summarize:
        data = json.loads(args.out.read_text())
        sys.stdout.write(format_summary(data))
        return
    if args.case is None:
        parser.error("--case is required unless --summarize is set")
    run_case(
        CASES[args.case],
        args.out,
        host=args.host,
        port=args.port,
        threads=args.threads,
        connections=max(args.connections, args.threads),
        warmup=args.warmup,
        duration=args.duration,
        runs=args.runs,
    )


if __name__ == "__main__":
    main()

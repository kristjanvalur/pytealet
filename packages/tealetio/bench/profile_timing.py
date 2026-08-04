"""Per-request phase timing for the connection-handler tealet.

Covers wall time on the scheduler thread from handler entry (streams already
open) through body work and ``wait_closed`` shutdown — not uring worker CQEs.

Enable via server ``--profile``. Under load, set ``TEALETIO_HANDLER_PROFILE_SAMPLE``
(default 64) to sample every Nth request after the first warmup skip and print
aggregate averages as ``[handler-tealet-timing]``.
"""

from __future__ import annotations

import json
import os
import socket
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Any, TextIO


_STREAM_OPEN_TIMES: dict[int, float] = {}


def stamp_stream_open(sock: socket.socket) -> None:
    _STREAM_OPEN_TIMES[sock.fileno()] = time.perf_counter()


def pre_handler_ms(sock: socket.socket) -> float | None:
    """Milliseconds from stream open (worker) to handler entry (scheduler).

    This is *before* the handler tealet body; includes marshal queue + spawn.
    """

    opened = _STREAM_OPEN_TIMES.pop(sock.fileno(), None)
    if opened is None:
        return None
    return (time.perf_counter() - opened) * 1000.0


def _sample_period() -> int:
    raw = os.environ.get("TEALETIO_HANDLER_PROFILE_SAMPLE", "64")
    try:
        value = int(raw)
    except ValueError:
        return 64
    return max(1, value)


@dataclass
class PhaseTimer:
    """Accumulate named phases for one HTTP request on the handler tealet."""

    backend: str
    req_num: int
    _t0: float = field(default_factory=time.perf_counter)
    _last: float = field(default_factory=time.perf_counter)
    phases: list[dict[str, Any]] = field(default_factory=list)
    readline_calls: int = 0
    readline_wait_s: float = 0.0
    readline_bytes: int = 0
    # first readline often parks on PulseEvent; later lines may hit buffered data
    first_readline_ms: float = 0.0
    later_readline_ms: float = 0.0

    def mark(self, name: str, **extra: Any) -> None:
        now = time.perf_counter()
        self.phases.append(
            {
                "phase": name,
                "since_start_ms": (now - self._t0) * 1000.0,
                "delta_ms": (now - self._last) * 1000.0,
                **extra,
            }
        )
        self._last = now

    def add_readline(self, wait_s: float, nbytes: int) -> None:
        self.readline_calls += 1
        self.readline_wait_s += wait_s
        self.readline_bytes += nbytes
        wait_ms = wait_s * 1000.0
        if self.readline_calls == 1:
            self.first_readline_ms = wait_ms
        else:
            self.later_readline_ms += wait_ms

    def finish(self, stream: TextIO | None = None) -> None:
        out = stream or sys.stderr
        total_ms = (time.perf_counter() - self._t0) * 1000.0
        payload = {
            "backend": self.backend,
            "req": self.req_num,
            "total_ms": total_ms,
            "phases": self.phases,
            "readline_calls": self.readline_calls,
            "readline_wait_ms": self.readline_wait_s * 1000.0,
            "readline_bytes": self.readline_bytes,
            "first_readline_ms": self.first_readline_ms,
            "later_readline_ms": self.later_readline_ms,
        }
        print(f"PROFILE {json.dumps(payload, separators=(',', ':'))}", file=out, flush=True)
        _HandlerAggregate.record(payload)


class _HandlerAggregate:
    """Rolling averages for sampled handler-tealet profiles under wrk."""

    _lock = threading.Lock()
    _count = 0
    _sums: dict[str, float] = {}
    _phase_keys = (
        "pre_handler_ms",
        "total_ms",
        "drain_ms",
        "write_ms",
        "drain_out_ms",
        "close_ms",
        "flush_ms",
        "sock_close_ms",
        "readline_wait_ms",
        "first_readline_ms",
        "later_readline_ms",
        "readline_calls",
        "readline_bytes",
    )
    _summary_every = 32

    @classmethod
    def record(cls, payload: dict[str, Any]) -> None:
        phase_delta = {ph["phase"]: float(ph["delta_ms"]) for ph in payload.get("phases", [])}
        pre = next(
            (float(ph["pre_handler_ms"]) for ph in payload.get("phases", []) if ph.get("pre_handler_ms") is not None),
            None,
        )
        row = {
            "pre_handler_ms": pre if pre is not None else 0.0,
            "total_ms": float(payload["total_ms"]),
            "drain_ms": phase_delta.get("drain", 0.0),
            "write_ms": phase_delta.get("write", 0.0),
            "drain_out_ms": phase_delta.get("drain_out", 0.0),
            "close_ms": phase_delta.get("close", 0.0),
            "flush_ms": phase_delta.get("flush", 0.0),
            "sock_close_ms": phase_delta.get("sock_close", 0.0),
            "readline_wait_ms": float(payload.get("readline_wait_ms", 0.0)),
            "first_readline_ms": float(payload.get("first_readline_ms", 0.0)),
            "later_readline_ms": float(payload.get("later_readline_ms", 0.0)),
            "readline_calls": float(payload.get("readline_calls", 0.0)),
            "readline_bytes": float(payload.get("readline_bytes", 0.0)),
        }
        with cls._lock:
            cls._count += 1
            for key, value in row.items():
                cls._sums[key] = cls._sums.get(key, 0.0) + value
            if cls._count % cls._summary_every == 0:
                cls._print_locked(final=False)

    @classmethod
    def dump(cls, *, final: bool = False) -> None:
        with cls._lock:
            if cls._count == 0:
                return
            cls._print_locked(final=final)

    @classmethod
    def _print_locked(cls, *, final: bool) -> None:
        n = cls._count
        inv = 1.0 / n
        tag = "final" if final else "progress"
        parts = [tag, f"n={n}"]
        for key in cls._phase_keys:
            if key not in cls._sums:
                continue
            parts.append(f"{key}={cls._sums[key] * inv:.3f}")
        print(f"[handler-tealet-timing] {' '.join(parts)}", file=sys.stderr, flush=True)


def drain_request_profile(reader: Any, timer: PhaseTimer) -> None:
    """Discard HTTP headers, recording each readline wait on the handler tealet."""

    while True:
        t0 = time.perf_counter()
        line = reader.readline()
        timer.add_readline(time.perf_counter() - t0, len(line) if line else 0)
        if not line or line in (b"\r\n", b"\n"):
            break


async def drain_request_async_profile(reader: Any, timer: PhaseTimer) -> None:
    while True:
        t0 = time.perf_counter()
        line = await reader.readline()
        timer.add_readline(time.perf_counter() - t0, len(line) if line else 0)
        if not line or line in (b"\r\n", b"\n"):
            break


class HandlerProfileGate:
    """Decide which requests get a PhaseTimer under load."""

    def __init__(self) -> None:
        self._seq = 0
        self._period = _sample_period()

    def next_req_num(self) -> int | None:
        """Return a profile req number, or None to skip this request.

        Skips the first accept (listen probe / warmup), then samples every
        ``TEALETIO_HANDLER_PROFILE_SAMPLE`` requests.
        """

        self._seq += 1
        if self._seq == 1:
            return None
        # seq 2 is first real request -> req_num 1
        req_num = self._seq - 1
        if req_num == 1 or req_num % self._period == 0:
            return req_num
        return None

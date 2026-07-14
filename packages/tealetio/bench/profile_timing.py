"""Per-request phase timing for bench profile runs."""

from __future__ import annotations

import json
import socket
import sys
import time
from dataclasses import dataclass, field
from typing import Any, TextIO


_STREAM_OPEN_TIMES: dict[int, float] = {}


def stamp_stream_open(sock: socket.socket) -> None:
    _STREAM_OPEN_TIMES[sock.fileno()] = time.monotonic()


def pre_handler_ms(sock: socket.socket) -> float | None:
    opened = _STREAM_OPEN_TIMES.get(sock.fileno())
    if opened is None:
        return None
    return (time.monotonic() - opened) * 1000.0


@dataclass
class PhaseTimer:
    """Accumulate named phases for one HTTP request."""

    backend: str
    req_num: int
    _t0: float = field(default_factory=time.monotonic)
    _last: float = field(default_factory=time.monotonic)
    phases: list[dict[str, Any]] = field(default_factory=list)
    readline_calls: int = 0
    readline_wait_s: float = 0.0
    readline_bytes: int = 0

    def mark(self, name: str, **extra: Any) -> None:
        now = time.monotonic()
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

    def finish(self, stream: TextIO | None = None) -> None:
        out = stream or sys.stderr
        payload = {
            "backend": self.backend,
            "req": self.req_num,
            "total_ms": (time.monotonic() - self._t0) * 1000.0,
            "phases": self.phases,
            "readline_calls": self.readline_calls,
            "readline_wait_ms": self.readline_wait_s * 1000.0,
            "readline_bytes": self.readline_bytes,
        }
        print(f"PROFILE {json.dumps(payload, separators=(',', ':'))}", file=out, flush=True)


def drain_request_profile(reader: Any, timer: PhaseTimer) -> None:
    """Discard HTTP headers, recording each readline wait."""

    while True:
        t0 = time.monotonic()
        line = reader.readline()
        timer.add_readline(time.monotonic() - t0, len(line) if line else 0)
        if not line or line in (b"\r\n", b"\n"):
            break


async def drain_request_async_profile(reader: Any, timer: PhaseTimer) -> None:
    while True:
        t0 = time.monotonic()
        line = await reader.readline()
        timer.add_readline(time.monotonic() - t0, len(line) if line else 0)
        if not line or line in (b"\r\n", b"\n"):
            break
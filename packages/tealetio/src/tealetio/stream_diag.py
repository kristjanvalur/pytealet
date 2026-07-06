"""Opt-in stream/server diagnostics.

Enable with ``TEALETIO_STREAM_DIAG=1``. Events go to stderr with monotonic
timestamps so bench runs can be correlated with wrk load.
"""

from __future__ import annotations

import os
import sys
import threading
import time
from collections import Counter

def enabled() -> bool:
    return os.environ.get("TEALETIO_STREAM_DIAG", "").lower() in ("1", "true", "yes")


def uring_accept_enabled() -> bool:
    """Broader gate for multishot-accept CQE tracing."""

    if enabled():
        return True
    return os.environ.get("TEALETIO_URING_ACCEPT_LOG", "").lower() in ("1", "true", "yes")


def _thread_label() -> str:
    current = threading.current_thread()
    return current.name or f"tid-{threading.get_ident()}"


class _StreamDiag:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: Counter[str] = Counter()
        self._last_event = 0.0
        self._blocking: dict[int, tuple[str, float, str]] = {}

    def event(self, name: str, **fields: object) -> None:
        if not enabled():
            return
        now = time.monotonic()
        with self._lock:
            self._counters[name] += 1
            self._last_event = now
            count = self._counters[name]
        parts = " ".join(f"{key}={value}" for key, value in fields.items())
        suffix = f" {parts}" if parts else ""
        print(
            f"[stream-diag {now:.3f} {_thread_label()}] {name} #{count}{suffix}",
            file=sys.stderr,
            flush=True,
        )

    def block_enter(self, site: str, **fields: object) -> None:
        if not enabled():
            return
        now = time.monotonic()
        ident = threading.get_ident()
        detail = " ".join(f"{key}={value}" for key, value in fields.items())
        with self._lock:
            self._blocking[ident] = (site, now, detail)
            self._last_event = now
        parts = f" {detail}" if detail else ""
        print(
            f"[stream-diag {now:.3f} {_thread_label()}] BLOCK {site}{parts}",
            file=sys.stderr,
            flush=True,
        )

    def block_exit(self, site: str) -> None:
        if not enabled():
            return
        now = time.monotonic()
        ident = threading.get_ident()
        with self._lock:
            entry = self._blocking.pop(ident, None)
            self._last_event = now
        waited = ""
        if entry is not None and entry[0] == site:
            waited = f" waited={now - entry[1]:.3f}s"
        print(
            f"[stream-diag {now:.3f} {_thread_label()}] UNBLOCK {site}{waited}",
            file=sys.stderr,
            flush=True,
        )

    def snapshot(self) -> tuple[float, dict[str, int], list[tuple[int, str, float, str]]]:
        with self._lock:
            now = time.monotonic()
            idle = now - self._last_event
            counters = dict(self._counters)
            blocking = [
                (ident, site, now - started, detail)
                for ident, (site, started, detail) in self._blocking.items()
            ]
        return idle, counters, blocking

    def total_events(self) -> int:
        with self._lock:
            return sum(self._counters.values())


_diag = _StreamDiag()

def uring_accept_cqe(**fields: object) -> None:
    """Log one multishot-accept completion (terminal CQEs, errors)."""

    if not uring_accept_enabled():
        return
    _diag.event("uring_accept_multishot_cqe", **fields)


event = _diag.event
block_enter = _diag.block_enter
block_exit = _diag.block_exit
snapshot = _diag.snapshot
total_events = _diag.total_events
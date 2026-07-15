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


class _AcceptDeliveryTiming:
    """Per-fd monotonic stamps for accept→handler latency breakdown."""

    def __init__(self) -> None:
        self._worker_at: dict[int, float] = {}
        self._open_at: dict[int, float] = {}
        self._marshal_at: dict[int, float] = {}

    def worker_conn(self, fd: int) -> None:
        self._worker_at[fd] = time.monotonic()

    def streams_opened(self, fd: int) -> float | None:
        now = time.monotonic()
        self._open_at[fd] = now
        started = self._worker_at.get(fd)
        if started is None:
            return None
        return (now - started) * 1000.0

    def marshal(self, fd: int) -> float | None:
        now = time.monotonic()
        self._marshal_at[fd] = now
        opened = self._open_at.get(fd)
        if opened is None:
            return None
        return (now - opened) * 1000.0

    def scheduler(self, fd: int) -> float | None:
        now = time.monotonic()
        marshalled = self._marshal_at.pop(fd, None)
        self._worker_at.pop(fd, None)
        self._open_at.pop(fd, None)
        if marshalled is None:
            return None
        return (now - marshalled) * 1000.0


class _StreamDiag:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: Counter[str] = Counter()
        self._last_event = 0.0
        self._blocking: dict[int, tuple[str, float, str]] = {}
        self._accept = _AcceptDeliveryTiming()

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
                (ident, site, now - started, detail) for ident, (site, started, detail) in self._blocking.items()
            ]
        return idle, counters, blocking

    def total_events(self) -> int:
        with self._lock:
            return sum(self._counters.values())


_diag = _StreamDiag()


def accept_worker_conn(fd: int) -> None:
    if not enabled():
        return
    _diag._accept.worker_conn(fd)
    _diag.event("accept_worker", fd=fd)


def accept_streams_opened(fd: int) -> None:
    if not enabled():
        return
    open_ms = _diag._accept.streams_opened(fd)
    fields: dict[str, object] = {"fd": fd}
    if open_ms is not None:
        fields["open_streams_ms"] = round(open_ms, 3)
    _diag.event("accept_streams_opened", **fields)


def accept_marshal(fd: int) -> None:
    if not enabled():
        return
    marshal_ms = _diag._accept.marshal(fd)
    fields: dict[str, object] = {"fd": fd}
    if marshal_ms is not None:
        fields["since_open_ms"] = round(marshal_ms, 3)
    _diag.event("accept_marshal", **fields)


def accept_scheduler(fd: int) -> None:
    if not enabled():
        return
    queue_ms = _diag._accept.scheduler(fd)
    fields: dict[str, object] = {"fd": fd}
    if queue_ms is not None:
        fields["marshal_queue_ms"] = round(queue_ms, 3)
    _diag.event("accept_scheduler", **fields)


def accept_spawn(fd: int) -> None:
    if not enabled():
        return
    _diag.event("accept_spawn", fd=fd)


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

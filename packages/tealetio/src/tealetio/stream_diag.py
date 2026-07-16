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


def _truthy(name: str) -> bool:
    return os.environ.get(name, "").lower() in ("1", "true", "yes")


def enabled() -> bool:
    return _truthy("TEALETIO_STREAM_DIAG")


def accept_path_enabled() -> bool:
    return _truthy("TEALETIO_ACCEPT_PATH_TIMING") or enabled()


def uring_accept_enabled() -> bool:
    """Broader gate for multishot-accept CQE tracing."""

    if enabled():
        return True
    return os.environ.get("TEALETIO_URING_ACCEPT_LOG", "").lower() in ("1", "true", "yes")


def _thread_label() -> str:
    current = threading.current_thread()
    return current.name or f"tid-{threading.get_ident()}"


class _AcceptPathTiming:
    """Per-fd perf-counter phases from accept CQE handler entry to streams ready."""

    _PHASES = (
        "socket_wrap",
        "worker_enter",
        "open_streams",
        "pooled_enter",
        "pool_enter",
        "pool_create",
        "pool_shared",
        "recv_iter",
        "send_buf",
        "stream_objs",
    )

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cqe_at: dict[int, float] = {}
        self._last_at: dict[int, float] = {}
        self._deltas_us: dict[int, dict[str, float]] = {}

    def begin(self, fd: int, started_at: float) -> None:
        with self._lock:
            self._cqe_at[fd] = started_at
            self._last_at[fd] = started_at
            self._deltas_us[fd] = {}

    def mark(self, fd: int, phase: str) -> None:
        now = time.perf_counter()
        with self._lock:
            last = self._last_at.get(fd)
            if last is None:
                return
            self._deltas_us.setdefault(fd, {})[phase] = (now - last) * 1_000_000.0
            self._last_at[fd] = now

    def finish(self, fd: int) -> dict[str, object] | None:
        now = time.perf_counter()
        with self._lock:
            started = self._cqe_at.pop(fd, None)
            self._last_at.pop(fd, None)
            deltas = self._deltas_us.pop(fd, {})
        if started is None:
            return None
        total_us = (now - started) * 1_000_000.0
        fields: dict[str, object] = {
            "fd": fd,
            "cqe_to_ready_us": round(total_us, 1),
        }
        for phase in self._PHASES:
            if phase in deltas:
                fields[f"{phase}_us"] = round(deltas[phase], 1)
        return fields


class _RecvIterPathTiming:
    """Per-fd perf-counter phases for ``RecvIterBuffer`` construction."""

    _PHASES = (
        "scheduler",
        "setup",
        "marshal_cb",
        "recv_many_enter",
        "recv_guard",
        "recv_op_new",
        "recv_entry",
        "submit_enter",
        "ring_submit",
        "submit_done",
        "recv_store",
    )

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._started_at: dict[int, float] = {}
        self._last_at: dict[int, float] = {}
        self._deltas_us: dict[int, dict[str, float]] = {}

    def begin(self, fd: int) -> None:
        now = time.perf_counter()
        with self._lock:
            self._started_at[fd] = now
            self._last_at[fd] = now
            self._deltas_us[fd] = {}

    def mark(self, fd: int, phase: str) -> None:
        now = time.perf_counter()
        with self._lock:
            last = self._last_at.get(fd)
            if last is None:
                return
            self._deltas_us.setdefault(fd, {})[phase] = (now - last) * 1_000_000.0
            self._last_at[fd] = now

    def finish(self, fd: int) -> dict[str, object] | None:
        now = time.perf_counter()
        with self._lock:
            started = self._started_at.pop(fd, None)
            self._last_at.pop(fd, None)
            deltas = self._deltas_us.pop(fd, {})
        if started is None:
            return None
        total_us = (now - started) * 1_000_000.0
        fields: dict[str, object] = {
            "fd": fd,
            "total_us": round(total_us, 1),
        }
        for phase in self._PHASES:
            if phase in deltas:
                fields[f"{phase}_us"] = round(deltas[phase], 1)
        return fields


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
        self._accept_path = _AcceptPathTiming()
        self._recv_iter_path = _RecvIterPathTiming()

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


def recv_iter_path_begin(fd: int) -> None:
    if not accept_path_enabled():
        return
    _diag._recv_iter_path.begin(fd)


def recv_iter_path_mark(fd: int, phase: str) -> None:
    if not accept_path_enabled():
        return
    _diag._recv_iter_path.mark(fd, phase)


def recv_iter_path_finish(fd: int) -> None:
    if not accept_path_enabled():
        return
    fields = _diag._recv_iter_path.finish(fd)
    if fields is None:
        return
    parts = " ".join(f"{key}={value}" for key, value in fields.items())
    with _diag._lock:
        _diag._last_event = time.monotonic()
    print(f"[recv-iter-timing] ready {parts}", file=sys.stderr, flush=True)


def accept_path_begin(fd: int, started_at: float) -> None:
    if not accept_path_enabled():
        return
    _diag._accept_path.begin(fd, started_at)


def accept_path_mark(fd: int, phase: str) -> None:
    if not accept_path_enabled():
        return
    _diag._accept_path.mark(fd, phase)


def accept_path_finish(fd: int) -> None:
    if not accept_path_enabled():
        return
    fields = _diag._accept_path.finish(fd)
    if fields is None:
        return
    parts = " ".join(f"{key}={value}" for key, value in fields.items())
    with _diag._lock:
        _diag._last_event = time.monotonic()
    print(f"[accept-path-timing] ready {parts}", file=sys.stderr, flush=True)


def accept_worker_conn(fd: int) -> None:
    if not enabled():
        if accept_path_enabled():
            accept_path_mark(fd, "worker_enter")
        return
    _diag._accept.worker_conn(fd)
    accept_path_mark(fd, "worker_enter")
    _diag.event("accept_worker", fd=fd)


def accept_streams_opened(fd: int) -> None:
    accept_path_finish(fd)
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

"""Break-wait handoff and optional experiment timing."""

from __future__ import annotations

import os
import sys
import threading
import time
from typing import Protocol


def _truthy(name: str) -> bool:
    return os.environ.get(name, "").lower() in ("1", "true", "yes")


# Cache at import — break-wait hooks sit on the selector wake path.
_BREAK_WAIT_SLEEP = _truthy("TEALETIO_BREAK_WAIT_SLEEP")
_BREAK_WAIT_TIMING = _truthy("TEALETIO_BREAK_WAIT_TIMING") or _BREAK_WAIT_SLEEP
_EVENT_WAKEUP_TIMING = _truthy("TEALETIO_EVENT_WAKEUP_TIMING") or _BREAK_WAIT_TIMING

_timing_log_lock = threading.Lock()


def _timing_log(prefix: str, event: str, **fields: object) -> None:
    parts = " ".join(f"{key}={value}" for key, value in fields.items())
    line = f"{prefix} {event} {parts}\n"
    with _timing_log_lock:
        sys.stderr.write(line)
        sys.stderr.flush()


def break_wait_sleep_enabled() -> bool:
    return _BREAK_WAIT_SLEEP


def break_wait_timing_enabled() -> bool:
    return _BREAK_WAIT_TIMING


def event_wakeup_timing_enabled() -> bool:
    return _EVENT_WAKEUP_TIMING

class WakeupManager(Protocol):
    """Cross-thread wakeup primitive for proactor ``wait`` / ``wait_async``."""

    def wakeup(self) -> None:
        """Signal a pending wakeup, optionally blocking until a waiter claims it."""

    def wait(self, timeout: float | None = None) -> bool:
        """Block until ``wakeup()`` or ``timeout`` elapses."""

    def poll(self) -> bool:
        """Return whether a wakeup is pending, consuming it when true."""


class TokenHandoffWakeupManager:
    """Token-based wakeup with signaller handoff.

    One consumer at a time: either a thread in ``wait()`` or ``poll()``, never
    both concurrently on the same instance. Multiple signaller threads may call
    ``wakeup()``; they coalesce on one token at a time and, when a blocking
    waiter is present, block until that waiter claims the token.

    When no thread is in ``wait()``, ``wakeup()`` latches the next token and
    returns immediately. ``poll()`` is the non-blocking consume path for
    ``wait_async``; ``wait()`` is the blocking path used by synchronous drivers
    and enables signaller handoff.
    """

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._waiting = False
        self._issued_token = 0
        self._received_token = 0

    def wakeup(self) -> None:
        """Issue a wakeup token, or join handoff until the waiter claims it."""

        with self._cond:
            if self._issued_token == self._received_token:
                self._issued_token += 1
                if self._waiting:
                    self._cond.notify_all()
            if not self._waiting:
                return
            issued_token = self._issued_token
            self._cond.wait_for(lambda: self._received_token >= issued_token)

    def wait(self, timeout: float | None = None) -> bool:
        """Block until a latched or newly issued token is claimed."""

        with self._cond:
            self._waiting = True
            try:
                issued = self._cond.wait_for(lambda: self._issued_token > self._received_token, timeout=timeout)
                if issued:
                    self._received_token = self._issued_token
                    self._cond.notify_all()
                    return True
                return False
            finally:
                self._waiting = False

    def poll(self) -> bool:
        """Return whether a wakeup token is pending, consuming it when true."""

        with self._cond:
            result = self._issued_token > self._received_token
            if result:
                self._received_token = self._issued_token
            return result


class EventWakeupManager:
    """``threading.Event``-backed wakeup without signaller handoff.

    ``wakeup()`` returns immediately. Use ``wait()`` for blocking synchronous
    waits and ``poll()`` for the non-blocking consume path in ``wait_async``.
    """

    def __init__(self) -> None:
        self._event = threading.Event()

    def wait(self, timeout: float | None = None) -> bool:
        """Block until ``wakeup()`` or ``timeout`` elapses."""

        woke = self._event.wait(timeout=timeout)
        if woke:
            self._event.clear()
            note_event_wakeup_wake(woke=True)
        return woke

    def wakeup(self) -> None:
        """Wake a sleeping waiter or latch until ``wait()`` / ``poll()``."""

        self._event.set()
        note_event_wakeup_signal()

    def poll(self) -> bool:
        """Return whether a wakeup is pending, consuming it when true."""

        result = self._event.is_set()
        if result:
            self._event.clear()
        return result


def create_wakeup_manager() -> WakeupManager:
    """Build the configured wakeup manager for ``UringProactor``.

    ``TEALETIO_WAKEUP_MANAGER`` selects the implementation:

    - ``event`` (default): :class:`EventWakeupManager`
    - ``token``: :class:`TokenHandoffWakeupManager`
    """

    kind = os.environ.get("TEALETIO_WAKEUP_MANAGER", "event").strip().lower()
    if kind in ("event", ""):
        return EventWakeupManager()
    if kind in ("token", "handoff", "token_handoff"):
        return TokenHandoffWakeupManager()
    raise ValueError(f"unsupported TEALETIO_WAKEUP_MANAGER={kind!r}; use 'event' or 'token'")


class _BreakWaitTiming:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._signal_at = 0.0
        self._signal_tid = 0
        self._signal_site = ""
        self._seq = 0

    def note_signal(self, site: str) -> int:
        now = time.perf_counter()
        with self._lock:
            self._seq += 1
            seq = self._seq
            self._signal_at = now
            self._signal_tid = threading.get_ident()
            self._signal_site = site
        return seq

    def signal_snapshot(self) -> tuple[int, float, int, str]:
        with self._lock:
            return self._seq, self._signal_at, self._signal_tid, self._signal_site

    def _log(self, event: str, **fields: object) -> None:
        if break_wait_timing_enabled():
            _timing_log("[break-wait-timing]", event, **fields)


_timing = _BreakWaitTiming()


class _EventWakeupTiming:
    """Perf-counter stamps for ``threading.Event`` set → wait return."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._signal_at = 0.0
        self._signal_tid = 0
        self._seq = 0

    def note_signal(self) -> int:
        now = time.perf_counter()
        with self._lock:
            self._seq += 1
            seq = self._seq
            self._signal_at = now
            self._signal_tid = threading.get_ident()
        return seq

    def note_wake(self, *, woke: bool) -> tuple[int, float | None]:
        now = time.perf_counter()
        with self._lock:
            seq = self._seq
            signal_at = self._signal_at
            signal_tid = self._signal_tid
        since_signal_us = (now - signal_at) * 1_000_000.0 if woke and signal_at else None
        return seq, since_signal_us

    def _log(self, event: str, **fields: object) -> None:
        if event_wakeup_timing_enabled():
            _timing_log("[event-wakeup-timing]", event, **fields)


_event_wakeup_timing = _EventWakeupTiming()


def note_event_wakeup_signal() -> None:
    """Record monotonic time immediately after ``threading.Event.set()``."""

    seq = _event_wakeup_timing.note_signal()
    if event_wakeup_timing_enabled():
        _event_wakeup_timing._log(
            "signal",
            seq=seq,
            signal_tid=threading.get_ident(),
        )


def note_event_wakeup_wake(*, woke: bool) -> None:
    """Record elapsed time from the last ``set()`` to ``Event.wait()`` return."""

    if not event_wakeup_timing_enabled():
        return
    seq, since_signal_us = _event_wakeup_timing.note_wake(woke=woke)
    fields: dict[str, object] = {
        "seq": seq,
        "wait_tid": threading.get_ident(),
        "woke": int(woke),
    }
    if since_signal_us is not None:
        fields["since_signal_us"] = round(since_signal_us, 1)
    _event_wakeup_timing._log("wait_return", **fields)


def note_break_wait_signal(site: str) -> None:
    """Record monotonic time when ``wakeup()`` is entered."""

    seq = _timing.note_signal(site)
    if break_wait_timing_enabled():
        _timing._log(
            "signal",
            site=site,
            seq=seq,
            tid=threading.get_ident(),
        )


def yield_after_break_wait_wakeup(site: str) -> None:
    """Optionally ``sleep(0)`` after handoff; log how long the sleep call took."""

    if not break_wait_sleep_enabled():
        return
    sleep_start = time.perf_counter()
    time.sleep(0)
    sleep_us = (time.perf_counter() - sleep_start) * 1_000_000.0
    if break_wait_timing_enabled():
        seq, signal_at, signal_tid, signal_site = _timing.signal_snapshot()
        since_signal_us = (time.perf_counter() - signal_at) * 1_000_000.0 if signal_at else None
        fields: dict[str, object] = {
            "site": site,
            "seq": seq,
            "tid": threading.get_ident(),
            "sleep_us": round(sleep_us, 1),
        }
        if signal_site:
            fields["signal_site"] = signal_site
        if signal_tid:
            fields["signal_tid"] = signal_tid
        if since_signal_us is not None:
            fields["since_signal_us"] = round(since_signal_us, 1)
        _timing._log("sleep0_done", **fields)


def note_break_wait_handoff(site: str) -> None:
    """Log elapsed time from the last signal to ``wakeup()`` returning."""

    if not break_wait_timing_enabled():
        return
    seq, signal_at, signal_tid, signal_site = _timing.signal_snapshot()
    handoff_us = (time.perf_counter() - signal_at) * 1_000_000.0 if signal_at else None
    fields: dict[str, object] = {
        "site": site,
        "seq": seq,
        "tid": threading.get_ident(),
    }
    if signal_site:
        fields["signal_site"] = signal_site
    if signal_tid:
        fields["signal_tid"] = signal_tid
    if handoff_us is not None:
        fields["since_signal_us"] = round(handoff_us, 1)
    _timing._log("handoff_done", **fields)


def note_break_wait_wake(site: str, woke: bool) -> None:
    """Log elapsed time from the last signal to ``wait()`` returning."""

    if not break_wait_timing_enabled():
        return
    now = time.perf_counter()
    seq, signal_at, signal_tid, signal_site = _timing.signal_snapshot()
    wake_us = (now - signal_at) * 1_000_000.0 if signal_at else None
    fields: dict[str, object] = {
        "site": site,
        "seq": seq,
        "tid": threading.get_ident(),
        "woke": int(woke),
    }
    if signal_site:
        fields["signal_site"] = signal_site
    if signal_tid:
        fields["signal_tid"] = signal_tid
    if wake_us is not None:
        fields["since_signal_us"] = round(wake_us, 1)
    _timing._log("wait_return", **fields)

"""Optional yield and microtiming after break-wait wakeups (experiments)."""

from __future__ import annotations

import os
import sys
import threading
import time


def _truthy(name: str) -> bool:
    return os.environ.get(name, "").lower() in ("1", "true", "yes")


def break_wait_sleep_enabled() -> bool:
    return _truthy("TEALETIO_BREAK_WAIT_SLEEP")


def break_wait_timing_enabled() -> bool:
    return _truthy("TEALETIO_BREAK_WAIT_TIMING") or break_wait_sleep_enabled()


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
        if not break_wait_timing_enabled():
            return
        parts = " ".join(f"{key}={value}" for key, value in fields.items())
        print(f"[break-wait-timing] {event} {parts}", file=sys.stderr, flush=True)


_timing = _BreakWaitTiming()


def note_break_wait_signal(site: str) -> None:
    """Record monotonic time immediately after the wakeup primitive is signalled."""

    seq = _timing.note_signal(site)
    if break_wait_timing_enabled():
        _timing._log(
            "signal",
            site=site,
            seq=seq,
            tid=threading.get_ident(),
        )


def yield_after_break_wait_wakeup(site: str) -> None:
    """Optionally ``sleep(0)`` after signal; log how long the sleep call took."""

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


def note_break_wait_wake(site: str, woke: bool) -> None:
    """Log elapsed time from the last signal to ``Event.wait()`` returning."""

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

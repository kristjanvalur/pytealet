"""Opt-in stream/server diagnostics.

Enable with ``TEALETIO_STREAM_DIAG=1``. Events go to stderr with monotonic
timestamps so bench runs can be correlated with wrk load.

Worker-side completion timing (``TEALETIO_WORKER_COMPLETION_TIMING=1``) breaks
down uring completion-thread work per op kind: build (CQE → emit), emit
(result / done callbacks), and tail (deactivate / post-emit work).

Tealet switch microtiming (``TEALETIO_SWITCH_TIMING=1``) records pure switch-in
cost: nanoseconds from the departing tealet's ``switch()`` call until the
arriving tealet resumes after its own ``switch()`` returns. That excludes the
time other tealets spent running between parks.
"""

from __future__ import annotations

import atexit
import os
import sys
import threading
import time
from collections import Counter
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import tealet


def _truthy(name: str) -> bool:
    return os.environ.get(name, "").lower() in ("1", "true", "yes")


# Cache at import: these flags are fixed for the process. Reading os.environ on
# every switch/wake was ~10% of selector CPU under wrk.
_STREAM_DIAG = _truthy("TEALETIO_STREAM_DIAG")
_ACCEPT_PATH_TIMING = _truthy("TEALETIO_ACCEPT_PATH_TIMING") or _STREAM_DIAG
_WORKER_COMPLETION_TIMING = _truthy("TEALETIO_WORKER_COMPLETION_TIMING")
_SWITCH_TIMING = _truthy("TEALETIO_SWITCH_TIMING")
_URING_ACCEPT_LOG = _STREAM_DIAG or _truthy("TEALETIO_URING_ACCEPT_LOG")
_SCHEDULER_TIMING = _truthy("TEALETIO_SCHEDULER_TIMING")
_SPAWN_TIMING = _truthy("TEALETIO_SPAWN_TIMING")


def enabled() -> bool:
    return _STREAM_DIAG


def accept_path_enabled() -> bool:
    return _ACCEPT_PATH_TIMING


def worker_completion_enabled() -> bool:
    return _WORKER_COMPLETION_TIMING


def switch_timing_enabled() -> bool:
    return _SWITCH_TIMING


def scheduler_timing_enabled() -> bool:
    return _SCHEDULER_TIMING


def spawn_timing_enabled() -> bool:
    return _SPAWN_TIMING

def uring_accept_enabled() -> bool:
    """Broader gate for multishot-accept CQE tracing."""

    return _URING_ACCEPT_LOG


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


class _WorkerCompletionTiming:
    """Thread-local phase timer for one uring completion on a service thread.

    Phases (us, sequential deltas):
    - ``build``: CQE handler entry until result/done callbacks start
    - ``emit``: continuous result callback or oneshot done callbacks
    - ``tail``: remaining work after emit until the completion handler returns
    """

    _SUMMARY_EVERY = 256

    def __init__(self) -> None:
        self._tls = threading.local()
        self._lock = threading.Lock()
        # kind -> {count, total_us, build_us, emit_us, tail_us}
        self._stats: dict[str, list[float]] = {}
        self._since_summary = 0
        self._atexit_registered = False

    def _ensure_atexit(self) -> None:
        if self._atexit_registered:
            return
        self._atexit_registered = True
        atexit.register(self.dump_summary, final=True)

    def begin(self, kind: str) -> None:
        tls = self._tls
        tls.active = True
        tls.kind = kind
        now = time.perf_counter()
        tls.started = now
        tls.last = now
        tls.build_us = 0.0
        tls.emit_us = 0.0
        tls.saw_emit = False

    def mark_emit_start(self) -> None:
        tls = self._tls
        if not getattr(tls, "active", False):
            return
        now = time.perf_counter()
        tls.build_us = (now - tls.started) * 1_000_000.0
        tls.last = now
        tls.saw_emit = True

    def mark_emit_end(self) -> None:
        tls = self._tls
        if not getattr(tls, "active", False) or not getattr(tls, "saw_emit", False):
            return
        now = time.perf_counter()
        tls.emit_us = (now - tls.last) * 1_000_000.0
        tls.last = now

    def end(self) -> None:
        tls = self._tls
        if not getattr(tls, "active", False):
            return
        now = time.perf_counter()
        kind = tls.kind
        total_us = (now - tls.started) * 1_000_000.0
        if tls.saw_emit:
            build_us = tls.build_us
            emit_us = tls.emit_us
            tail_us = max(0.0, (now - tls.last) * 1_000_000.0)
        else:
            # no emit/finish callbacks (or empty callback list)
            build_us = total_us
            emit_us = 0.0
            tail_us = 0.0
        tls.active = False

        self._ensure_atexit()
        with self._lock:
            row = self._stats.get(kind)
            if row is None:
                row = [0.0, 0.0, 0.0, 0.0, 0.0]
                self._stats[kind] = row
            row[0] += 1.0
            row[1] += total_us
            row[2] += build_us
            row[3] += emit_us
            row[4] += tail_us
            self._since_summary += 1
            should_dump = self._since_summary >= self._SUMMARY_EVERY
            if should_dump:
                self._since_summary = 0
                snapshot = {k: list(v) for k, v in self._stats.items()}
            else:
                snapshot = None
        if snapshot is not None:
            self._print_summary(snapshot, final=False)

    def dump_summary(self, *, final: bool = False) -> None:
        with self._lock:
            if not self._stats:
                return
            snapshot = {k: list(v) for k, v in self._stats.items()}
            if final:
                self._stats.clear()
                self._since_summary = 0
        self._print_summary(snapshot, final=final)

    @staticmethod
    def _print_summary(snapshot: dict[str, list[float]], *, final: bool) -> None:
        tag = "final" if final else "progress"
        # sort by total time spent so hot paths surface first
        items = sorted(snapshot.items(), key=lambda kv: kv[1][1], reverse=True)
        parts: list[str] = [tag]
        for kind, row in items:
            count = int(row[0])
            if count <= 0:
                continue
            inv = 1.0 / count
            parts.append(
                f"{kind}:n={count}"
                f":total={row[1] * inv:.1f}"
                f":build={row[2] * inv:.1f}"
                f":emit={row[3] * inv:.1f}"
                f":tail={row[4] * inv:.1f}"
            )
        if len(parts) == 1:
            return
        print(f"[worker-completion-timing] {' '.join(parts)}", file=sys.stderr, flush=True)


class _SwitchTiming:
    """Microtiming for scheduler tealet transfers.

    *xfer* is pure switch-in cost: the waker stamps ``_xfer_depart_ns``
    immediately before transfer; the wakee samples arrive − that stamp on the
    first instruction after its own ``switch()`` / ``run()`` returns.

    Stamps must also be set on C-driven exits (``resolve_target``) and throws,
    or the next resume attributes the dying tealet's leftover work to *xfer*.

    *away* is wall time from this tealet's ``switch()``/``run()`` call until it
    resumes (other tealets' run time). Context only, not switch cost.
    """

    _SUMMARY_EVERY = 10_000
    # discard impossible samples (stale stamp / clock weirdness)
    _XFER_PLAUSIBLE_NS = 500_000  # 500 µs

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # set by waker immediately before switch; read by wakee on resume
        self._xfer_depart_ns = 0
        self._count = 0
        self._xfer_count = 0
        self._xfer_total_ns = 0
        self._xfer_min_ns = 0
        self._xfer_max_ns = 0
        self._xfer_dropped = 0
        self._away_total_ns = 0
        self._away_max_ns = 0
        self._window_start = time.perf_counter()
        self._atexit_registered = False

    def _ensure_atexit(self) -> None:
        if self._atexit_registered:
            return
        self._atexit_registered = True
        atexit.register(self.dump_summary, final=True)

    def note_depart(self) -> None:
        """Stamp departure for a transfer that does not go through ``switch()``.

        Call immediately before C-level switches from ``resolve_target`` / throw.
        """

        self._xfer_depart_ns = time.perf_counter_ns()

    def switch(self, target: tealet.tealet) -> None:
        leave = time.perf_counter_ns()
        self._xfer_depart_ns = leave
        target.switch()
        self._record_resume(leave)

    def note_run(self, target: tealet.tealet, func: object, arg: object) -> object:
        """Time ``tealet.run`` the same way as ``switch`` (eager task start)."""

        import tealet as tealet_mod

        leave = time.perf_counter_ns()
        self._xfer_depart_ns = leave
        result = tealet_mod.tealet.run(target, func, arg)
        self._record_resume(leave)
        return result

    def _record_resume(self, leave_ns: int) -> None:
        arrive = time.perf_counter_ns()
        # waker (or resolve_target/throw) stamped just before switching to us
        xfer = arrive - self._xfer_depart_ns
        away = arrive - leave_ns
        if away < 0:
            return
        xfer_ok = 0 <= xfer <= self._XFER_PLAUSIBLE_NS
        self._ensure_atexit()
        with self._lock:
            self._count += 1
            self._away_total_ns += away
            if self._count == 1:
                self._away_max_ns = away
            elif away > self._away_max_ns:
                self._away_max_ns = away
            if xfer_ok:
                self._xfer_count += 1
                self._xfer_total_ns += xfer
                if self._xfer_count == 1:
                    self._xfer_min_ns = xfer
                    self._xfer_max_ns = xfer
                else:
                    if xfer < self._xfer_min_ns:
                        self._xfer_min_ns = xfer
                    if xfer > self._xfer_max_ns:
                        self._xfer_max_ns = xfer
            else:
                self._xfer_dropped += 1
            should_dump = self._count % self._SUMMARY_EVERY == 0
            snapshot = self._snapshot_locked() if should_dump else None
        if snapshot is not None:
            self._print_summary(snapshot, final=False)

    def dump_summary(self, *, final: bool = False) -> None:
        with self._lock:
            if self._count == 0:
                return
            snapshot = self._snapshot_locked()
            if final:
                self._count = 0
                self._xfer_count = 0
                self._xfer_total_ns = 0
                self._xfer_min_ns = 0
                self._xfer_max_ns = 0
                self._xfer_dropped = 0
                self._away_total_ns = 0
                self._away_max_ns = 0
                self._window_start = time.perf_counter()
        self._print_summary(snapshot, final=final)

    def _snapshot_locked(self) -> dict[str, float | int]:
        window_s = max(time.perf_counter() - self._window_start, 1e-9)
        n = self._count
        xn = self._xfer_count
        inv = 1.0 / n if n else 0.0
        xinv = 1.0 / xn if xn else 0.0
        return {
            "n": n,
            "xfer_n": xn,
            "xfer_dropped": self._xfer_dropped,
            "xfer_avg_ns": self._xfer_total_ns * xinv,
            "xfer_min_ns": self._xfer_min_ns if xn else 0,
            "xfer_max_ns": self._xfer_max_ns if xn else 0,
            "xfer_total_us": self._xfer_total_ns / 1000.0,
            "xfer_share_pct": (self._xfer_total_ns / 1e9) / window_s * 100.0,
            "away_avg_us": self._away_total_ns * inv / 1000.0,
            "away_max_us": self._away_max_ns / 1000.0,
            "switches_per_s": n / window_s,
            "window_s": window_s,
        }

    @staticmethod
    def _print_summary(snapshot: dict[str, float | int], *, final: bool) -> None:
        tag = "final" if final else "progress"
        print(
            f"[switch-timing] {tag}"
            f" n={snapshot['n']}"
            f" xfer_n={snapshot['xfer_n']}"
            f" xfer_dropped={snapshot['xfer_dropped']}"
            f" xfer_avg_ns={snapshot['xfer_avg_ns']:.0f}"
            f" xfer_min_ns={snapshot['xfer_min_ns']}"
            f" xfer_max_ns={snapshot['xfer_max_ns']}"
            f" xfer_total_us={snapshot['xfer_total_us']:.1f}"
            f" xfer_share_pct={snapshot['xfer_share_pct']:.2f}"
            f" away_avg_us={snapshot['away_avg_us']:.1f}"
            f" away_max_us={snapshot['away_max_us']:.1f}"
            f" switches_per_s={snapshot['switches_per_s']:.0f}"
            f" window_s={snapshot['window_s']:.2f}",
            file=sys.stderr,
            flush=True,
        )


class _SchedulerTiming:
    """Driver-loop / select trip counters for busy-spin detection.

    Enable with ``TEALETIO_SCHEDULER_TIMING=1``. Key signals:

    - *busy_continue*: driver skipped ``wait`` because runnables remained after
      a batch (tight loop without select). High rate + low *select_block* share
      suggests scheduler thrash.
    - *select_timeout_zero* vs *select_timeout_block*: non-blocking vs blocking
      poll. Many zero-timeout selects while CPU-bound is busy-polling.
    - *batch_share_pct* / *wait_share_pct*: wall-time split of the driver trip.
    """

    _SUMMARY_EVERY = 5_000

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._window_start = time.perf_counter()
        self._atexit_registered = False
        # driver loop
        self.loop_iters = 0
        self.batch_calls = 0
        self.batch_ns = 0
        self.batch_transfers = 0
        self.busy_continue = 0
        self.wait_calls = 0
        self.wait_ns = 0
        # schedule path
        self.schedule_calls = 0
        self.make_runnable = 0
        self.break_wait = 0
        # select / poll
        self.select_calls = 0
        self.select_ns = 0
        self.select_timeout_zero = 0
        self.select_timeout_block = 0  # timeout is None or > 0
        self.select_empty = 0
        self.select_wakeup_only = 0
        self.select_with_io = 0
        self.poll_completed_ops = 0
        self.wait_no_progress = 0  # wait returned, next batch transferred 0

    def _ensure_atexit(self) -> None:
        if self._atexit_registered:
            return
        self._atexit_registered = True
        atexit.register(self.dump_summary, final=True)

    def note_loop_iter(self) -> None:
        self.loop_iters += 1

    def note_batch(self, transfers: int, elapsed_ns: int) -> None:
        self._ensure_atexit()
        self.batch_calls += 1
        self.batch_ns += max(elapsed_ns, 0)
        self.batch_transfers += max(transfers, 0)
        self._maybe_dump()

    def note_busy_continue(self) -> None:
        self.busy_continue += 1

    def note_wait(self, elapsed_ns: int) -> None:
        self._ensure_atexit()
        self.wait_calls += 1
        self.wait_ns += max(elapsed_ns, 0)
        self._maybe_dump()

    def note_schedule(self) -> None:
        self.schedule_calls += 1

    def note_make_runnable(self) -> None:
        self.make_runnable += 1

    def note_break_wait(self) -> None:
        self.break_wait += 1

    def note_select(
        self,
        *,
        timeout: float | None,
        elapsed_ns: int,
        n_events: int,
        woke: bool,
        n_completed: int,
    ) -> None:
        self._ensure_atexit()
        self.select_calls += 1
        self.select_ns += max(elapsed_ns, 0)
        if timeout is not None and timeout <= 0:
            self.select_timeout_zero += 1
        else:
            self.select_timeout_block += 1
        if n_events == 0:
            self.select_empty += 1
        elif woke and n_completed == 0 and n_events <= 1:
            self.select_wakeup_only += 1
        else:
            self.select_with_io += 1
        self.poll_completed_ops += n_completed
        self._maybe_dump()

    def note_wait_no_progress(self) -> None:
        self.wait_no_progress += 1

    def _maybe_dump(self) -> None:
        # dump on batch/wait boundaries when enough activity
        n = self.batch_calls + self.wait_calls
        if n > 0 and n % self._SUMMARY_EVERY == 0:
            self.dump_summary(final=False)

    def dump_summary(self, *, final: bool = False) -> None:
        with self._lock:
            if self.loop_iters == 0 and self.select_calls == 0 and self.batch_calls == 0:
                return
            snap = self._snapshot()
            if final:
                # reset window counters (keep atexit registration)
                atexit_reg = self._atexit_registered
                self.__dict__.clear()
                self.__init__()
                self._atexit_registered = atexit_reg
        self._print(snap, final=final)

    def _snapshot(self) -> dict[str, float | int]:
        window_s = max(time.perf_counter() - self._window_start, 1e-9)
        inv_w = 1.0 / window_s
        return {
            "window_s": window_s,
            "loop_iters": self.loop_iters,
            "loop_per_s": self.loop_iters * inv_w,
            "batch_calls": self.batch_calls,
            "batch_transfers": self.batch_transfers,
            "batch_avg_xfer": (self.batch_transfers / self.batch_calls) if self.batch_calls else 0.0,
            "batch_share_pct": (self.batch_ns / 1e9) * inv_w * 100.0,
            "busy_continue": self.busy_continue,
            "busy_continue_pct": (100.0 * self.busy_continue / self.loop_iters) if self.loop_iters else 0.0,
            "wait_calls": self.wait_calls,
            "wait_share_pct": (self.wait_ns / 1e9) * inv_w * 100.0,
            "wait_avg_us": (self.wait_ns / self.wait_calls / 1000.0) if self.wait_calls else 0.0,
            "wait_no_progress": self.wait_no_progress,
            "schedule_calls": self.schedule_calls,
            "make_runnable": self.make_runnable,
            "break_wait": self.break_wait,
            "select_calls": self.select_calls,
            "select_share_pct": (self.select_ns / 1e9) * inv_w * 100.0,
            "select_avg_us": (self.select_ns / self.select_calls / 1000.0) if self.select_calls else 0.0,
            "select_timeout_zero": self.select_timeout_zero,
            "select_timeout_block": self.select_timeout_block,
            "select_zero_pct": (
                100.0 * self.select_timeout_zero / self.select_calls if self.select_calls else 0.0
            ),
            "select_empty": self.select_empty,
            "select_wakeup_only": self.select_wakeup_only,
            "select_with_io": self.select_with_io,
            "poll_completed_ops": self.poll_completed_ops,
            "sched_per_s": self.schedule_calls * inv_w,
            "runnable_per_s": self.make_runnable * inv_w,
        }

    @staticmethod
    def _print(snapshot: dict[str, float | int], *, final: bool) -> None:
        tag = "final" if final else "progress"
        print(
            f"[scheduler-timing] {tag}"
            f" window_s={snapshot['window_s']:.2f}"
            f" loop/s={snapshot['loop_per_s']:.0f}"
            f" busy_continue={snapshot['busy_continue']}"
            f" busy_continue_pct={snapshot['busy_continue_pct']:.1f}"
            f" batch_share_pct={snapshot['batch_share_pct']:.1f}"
            f" wait_share_pct={snapshot['wait_share_pct']:.1f}"
            f" wait_avg_us={snapshot['wait_avg_us']:.1f}"
            f" wait_no_progress={snapshot['wait_no_progress']}"
            f" select/s={snapshot['select_calls'] / max(snapshot['window_s'], 1e-9):.0f}"
            f" select_zero_pct={snapshot['select_zero_pct']:.1f}"
            f" select_avg_us={snapshot['select_avg_us']:.1f}"
            f" select_empty={snapshot['select_empty']}"
            f" select_wakeup_only={snapshot['select_wakeup_only']}"
            f" select_with_io={snapshot['select_with_io']}"
            f" batch_avg_xfer={snapshot['batch_avg_xfer']:.2f}"
            f" sched/s={snapshot['sched_per_s']:.0f}"
            f" make_runnable/s={snapshot['runnable_per_s']:.0f}"
            f" break_wait={snapshot['break_wait']}"
            f" poll_ops={snapshot['poll_completed_ops']}",
            file=sys.stderr,
            flush=True,
        )


class _SpawnTiming:
    """Eager-start latency: ``tealet.run`` request → task / user entry.

    Enable with ``TEALETIO_SPAWN_TIMING=1``.

    *run_to_main_ns* — stamp just before ``tealet.run`` until ``task_main``
    first instruction (C transfer + Python task_main entry).

    *run_to_user_ns* — same stamp until the user callable is about to run
    (adds ``contextvars.Context.run`` setup). That is when the handler body
    (e.g. ``serve`` / ``client_handler``) starts.
    """

    _SUMMARY_EVERY = 2_000
    _PLAUSIBLE_NS = 5_000_000  # 5 ms

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # set immediately before tealet.run; read on the new tealet
        self._run_depart_ns = 0
        self._main_count = 0
        self._user_count = 0
        self._main_total_ns = 0
        self._main_min_ns = 0
        self._main_max_ns = 0
        self._user_total_ns = 0
        self._user_min_ns = 0
        self._user_max_ns = 0
        self._dropped = 0
        self._window_start = time.perf_counter()
        self._atexit_registered = False

    def _ensure_atexit(self) -> None:
        if self._atexit_registered:
            return
        self._atexit_registered = True
        atexit.register(self.dump_summary, final=True)

    def note_run_begin(self) -> None:
        self._run_depart_ns = time.perf_counter_ns()

    def note_task_main(self) -> None:
        depart = self._run_depart_ns
        if depart <= 0:
            return
        dt = time.perf_counter_ns() - depart
        if dt < 0 or dt > self._PLAUSIBLE_NS:
            self._dropped += 1
            return
        self._ensure_atexit()
        with self._lock:
            self._main_count += 1
            self._main_total_ns += dt
            if self._main_count == 1:
                self._main_min_ns = dt
                self._main_max_ns = dt
            else:
                if dt < self._main_min_ns:
                    self._main_min_ns = dt
                if dt > self._main_max_ns:
                    self._main_max_ns = dt
            should_dump = self._main_count % self._SUMMARY_EVERY == 0
            snap = self._snapshot_locked() if should_dump else None
        if snap is not None:
            self._print(snap, final=False)

    def note_user_func(self) -> None:
        depart = self._run_depart_ns
        if depart <= 0:
            return
        # consume so a later non-eager task_main does not use a stale stamp
        self._run_depart_ns = 0
        dt = time.perf_counter_ns() - depart
        if dt < 0 or dt > self._PLAUSIBLE_NS:
            return
        with self._lock:
            self._user_count += 1
            self._user_total_ns += dt
            if self._user_count == 1:
                self._user_min_ns = dt
                self._user_max_ns = dt
            else:
                if dt < self._user_min_ns:
                    self._user_min_ns = dt
                if dt > self._user_max_ns:
                    self._user_max_ns = dt

    def dump_summary(self, *, final: bool = False) -> None:
        with self._lock:
            if self._main_count == 0:
                return
            snap = self._snapshot_locked()
            if final:
                atexit_reg = self._atexit_registered
                self.__dict__.clear()
                self.__init__()
                self._atexit_registered = atexit_reg
        self._print(snap, final=final)

    def _snapshot_locked(self) -> dict[str, float | int]:
        window_s = max(time.perf_counter() - self._window_start, 1e-9)
        n = self._main_count
        un = self._user_count
        inv = 1.0 / n if n else 0.0
        uinv = 1.0 / un if un else 0.0
        return {
            "n": n,
            "user_n": un,
            "dropped": self._dropped,
            "run_to_main_avg_ns": self._main_total_ns * inv,
            "run_to_main_min_ns": self._main_min_ns,
            "run_to_main_max_ns": self._main_max_ns,
            "run_to_user_avg_ns": self._user_total_ns * uinv,
            "run_to_user_min_ns": self._user_min_ns if un else 0,
            "run_to_user_max_ns": self._user_max_ns if un else 0,
            "spawns_per_s": n / window_s,
            "window_s": window_s,
        }

    @staticmethod
    def _print(snapshot: dict[str, float | int], *, final: bool) -> None:
        tag = "final" if final else "progress"
        print(
            f"[spawn-timing] {tag}"
            f" n={snapshot['n']}"
            f" user_n={snapshot['user_n']}"
            f" dropped={snapshot['dropped']}"
            f" run_to_main_avg_ns={snapshot['run_to_main_avg_ns']:.0f}"
            f" run_to_main_min_ns={snapshot['run_to_main_min_ns']}"
            f" run_to_main_max_ns={snapshot['run_to_main_max_ns']}"
            f" run_to_user_avg_ns={snapshot['run_to_user_avg_ns']:.0f}"
            f" run_to_user_min_ns={snapshot['run_to_user_min_ns']}"
            f" run_to_user_max_ns={snapshot['run_to_user_max_ns']}"
            f" spawns_per_s={snapshot['spawns_per_s']:.0f}"
            f" window_s={snapshot['window_s']:.2f}",
            file=sys.stderr,
            flush=True,
        )


class _StreamDiag:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: Counter[str] = Counter()
        self._last_event = 0.0
        self._blocking: dict[int, tuple[str, float, str]] = {}
        self._accept = _AcceptDeliveryTiming()
        self._accept_path = _AcceptPathTiming()
        self._recv_iter_path = _RecvIterPathTiming()
        self._worker_completion = _WorkerCompletionTiming()
        self._switch_timing = _SwitchTiming()
        self._scheduler_timing = _SchedulerTiming()
        self._spawn_timing = _SpawnTiming()

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


def worker_completion_begin(kind: str) -> None:
    if not worker_completion_enabled():
        return
    _diag._worker_completion.begin(kind)


def worker_completion_mark_emit_start() -> None:
    if not worker_completion_enabled():
        return
    _diag._worker_completion.mark_emit_start()


def worker_completion_mark_emit_end() -> None:
    if not worker_completion_enabled():
        return
    _diag._worker_completion.mark_emit_end()


def worker_completion_end() -> None:
    if not worker_completion_enabled():
        return
    _diag._worker_completion.end()


def worker_completion_dump(*, final: bool = False) -> None:
    if not worker_completion_enabled():
        return
    _diag._worker_completion.dump_summary(final=final)


def tealet_switch(target: tealet.tealet) -> None:
    """``target.switch()`` with optional pure switch-cost microtiming."""

    if not _SWITCH_TIMING:
        target.switch()
        return
    _diag._switch_timing.switch(target)


def tealet_run(target: tealet.tealet, func: object, arg: object = None) -> object:
    """``tealet.run()`` with optional switch microtiming and spawn latency stamp."""

    import tealet as tealet_mod

    if _SPAWN_TIMING:
        _diag._spawn_timing.note_run_begin()
    if not _SWITCH_TIMING:
        return tealet_mod.tealet.run(target, func, arg)
    return _diag._switch_timing.note_run(target, func, arg)


def note_spawn_task_main() -> None:
    """Sample eager-start latency at ``task_main`` entry (after tealet.run transfer)."""

    if not _SPAWN_TIMING:
        return
    _diag._spawn_timing.note_task_main()


def note_spawn_user_func() -> None:
    """Sample eager-start latency just before the user callable runs."""

    if not _SPAWN_TIMING:
        return
    _diag._spawn_timing.note_user_func()


def spawn_timing_dump(*, final: bool = False) -> None:
    if not _SPAWN_TIMING:
        return
    _diag._spawn_timing.dump_summary(final=final)


def note_switch_depart() -> None:
    """Stamp pure-transfer depart for C-driven switches (task exit / throw)."""

    if not _SWITCH_TIMING:
        return
    _diag._switch_timing.note_depart()


def switch_timing_dump(*, final: bool = False) -> None:
    if not _SWITCH_TIMING:
        return
    _diag._switch_timing.dump_summary(final=final)


def sched_note_loop_iter() -> None:
    if not _SCHEDULER_TIMING:
        return
    _diag._scheduler_timing.note_loop_iter()


def sched_note_batch(transfers: int, elapsed_ns: int) -> None:
    if not _SCHEDULER_TIMING:
        return
    _diag._scheduler_timing.note_batch(transfers, elapsed_ns)


def sched_note_busy_continue() -> None:
    if not _SCHEDULER_TIMING:
        return
    _diag._scheduler_timing.note_busy_continue()


def sched_note_wait(elapsed_ns: int) -> None:
    if not _SCHEDULER_TIMING:
        return
    _diag._scheduler_timing.note_wait(elapsed_ns)


def sched_note_schedule() -> None:
    if not _SCHEDULER_TIMING:
        return
    _diag._scheduler_timing.note_schedule()


def sched_note_make_runnable() -> None:
    if not _SCHEDULER_TIMING:
        return
    _diag._scheduler_timing.note_make_runnable()


def sched_note_break_wait() -> None:
    if not _SCHEDULER_TIMING:
        return
    _diag._scheduler_timing.note_break_wait()


def sched_note_select(
    *,
    timeout: float | None,
    elapsed_ns: int,
    n_events: int,
    woke: bool,
    n_completed: int,
) -> None:
    if not _SCHEDULER_TIMING:
        return
    _diag._scheduler_timing.note_select(
        timeout=timeout,
        elapsed_ns=elapsed_ns,
        n_events=n_events,
        woke=woke,
        n_completed=n_completed,
    )


def sched_note_wait_no_progress() -> None:
    if not _SCHEDULER_TIMING:
        return
    _diag._scheduler_timing.note_wait_no_progress()


def scheduler_timing_dump(*, final: bool = False) -> None:
    if not _SCHEDULER_TIMING:
        return
    _diag._scheduler_timing.dump_summary(final=final)


event = _diag.event
block_enter = _diag.block_enter
block_exit = _diag.block_exit
snapshot = _diag.snapshot
total_events = _diag.total_events

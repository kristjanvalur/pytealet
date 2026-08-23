"""Tealet-aware profiler built on the stdlib :mod:`profile` module.

``sys.setprofile`` is thread-local and knows nothing about stack slicing, so a
plain :class:`profile.Profile` mis-pairs call/return events across
``switch()`` / ``throw()``. This subclass keeps a **stack** (parallel ``cur``
plus its own timings) per tealet, and a per-thread default stack until the
first transfer.

Stacks that start with the same root function — the first real call's code
object ``(co_filename, co_firstlineno, co_name)`` — form a **stack family**.
The same lambda line, or the same ``Thread(target=f)`` on many threads, is one
family. Query individuals with :meth:`Profile.stacks`, families with
:meth:`Profile.stack_families`, or the grand total with :meth:`Profile.combined`
(``print_stats`` uses the total). By default a finished tealet is **folded**
into its family: timings stay, the parallel stack is dropped. Pass
``fold_on_exit=False`` to keep every individual until ``create_stats``.

``_tealet.settrace`` is interpreter-wide. A module trampoline looks up this
thread's profiler in TLS so each thread can own a ``Profile``.

The default timer is ``time.thread_time`` (this thread's CPU), not stdlib
``profile``'s process-wide ``time.process_time``. Each stack is one thread of
control, so thread CPU does not need a GIL last-active pause. Pass
``timer=time.process_time`` or ``timer=time.perf_counter`` to opt into those
clocks. Tealet switch still stops the origin stack so time in another tealet
is not billed to it.
"""

from __future__ import annotations

import os
import profile as stdlib_profile
import sys
import threading
import time
import weakref
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import _tealet

__all__ = ["Profile", "StackStats", "run", "runctx"]

# typeshed's profile.Profile stub omits the parallel-stack internals we wrap.
if TYPE_CHECKING:
    _StdlibProfile = Any
else:
    _StdlibProfile = stdlib_profile.Profile

_profile_mod: Any = stdlib_profile
_Utils = _profile_mod._Utils
_profile_cls: Any = stdlib_profile.Profile

# _tealet.settrace is one slot for the interpreter; sys.setprofile is per-thread.
# the trampoline stays installed while any thread has a Profile enabled, and
# forwards to that thread's profiler.
_hook_tls = threading.local()
_hook_lock = threading.Lock()
_hook_users = 0
_hook_previous: Callable[..., Any] | None = None
_PROFILE_FILE = __file__
_MAIN_FAMILY = ("<main>", 0, "<main>")


def _on_thread_switch(event, args):
    prof = getattr(_hook_tls, "profiler", None)
    if prof is not None:
        prof._on_switch(event, args)


def _install_thread_hook() -> None:
    global _hook_users, _hook_previous
    with _hook_lock:
        if _hook_users == 0:
            _hook_previous = _tealet.gettrace()
            _tealet.settrace(_on_thread_switch)
        _hook_users += 1


def _remove_thread_hook() -> None:
    global _hook_users, _hook_previous
    with _hook_lock:
        _hook_users -= 1
        if _hook_users == 0:
            if _tealet.gettrace() is _on_thread_switch:
                _tealet.settrace(_hook_previous)
            _hook_previous = None


def _family_key(code) -> tuple[str, int, str]:
    return (code.co_filename, code.co_firstlineno, code.co_name)


def _snapshot_timings(timings: dict) -> dict:
    stats = {}
    for func, (cc, _ns, tt, ct, callers) in timings.items():
        callers = callers.copy()
        nc = 0
        for callcnt in callers.values():
            nc += callcnt
        stats[func] = cc, nc, tt, ct, callers
    return stats


def _merge_timings(dst: dict, src: dict) -> None:
    for func, (cc, ns, tt, ct, callers) in src.items():
        if func in dst:
            dcc, dns, dtt, dct, dcallers = dst[func]
            merged_callers = dcallers.copy()
            for callee, n in callers.items():
                merged_callers[callee] = merged_callers.get(callee, 0) + n
            dst[func] = (dcc + cc, dns + ns, dtt + tt, dct + ct, merged_callers)
        else:
            dst[func] = (cc, ns, tt, ct, callers.copy())


class _Folded:
    """Family accumulator for stacks dropped at tealet exit."""

    __slots__ = ("family", "nstacks", "thread_ids", "timings")

    def __init__(self, family):
        self.family = family
        self.nstacks = 0
        self.thread_ids: set[int] = set()
        self.timings: dict = {}


class _Stack:
    """One profiled call stack: parallel ``cur`` plus private timings."""

    __slots__ = ("c_func_name", "cur", "family", "finalized", "t", "thread_id", "timings")

    def __init__(self, cur, t, c_func_name, timings):
        self.cur = cur
        self.t = t
        self.c_func_name = c_func_name
        self.timings = timings
        self.family = None
        self.finalized = False
        self.thread_id = threading.get_ident()


class StackStats:
    """A ``pstats``-compatible snapshot of one stack or stack family."""

    def __init__(
        self,
        stats: dict,
        *,
        family: tuple[str, int, str] | None = None,
        nstacks: int = 1,
        thread_ids: frozenset[int] | None = None,
    ) -> None:
        self.stats = stats
        self.family = family
        self.nstacks = nstacks
        self.thread_ids = thread_ids if thread_ids is not None else frozenset()
        self.thread_id = next(iter(self.thread_ids)) if len(self.thread_ids) == 1 else None

    def create_stats(self) -> None:
        return None

    def print_stats(self, sort: Any = -1) -> None:
        import pstats

        arg: Any = self
        pstats.Stats(arg).strip_dirs().sort_stats(sort).print_stats()


class Profile(_StdlibProfile):
    """:class:`profile.Profile` that follows tealet transfers.

    Public helpers match the stdlib class (``run``, ``runctx``, ``runcall``,
    ``print_stats``, ``dump_stats``) plus ``enable`` / ``disable``. Each stack
    keeps true recursion bookkeeping; :meth:`stacks`, :meth:`stack_families`,
    and :meth:`combined` expose individuals, same-root groups, and the total.

    ``fold_on_exit`` (default on) merges a tealet's timings into its family
    when it reaches ``STATE_EXIT`` and drops the individual. Pass
    ``False`` to retain every stack for :meth:`stacks`.

    The default clock is ``time.thread_time``. Stdlib ``profile`` uses
    process-wide ``time.process_time``; pass ``timer=`` to override.
    """

    def __init__(
        self,
        timer: Callable[[], Any] | None = None,
        bias: Any = None,
        *,
        fold_on_exit: bool = True,
    ) -> None:
        if timer is None:
            timer = time.thread_time
        super().__init__(timer=timer, bias=bias)
        self.fold_on_exit = fold_on_exit
        self._base_dispatcher = self.dispatcher
        self.dispatcher = self._dispatch
        base_dispatch = _profile_cls.dispatch
        self.dispatch = {
            **base_dispatch,
            "call": type(self).trace_dispatch_call,
            "return": type(self).trace_dispatch_return,
            "c_return": type(self).trace_dispatch_return,
            "c_exception": type(self).trace_dispatch_return,
        }
        self._lock = threading.Lock()
        self._cur_lock = threading.Lock()
        self._all_stacks: list[_Stack] = []
        self._folded: dict[tuple[str, int, str], _Folded] = {}
        self._tls = threading.local()
        self._bind_thread_default()

    def enable(self) -> None:
        """Start profiling on this thread."""
        self._ensure_thread()
        if getattr(self._tls, "enabled", False):
            return
        # load this thread's stack; do not store leftover cur from another thread.
        self._load_current()
        self._tls.old_profile = sys.getprofile()
        _hook_tls.profiler = self
        self._tls.enabled = True
        _install_thread_hook()
        # last, so enable() itself is not half-traced
        sys.setprofile(self.dispatcher)

    def disable(self) -> None:
        """Stop profiling on this thread and restore previous hooks."""
        if not getattr(self._tls, "enabled", False):
            return
        # first, so disable() is not left on the parallel stack
        sys.setprofile(self._tls.old_profile)
        self._tls.enabled = False
        if getattr(_hook_tls, "profiler", None) is self:
            _hook_tls.profiler = None
        _remove_thread_hook()

    def runctx(self, cmd, globals, locals):
        self.set_cmd(cmd)
        self._store_current()
        self.enable()
        try:
            exec(cmd, globals, locals)  # noqa: S102
        finally:
            self.disable()
        return self

    def runcall(self, func, /, *args, **kw):
        self.set_cmd(repr(func))
        self._store_current()
        self.enable()
        try:
            return func(*args, **kw)
        finally:
            self.disable()

    def create_stats(self) -> None:
        self.stats = self.combined().stats

    def stacks(self) -> list[StackStats]:
        """Return a snapshot for every retained stack (true recursion).

        With :attr:`fold_on_exit`, finished tealets are absent here and appear
        only in :meth:`stack_families` / :meth:`combined`.
        """
        self._finalize_all()
        return [
            StackStats(
                _snapshot_timings(st.timings),
                family=st.family or _MAIN_FAMILY,
                nstacks=1,
                thread_ids=frozenset({st.thread_id}),
            )
            for st in self._stack_list()
        ]

    def stack_families(self) -> list[StackStats]:
        """Return one snapshot per root-function family."""
        self._finalize_all()
        out = []
        for key, timings, nstacks, tids in self._family_parts():
            out.append(
                StackStats(
                    _snapshot_timings(timings),
                    family=key,
                    nstacks=nstacks,
                    thread_ids=frozenset(tids),
                )
            )
        out.sort(key=lambda view: (-view.nstacks, view.family or _MAIN_FAMILY))
        return out

    def combined(self) -> StackStats:
        """Return timings for every stack merged into one snapshot."""
        self._finalize_all()
        merged: dict = {}
        nstacks = 0
        tids: set[int] = set()
        for _key, timings, n, ids in self._family_parts():
            _merge_timings(merged, timings)
            nstacks += n
            tids.update(ids)
        return StackStats(
            _snapshot_timings(merged),
            family=None,
            nstacks=nstacks,
            thread_ids=frozenset(tids),
        )

    def calibrate(self, m, verbose=0):
        # measure stdlib stopwatch overhead; our extra cost is the stack swap.
        return stdlib_profile.Profile().calibrate(m, verbose)

    def trace_dispatch_call(self, frame, t):
        self._maybe_set_family(frame)
        return _profile_cls.trace_dispatch_call(self, frame, t)

    def trace_dispatch_return(self, frame, t):
        # enable() can start tracing mid-stack; ignore a return that has no
        # matching call on the fake root we preloaded.
        if self.cur is not None and frame is not self.cur[-2] and isinstance(self.cur[-2], self.fake_frame):
            return 0
        return _profile_cls.trace_dispatch_return(self, frame, t)

    def _maybe_set_family(self, frame) -> None:
        st = getattr(self._tls, "current", None)
        if st is None or st.family is not None:
            return
        if isinstance(frame, self.fake_frame):
            return
        filename = frame.f_code.co_filename
        if filename in ("profile", _PROFILE_FILE):
            return
        base = os.path.basename(filename)
        if base in ("threading.py", "__init__.py") and "threading" in filename.replace("\\", "/"):
            return
        st.family = _family_key(frame.f_code)

    def _dispatch(self, frame, event, arg):
        # trampoline and Profile internals live in this file; drop them so a
        # C-invoked settrace callback cannot desync the parallel stack.
        if frame.f_code.co_filename == _PROFILE_FILE:
            return
        # one Profile can be enabled on many threads; cur is instance-wide.
        with self._cur_lock:
            self._load_current()
            try:
                return self._base_dispatcher(frame, event, arg)
            finally:
                self._store_current()

    def _on_switch(self, event, args):
        del event
        if not getattr(self._tls, "enabled", False):
            return
        origin, target = args
        sys.setprofile(None)
        try:
            with self._cur_lock:
                self._switch_stacks(origin, target)
        finally:
            if getattr(self._tls, "enabled", False):
                sys.setprofile(self.dispatcher)

    def _switch_stacks(self, origin, target):
        self._ensure_thread()
        self._load_current()
        self._charge_elapsed()
        self._store_current()

        current = self._tls.current
        by_tealet = self._tls.stacks
        if self._tls.default is not None and current is self._tls.default:
            by_tealet[origin] = current
            self._tls.default = None
        else:
            by_tealet[origin] = current

        if origin.state == _tealet.STATE_EXIT:
            self._finalize_stack(current)
            by_tealet.pop(origin, None)
            self._fold_dead_stack(current)

        st = by_tealet.get(target)
        if st is None:
            st = self._new_stack("tealet")
            by_tealet[target] = st
        self._tls.current = st
        st.t = self.get_time()
        self._load_stack(st)

    def _pause_stack(self, st: _Stack) -> None:
        now = self.get_time()
        t = now - st.t - self.bias
        if st.cur is not None:
            rpt, rit, ret, rfn, rframe, rcur = st.cur
            st.cur = (rpt, rit + t, ret, rfn, rframe, rcur)
        st.t = now

    def _charge_elapsed(self) -> None:
        st = getattr(self._tls, "current", None)
        if st is not None:
            self._pause_stack(st)
            self.cur = st.cur
            self.t = st.t
            return
        t = self.get_time() - self.t - self.bias
        if self.cur is not None:
            rpt, rit, ret, rfn, rframe, rcur = self.cur
            self.cur = (rpt, rit + t, ret, rfn, rframe, rcur)
        self.t = self.get_time()

    def _bind_thread_default(self) -> None:
        st = _Stack(self.cur, self.t, self.c_func_name, self.timings)
        self._tls.default = st
        self._tls.current = st
        self._tls.stacks = weakref.WeakKeyDictionary()
        self._tls.ready = True
        self._tls.enabled = False
        self._register_stack(st)

    def _ensure_thread(self) -> None:
        if getattr(self._tls, "ready", False):
            return
        st = self._new_stack("profiler")
        self._tls.default = st
        self._tls.current = st
        self._tls.stacks = weakref.WeakKeyDictionary()
        self._tls.ready = True
        self._tls.enabled = False

    def _register_stack(self, st: _Stack) -> None:
        with self._lock:
            self._all_stacks.append(st)

    def _stack_list(self) -> list[_Stack]:
        with self._lock:
            return list(self._all_stacks)

    def _family_parts(self):
        parts: dict[tuple[str, int, str], _Folded] = {}
        for st in self._stack_list():
            key = st.family or _MAIN_FAMILY
            item = parts.get(key)
            if item is None:
                item = _Folded(key)
                parts[key] = item
            _merge_timings(item.timings, st.timings)
            item.nstacks += 1
            item.thread_ids.add(st.thread_id)
        with self._lock:
            folded = list(self._folded.values())
        for src in folded:
            item = parts.get(src.family)
            if item is None:
                item = _Folded(src.family)
                parts[src.family] = item
            _merge_timings(item.timings, src.timings)
            item.nstacks += src.nstacks
            item.thread_ids.update(src.thread_ids)
        return [(item.family, item.timings, item.nstacks, item.thread_ids) for item in parts.values()]

    def _fold_dead_stack(self, st: _Stack) -> None:
        if not self.fold_on_exit:
            return
        key = st.family or _MAIN_FAMILY
        with self._lock:
            folded = self._folded.get(key)
            if folded is None:
                folded = _Folded(key)
                self._folded[key] = folded
            _merge_timings(folded.timings, st.timings)
            folded.nstacks += 1
            folded.thread_ids.add(st.thread_id)
            try:
                self._all_stacks.remove(st)
            except ValueError:
                pass
        st.cur = None
        st.timings = {}

    def _new_stack(self, name: str) -> _Stack:
        saved = (self.cur, self.t, self.c_func_name, self.timings)
        self.cur = None
        self.timings = {}
        self.simulate_call(name)
        st = _Stack(self.cur, self.get_time(), "", self.timings)
        self.cur, self.t, self.c_func_name, self.timings = saved
        self._register_stack(st)
        return st

    def _load_stack(self, st: _Stack) -> None:
        self.cur = st.cur
        self.t = st.t
        self.c_func_name = st.c_func_name
        self.timings = st.timings

    def _save_stack(self, st: _Stack) -> None:
        st.cur = self.cur
        st.t = self.t
        st.c_func_name = self.c_func_name
        st.timings = self.timings

    def _load_current(self) -> None:
        self._ensure_thread()
        self._load_stack(self._tls.current)

    def _store_current(self) -> None:
        if not getattr(self._tls, "ready", False):
            return
        self._save_stack(self._tls.current)

    def _finalize_stack(self, st: _Stack) -> None:
        if st.finalized:
            return
        self._load_stack(st)
        self.simulate_cmd_complete()
        self._save_stack(st)
        st.finalized = True

    def _finalize_all(self) -> None:
        self._ensure_thread()
        self._store_current()
        saved = getattr(self._tls, "current", None)
        for st in self._stack_list():
            self._finalize_stack(st)
        if saved is not None:
            self._load_stack(saved)
            self._tls.current = saved


def run(statement: str, filename: str | None = None, sort: Any = -1):
    """Run *statement* under a tealet-aware profiler."""
    return _Utils(Profile).run(statement, filename, sort)


def runctx(
    statement: str,
    globals: dict[str, Any],
    locals: dict[str, Any] | None,
    filename: str | None = None,
    sort: Any = -1,
):
    """Run *statement* under a tealet-aware profiler with explicit namespaces."""
    return _Utils(Profile).runctx(statement, globals, locals, filename, sort)

"""Tealet-aware profiler built on the stdlib :mod:`profile` module.

``sys.setprofile`` is thread-local and knows nothing about stack slicing, so a
plain :class:`profile.Profile` mis-pairs call/return events across
``switch()`` / ``throw()``. This subclass keeps the same timings table, but
gives each tealet its own parallel stack.

Until the first switch or throw on a thread, samples accumulate on a per-thread
default stack. That default is then promoted to the origin tealet, and later
transfers swap stacks so time stays on the tealet that actually ran.
"""

from __future__ import annotations

import profile as stdlib_profile
import sys
import threading
import weakref
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import _tealet

__all__ = ["Profile", "run", "runctx"]

# typeshed's profile.Profile stub omits the parallel-stack internals we wrap.
if TYPE_CHECKING:
    _StdlibProfile = Any
else:
    _StdlibProfile = stdlib_profile.Profile

_profile_mod: Any = stdlib_profile
_Utils = _profile_mod._Utils
_profile_cls: Any = stdlib_profile.Profile


class _Stack:
    """Parallel-stack snapshot for one tealet (or the thread default)."""

    __slots__ = ("c_func_name", "cur", "t")

    def __init__(self, cur, t, c_func_name):
        self.cur = cur
        self.t = t
        self.c_func_name = c_func_name


class Profile(_StdlibProfile):
    """:class:`profile.Profile` that follows tealet transfers.

    Public helpers match the stdlib class (``run``, ``runctx``, ``runcall``,
    ``print_stats``, ``dump_stats``) plus ``enable`` / ``disable`` so a
    long-lived program can start and stop tracing without wrapping a single
    statement. ``enable`` installs both ``sys.setprofile`` and
    ``_tealet.settrace``; last setter still wins on the tealet hook.
    """

    def __init__(self, timer: Callable[[], Any] | None = None, bias: Any = None) -> None:
        super().__init__(timer=timer, bias=bias)
        self._base_dispatcher = self.dispatcher
        self.dispatcher = self._dispatch
        base_dispatch = _profile_cls.dispatch
        self.dispatch = {
            **base_dispatch,
            "return": type(self).trace_dispatch_return,
            "c_return": type(self).trace_dispatch_return,
            "c_exception": type(self).trace_dispatch_return,
        }
        self._tls = threading.local()
        self._stacks: weakref.WeakKeyDictionary[Any, _Stack] = weakref.WeakKeyDictionary()
        self._old_trace: Callable[..., Any] | None = None
        self._enable_count = 0
        self._bind_thread_default()

    def enable(self) -> None:
        """Start profiling on this thread."""
        self._ensure_thread()
        self._store_current()
        if getattr(self._tls, "enabled", False):
            return
        self._tls.old_profile = sys.getprofile()
        if self._enable_count == 0:
            self._old_trace = _tealet.gettrace()
            _tealet.settrace(self._on_switch)
        self._enable_count += 1
        self._tls.enabled = True
        # last, so enable() itself is not half-traced
        sys.setprofile(self.dispatcher)

    def disable(self) -> None:
        """Stop profiling on this thread and restore previous hooks."""
        if not getattr(self._tls, "enabled", False):
            return
        # first, so disable() is not left on the parallel stack
        sys.setprofile(self._tls.old_profile)
        self._tls.enabled = False
        self._enable_count -= 1
        if self._enable_count == 0:
            _tealet.settrace(self._old_trace)
            self._old_trace = None

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
        self._ensure_thread()
        self._store_current()
        stacks = list(self._stacks.values())
        default = getattr(self._tls, "default", None)
        current = getattr(self._tls, "current", None)
        if default is not None and default not in stacks:
            stacks.append(default)
        if current is not None and current not in stacks:
            stacks.append(current)
        for st in stacks:
            self._load_stack(st)
            self.simulate_cmd_complete()
            self._save_stack(st)
        self.snapshot_stats()

    def calibrate(self, m, verbose=0):
        # measure stdlib stopwatch overhead; our extra cost is the stack swap.
        return stdlib_profile.Profile().calibrate(m, verbose)

    def trace_dispatch_return(self, frame, t):
        # enable() can start tracing mid-stack; ignore a return that has no
        # matching call on the fake root we preloaded.
        if self.cur is not None and frame is not self.cur[-2] and isinstance(self.cur[-2], self.fake_frame):
            return 0
        return _profile_cls.trace_dispatch_return(self, frame, t)

    def _dispatch(self, frame, event, arg):
        if frame.f_code is type(self)._on_switch.__code__:
            self._tls.in_hook = event == "call"
            return
        if getattr(self._tls, "in_hook", False):
            return
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
        # the tealet hook is not the profile dispatcher, so these frames would
        # otherwise be charged to whichever stack is currently loaded.
        sys.setprofile(None)
        try:
            self._switch_stacks(origin, target)
        finally:
            sys.setprofile(self.dispatcher)

    def _switch_stacks(self, origin, target):
        self._ensure_thread()
        self._load_current()
        self._charge_elapsed()
        self._store_current()

        current = self._tls.current
        if self._tls.default is not None and current is self._tls.default:
            self._stacks[origin] = current
            self._tls.default = None
        else:
            self._stacks[origin] = current

        if origin.state == _tealet.STATE_EXIT:
            self._load_stack(current)
            self.simulate_cmd_complete()
            self._save_stack(current)
            self._stacks.pop(origin, None)

        st = self._stacks.get(target)
        if st is None:
            st = self._new_stack("tealet")
            self._stacks[target] = st
        self._tls.current = st
        st.t = self.get_time()
        self._load_stack(st)

    def _charge_elapsed(self) -> None:
        t = self.get_time() - self.t - self.bias
        if self.cur is not None:
            rpt, rit, ret, rfn, rframe, rcur = self.cur
            self.cur = (rpt, rit + t, ret, rfn, rframe, rcur)
        self.t = self.get_time()

    def _bind_thread_default(self) -> None:
        st = _Stack(self.cur, self.t, self.c_func_name)
        self._tls.default = st
        self._tls.current = st
        self._tls.ready = True
        self._tls.enabled = False

    def _ensure_thread(self) -> None:
        if getattr(self._tls, "ready", False):
            return
        st = self._new_stack("profiler")
        self._tls.default = st
        self._tls.current = st
        self._tls.ready = True
        self._tls.enabled = False

    def _new_stack(self, name: str) -> _Stack:
        saved = (self.cur, self.t, self.c_func_name)
        self.cur = None
        self.simulate_call(name)
        st = _Stack(self.cur, self.get_time(), "")
        self.cur, self.t, self.c_func_name = saved
        return st

    def _load_stack(self, st: _Stack) -> None:
        self.cur = st.cur
        self.t = st.t
        self.c_func_name = st.c_func_name

    def _save_stack(self, st: _Stack) -> None:
        st.cur = self.cur
        st.t = self.t
        st.c_func_name = self.c_func_name

    def _load_current(self) -> None:
        self._ensure_thread()
        self._load_stack(self._tls.current)

    def _store_current(self) -> None:
        if not getattr(self._tls, "ready", False):
            return
        self._save_stack(self._tls.current)


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

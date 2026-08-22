"""Tealet-aware profiler built on the stdlib :mod:`profile` module.

``sys.setprofile`` is thread-local and knows nothing about stack slicing, so a
plain :class:`profile.Profile` mis-pairs call/return events across
``switch()`` / ``throw()``. This subclass keeps the same timings table, but
gives each tealet its own parallel stack.

Until the first switch or throw on a thread, samples accumulate on a per-thread
default stack. That default is then promoted to the origin tealet, and later
transfers swap stacks so time stays on the tealet that actually ran.

``_tealet.settrace`` is interpreter-wide, unlike ``sys.setprofile``. A module
trampoline is installed once and looks up this thread's profiler in TLS, so
each thread can own a ``Profile`` the same way it would for multithreaded
``cProfile``.
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

# _tealet.settrace is one slot for the interpreter; sys.setprofile is per-thread.
# the trampoline stays installed while any thread has a Profile enabled, and
# forwards to that thread's profiler.
_hook_tls = threading.local()
_hook_lock = threading.Lock()
_hook_users = 0
_hook_previous: Callable[..., Any] | None = None
_PROFILE_FILE = __file__


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
    statement. ``enable`` installs ``sys.setprofile`` on this thread and
    registers this instance in TLS for the process-wide tealet trampoline.
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
        self._bind_thread_default()

    def enable(self) -> None:
        """Start profiling on this thread."""
        self._ensure_thread()
        self._store_current()
        if getattr(self._tls, "enabled", False):
            return
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
        self._ensure_thread()
        self._store_current()
        stacks = list(self._tls.stacks.values())
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
        # trampoline and Profile internals live in this file; drop them so a
        # C-invoked settrace callback cannot desync the parallel stack.
        if frame.f_code.co_filename == _PROFILE_FILE:
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
        sys.setprofile(None)
        try:
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
        stacks = self._tls.stacks
        if self._tls.default is not None and current is self._tls.default:
            stacks[origin] = current
            self._tls.default = None
        else:
            stacks[origin] = current

        if origin.state == _tealet.STATE_EXIT:
            self._load_stack(current)
            self.simulate_cmd_complete()
            self._save_stack(current)
            stacks.pop(origin, None)

        st = stacks.get(target)
        if st is None:
            st = self._new_stack("tealet")
            stacks[target] = st
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
        self._tls.stacks = weakref.WeakKeyDictionary()
        self._tls.ready = True
        self._tls.enabled = False

    def _ensure_thread(self) -> None:
        if getattr(self._tls, "ready", False):
            return
        st = self._new_stack("profiler")
        self._tls.default = st
        self._tls.current = st
        self._tls.stacks = weakref.WeakKeyDictionary()
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

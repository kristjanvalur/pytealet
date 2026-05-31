# A greenlet emulation module using tealets
import contextvars
import os
import sys
import threading
import weakref
import types

import _tealet
from . import _greenlet


class error(Exception):
    pass
class GreenletExit(BaseException):
    pass


class _TraceException(BaseException):
    """Internal transport for deferred throw tracing metadata."""

    def __init__(self, original, trace_payload):
        super().__init__(original)
        self.original = original
        self.trace_payload = trace_payload


class _ParentThreadError(ValueError):
    "Special version of ValueError for identification inside handlers"
    def __init__(self, current_tid, target_tid, is_alive):
        super().__init__("parent cannot be on a different thread")
        self.current_tid = current_tid
        self.target_tid = target_tid
        self.is_alive = is_alive


def _cross_thread_switch_error_message(current_tid, target_tid, is_alive):
    if is_alive:
        return (
            "Cannot switch to a different thread\n"
            f"\tCurrent:  {current_tid}\n"
            f"\tExpected: {target_tid}"
        )
    return "cannot switch to a different thread (which happens to have exited)"

class ErrorWrapper(object):
    def __enter__(self):
        pass

    def __exit__(self, _, val, tb):
        if val is None:
            return
        try:
            if isinstance(val, _TraceException):
                _consume_trace("throw", val.trace_payload)
                raise val.original.with_traceback(tb)
            if isinstance(val, _ParentThreadError):
                raise error(
                    _cross_thread_switch_error_message(
                        val.current_tid,
                        val.target_tid,
                        val.is_alive,
                    )
                ).with_traceback(tb)
            if isinstance(val, _tealet.ThreadMismatchError):
                raise error(
                    _cross_thread_switch_error_message(
                        val.current_tid,
                        val.target_tid,
                        bool(val.target_alive),
                    )
                ).with_traceback(tb)
            if isinstance(val, _tealet.TealetError):
                msg = str(val)
                # Map backend thread-mismatch wording for pending exception routing
                # to the compatibility message expected by greenlet tests.
                if (
                    "set_exception() not allowed from a different thread" in msg
                    or msg.startswith("thread mismatch:")
                ):
                    msg = "cannot switch to a different thread (which happens to have exited)"
                raise error(msg).with_traceback(tb)
        finally:
            val = tb = None


ErrorWrapper = ErrorWrapper()  # stateless singleton


def install(force=True):
    """Expose tealet.greenlet as importable top-level greenlet modules."""
    module = sys.modules[__name__]
    existing = sys.modules.get("greenlet")
    if existing is not None and existing is not module and not force:
        raise RuntimeError("greenlet is already installed from a different module")

    sys.modules["greenlet"] = module
    sys.modules["greenlet._greenlet"] = _greenlet
    return module

tealetmap = weakref.WeakValueDictionary()
_RUN_UNSET = object()
_garbage_process_guard = threading.local()
_stub_tls = threading.local()
_tracefunc = None

# Keep a strong reference to wrappers while they are actively switching.
# This guards dealloc/resurrection edge cases exercised by
# tests/compat_greenlet/test_greenlet.py::TestGreenlet::test_dealloc_switch_args_not_lost.
_running_refs = {}
_running_refcounts = {}


def gettrace():
    return _tracefunc


def _get_thread_stub():
    return getattr(_stub_tls, "stub", None)


def set_stub(create=True):
    if not isinstance(create, bool):
        raise TypeError("create must be a bool")

    old = _get_thread_stub()
    if create:
        stub = _tealet.tealet()
        stub.stub()
        _stub_tls.stub = stub
    elif hasattr(_stub_tls, "stub"):
        delattr(_stub_tls, "stub")
    return old


# tracing support.  Trace switch cargo and exceptions are wrapped so that a trace
# event can be generated after the switch.

def settrace(callback):
    if callback is not None and not callable(callback):
        raise TypeError("trace function must be callable")

    old = globals().get("_tracefunc", None)
    globals()["_tracefunc"] = callback
    return old


def _invoke_trace(event, origin, target):
    global _tracefunc
    if _tracefunc is None:
        return

    try:
        _tracefunc(event, (origin, target))
    except BaseException:
        _tracefunc = None
        raise


def _make_trace_payload(target_tealet):
    if _tracefunc is None:
        return None
    return (_tealet.current(), target_tealet)


def _consume_trace(event, trace_payload):
    if trace_payload is None:
        return
    origin_tealet, target_tealet = trace_payload
    origin = greenlet._get_or_create_wrapper(origin_tealet)
    target = greenlet._get_or_create_wrapper(target_tealet)
    _invoke_trace(event, origin, target)


def _pack_switch_transport(switch_payload, target_tealet):
    return (switch_payload, _make_trace_payload(target_tealet))


def _unpack_switch_transport(transport):
    switch_payload, trace_payload = transport
    _consume_trace("switch", trace_payload)
    return switch_payload


def _wrap_throw_for_trace(err, target_tealet):
    if not _tracefunc or isinstance(err, GreenletExit):
        return err
    trace_payload = _make_trace_payload(target_tealet)
    if trace_payload is None:
        return err
    return _TraceException(err, trace_payload)


def _pin_running(tealet, gr):
    # Reference-count pins because the same raw tealet can be re-entered
    # through nested switch paths.
    _running_refs[tealet] = gr
    _running_refcounts[tealet] = _running_refcounts.get(tealet, 0) + 1


def _unpin_running(tealet):
    # Drop the last strong pin once control has unwound out of switch/run.
    count = _running_refcounts.get(tealet, 0)
    if count <= 1:
        _running_refcounts.pop(tealet, None)
        _running_refs.pop(tealet, None)
    else:
        _running_refcounts[tealet] = count - 1


def _thread_is_alive(thread_id):
    return any(t.ident == thread_id for t in threading.enumerate())


def _is_unstarted_tealet(tealet):
    return tealet is not None and tealet.state in (_tealet.STATE_NEW, _tealet.STATE_STUB)

def getcurrent():
    gr = greenlet._get_or_create_wrapper(_tealet.current())
    # Drain per-main garbage when the owner thread re-enters greenlet APIs.
    if gr._main is gr and gr._garbage:
        gr._process_garbage()
    return gr

class greenlet(object):
    # keep internal attributes out of the instance dict
    __slots__ = (
        "_tealet",
        "parent",
        "_main",
        "_garbage",
        "_is_running",
        "run",
        "__dict__",
        "__weakref__",
    )

    @classmethod
    def _get_or_create_main_wrapper(cls, main_t=None):
        if main_t is None:
            main_t = _tealet.main()
        main_g = tealetmap.get(main_t)
        if main_g is None:
            main_g = cls(parent=main_t)
        return main_g

    @classmethod
    def _create_surrogate_wrapper(cls, raw_t, main_g):
        # Build a minimal wrapper when a live raw tealet exists but its Python
        # wrapper was collected during teardown/dealloc edge paths.
        gr = cls.__new__(cls)
        gr._tealet = raw_t
        gr.parent = main_g
        gr._main = main_g._main
        gr._garbage = []
        gr._is_running = False
        tealetmap[raw_t] = gr
        return gr

    @classmethod
    def _get_or_create_wrapper(cls, raw_t):
        # Resolve the current raw tealet to a wrapper without asserting that
        # only main can be missing from tealetmap.
        gr = tealetmap.get(raw_t)
        if gr is not None:
            return gr

        running = _running_refs.get(raw_t)
        if running is not None:
            return running

        main_t = _tealet.main()
        if main_t is raw_t:
            return cls._get_or_create_main_wrapper(main_t)

        return cls._create_surrogate_wrapper(raw_t, cls._get_or_create_main_wrapper(main_t))

    def __init__(self, run=_RUN_UNSET, parent=None):
        # must create it on this thread, not dynamically when run
        # this will bind it to the right thread
        if run is not _RUN_UNSET:
            self.run = run
        self._bootstrap(parent)
        self._is_running = False

    def __setattr__(self, name, value):
        if name != "parent":
            object.__setattr__(self, name, value)
            return

        if getattr(self, "_main", None) is self:
            raise AttributeError("cannot set the parent of a main greenlet")

        if not isinstance(value, greenlet):
            raise TypeError(
                f"GreenletChecker: Expected any type of greenlet, not {type(value).__name__}"
            )

        tealet = getattr(self, "_tealet", None)
        parent_tealet = getattr(value, "_tealet", None)
        if tealet is not None and parent_tealet is not None and tealet.thread_id != parent_tealet.thread_id:
            target_tid = parent_tealet.thread_id
            raise _ParentThreadError(
                threading.get_ident(),
                target_tid,
                any(t.ident == target_tid for t in threading.enumerate()),
            )

        p = value
        while p is not None:
            if p is self:
                raise ValueError("cyclic parent chain")
            p = object.__getattribute__(p, "parent")

        object.__setattr__(self, name, value)

    def __delattr__(self, name):
        if name == "__dict__":
            raise TypeError("can't delete __dict__")
        if name == "parent":
            raise AttributeError("can't delete attribute")
        object.__delattr__(self, name)
 
    def _bootstrap(self, parent=None):
        if isinstance(parent, _tealet.tealet):
            # main greenlet for this thread
            self._tealet = parent
            self._main = self
            object.__setattr__(self, "parent", None)  # bypass our own setattr parent rules
            self._garbage = []
        else:
            stub = _get_thread_stub()
            if stub is None:
                self._tealet = _tealet.tealet()
            else:
                self._tealet = _tealet.tealet(stub)
            if parent is None:
                parent = getcurrent()
            self.parent = parent
            self._main = parent._main
            self._main._process_garbage()
        tealetmap[self._tealet] = self
        return self._tealet

    def __del__(self):
        if getattr(self, "_main", None) is self:
            return  # we don't try to clean up main this way.
        tealet = getattr(self, "_tealet", None)
        if tealet is None:
            return
        # must be careful during teardown when module may be half cleared.
        try:
            if tealet.state == _tealet.STATE_RUN:
                if _tealet.current() == tealet:
                    # Can't kill ourselves from here
                    return
                current_wrapper = getcurrent()
                tealetmap[tealet] = self  # re-insert
                old = self.parent
                # re-parent it to ourselves.  if it fails, put it on its own main garbage heap.
                try:
                    self.parent = current_wrapper
                    self.throw()
                except (_ParentThreadError, error):
                    # This must be a foreign tealet.  Insert it to
                    # it's main tealet's garbage heap
                    self._main._garbage.append(self)
                finally:
                    self.parent = old
        except (NameError, TypeError, AttributeError):
            # we can hit those during teardown.
            return

    def __repr__(self):
        tealet = getattr(self, "_tealet", None)
        if tealet is None:
            state = "pending"
        elif _is_unstarted_tealet(tealet):
            state = "pending"
        elif self.dead:
            is_main = getattr(self, "_main", None) is self
            if is_main:
                thread_alive = _thread_is_alive(tealet.thread_id)
                if not thread_alive:
                    state = "(thread exited) dead"
                else:
                    state = "dead"
            else:
                state = "dead"
        else:
            parts = ["current" if tealet is _tealet.current() else "suspended", "active", "started"]
            if getattr(self, "_main", None) is self:
                parts.append("main")
            state = " ".join(parts)

        return (
            f"<{self.__class__.__module__}.{self.__class__.__name__} "
            f"object at 0x{id(self):x} {state}>"
        )

    def _process_garbage(self):
        if getattr(_garbage_process_guard, "active", False):
            return

        garbage = self._garbage
        if not garbage:
            return

        _garbage_process_guard.active = True
        try:
            pending = garbage[:]
            del garbage[:]
            # Resolve directly so internal cleanup does not recurse through
            # getcurrent() garbage-drain hooks.
            current = greenlet._get_or_create_wrapper(_tealet.current())
            for g in pending:
                if not g:
                    continue
                if g is current:
                    garbage.append(g)
                    continue
                old_parent = g.parent
                g.parent = current
                try:
                    g.throw()
                except error:
                    garbage.append(g)
                finally:
                    g.parent = old_parent
        finally:
            _garbage_process_guard.active = False

    @property
    def gr_frame(self):
        tealet = getattr(self, "_tealet", None)
        if tealet is None:
            return None
        if tealet is _tealet.current():
            return self._tealet.frame
        # tealet is paused.  Emulated greenlet by returning
        # the frame which called "switch" or "throw"
        f = tealet.frame
        if f:
            return f.f_back.f_back

    @property
    def gr_context(self):
        tealet = getattr(self, "_tealet", None)
        if self._is_running and tealet is not None and tealet.thread_id != threading.get_ident():
            raise ValueError("running in a different thread")
        if tealet is None:
            return None
        current = tealet.context
        if current is not None and not current:
            return None
        return current

    @gr_context.setter
    def gr_context(self, value):
        tealet = getattr(self, "_tealet", None)
        if self._is_running and tealet is not None and tealet.thread_id != threading.get_ident():
            raise ValueError("running in a different thread")

        if value is not None and not isinstance(value, contextvars.Context):
            raise TypeError("greenlet context must be a contextvars.Context or None")

        if tealet is not None:
            tealet.context = value

    @gr_context.deleter
    def gr_context(self):
        raise AttributeError("can't delete context attribute")

    @property
    def dead(self):
        tealet = getattr(self, "_tealet", None)
        if tealet is None:
            return False
        if tealet.state == _tealet.STATE_EXIT:
            return True
        if getattr(self, "_main", None) is self and not _thread_is_alive(tealet.thread_id):
            return True
        return False

    def __bool__(self):
        tealet = getattr(self, "_tealet", None)
        return tealet is not None and tealet.state == _tealet.STATE_RUN and not self.dead

    def __copy__(self):
        raise TypeError("uncopyable object")

    def __deepcopy__(self, memo):
        raise TypeError("uncopyable object")

    def switch(self, *args, **kwds):
        return self._switch_or_throw((args, kwds), None)

    def throw(self, t=None, v=None, tb=None):
        exc = greenlet._normalize_throw(t, v, tb)
        return self._switch_or_throw(None, exc)

    @staticmethod
    def _normalize_throw(t=None, v=None, tb=None):
        if not t:
            t = GreenletExit

        if tb is not None and not isinstance(tb, types.TracebackType):
            raise TypeError("throw() third argument must be a traceback object")

        if isinstance(t, BaseException):
            if v is not None:
                raise TypeError("instance exception may not have a separate value")
            exc = t
        elif isinstance(t, type) and issubclass(t, BaseException):
            if v is None:
                exc = t()
            elif isinstance(v, tuple):
                exc = t(*v)
            else:
                exc = t(v)
        else:
            raise TypeError(
                "exceptions must be classes, or instances, not %s" % (type(t).__name__,)
            )

        if tb is not None:
            exc = exc.with_traceback(tb)
        return exc

    def _switch_or_throw(self, switch_payload, err):
        with ErrorWrapper:
            tealet = self._tealet
            if tealet is None:
                tealet = self._bootstrap(self.parent)
            is_unstarted = _is_unstarted_tealet(tealet)
            payload = switch_payload
            if err is None:
                payload = _pack_switch_transport(switch_payload, tealet)
            else:
                err = _wrap_throw_for_trace(err, tealet)

            if is_unstarted:
                # getting this attribute can have side effets and run code, including switching.
                run = getattr(self, "run", _RUN_UNSET)
                if run is _RUN_UNSET:
                    raise AttributeError("run")
                _pin_running(tealet, self)
                self._is_running = True
                try:
                    try:
                        arg = tealet.run(self._greenlet_main, (run, payload, err))
                    except _tealet.StateError:
                        # A re-entrancy, caused by the getattr(self, "run") above 
                        # can cause the above to tried twice.  if we fail with a local
                        # state error, just do a normal switch or throw.
                        if _tealet.error_was_remote():
                            raise
                        if err is not None:
                            arg = tealet.throw(err)
                        else:
                            arg = tealet.switch(payload)
                finally:
                    self._is_running = False
                    _unpin_running(tealet)

                # If this greenlet just finished and its immediate parent is
                # unstarted, greenlet semantics require implicitly starting
                # that parent with our return payload.
                if tealet.state == _tealet.STATE_EXIT and arg is not None:
                    parent = self.parent
                    if parent is not None:
                        if _is_unstarted_tealet(parent._tealet):
                            # tealet.run() returns transport-shaped payload;
                            # decode before forwarding so parent receives the
                            # canonical (args, kwds) switch payload.
                            parent_payload = _unpack_switch_transport(arg)
                            return parent._switch_or_throw(parent_payload, None)
            else:
                if not self:
                    # switching to a dead greenlet, find its nearest live parent.
                    parent = self._switch_parent()
                    if parent is None:
                        # must be a different thread, unschedulable from here.
                        target_tid = tealet.thread_id
                        raise error(
                            _cross_thread_switch_error_message(
                                threading.get_ident(),
                                target_tid,
                                _thread_is_alive(target_tid),
                            )
                        )
                    if err is not None:
                        if isinstance(err, GreenletExit):
                            return err
                        return parent._switch_or_throw(None, err)
                    return parent._switch_or_throw(switch_payload, None)
                
                _pin_running(tealet, self)
                self._is_running = True
                try:
                    if err is not None:
                        arg = tealet.throw(err)
                    else:
                        arg = tealet.switch(payload)
                finally:
                    self._is_running = False
                    _unpin_running(tealet)

        # unpack the switch payload.  switch() returns differently shaped values
        # depending on how it was called.
        arg = _unpack_switch_transport(arg)
        args, kwds = arg
        if args and kwds:
            return (args, kwds)
        if kwds:
            return kwds
        if len(args) == 1:
            return args[0]
        return args

    @staticmethod
    def _greenlet_main(current, arg):
        run, switch_payload, err = arg
        current_wrapper = greenlet._get_or_create_wrapper(current)
        try:
            # Match greenlet startup ordering: clear the run attribute in the
            # target context before entering user code. If clearing triggers
            # Python callbacks that switch, control transfer semantics stay
            # consistent with upstream behavior.
            current_wrapper = greenlet._get_or_create_wrapper(current)
            try:
                del current_wrapper.run
            except AttributeError:
                pass
            finally:
                current_wrapper = None  # Don't hold strong reference to the wrapper durin user code.

            if err is not None:
                raise err
            switch_payload = _unpack_switch_transport(switch_payload)
            args, kwds = switch_payload
            # Hide trampoline frames while entering user callback so suspended
            # frame chains match greenlet-visible user code.
            result = _tealet.hide_frame(run, args, kwds)
            arg = ((result,), {})
        except GreenletExit as e:
            arg = ((e,), {})
        except BaseException as e:
            # Preserve parent-delivery semantics for uncaught worker errors by
            # queueing the exception onto the parent tealet before exit-switch.
            current_wrapper = greenlet._get_or_create_wrapper(current)
            p = current_wrapper._parent()
            wrapped = _wrap_throw_for_trace(e, p._tealet)
            p._tealet.set_exception(wrapped)
            arg = None  # ignored, 'e' is raised on other side.
        current_wrapper = current_wrapper or greenlet._get_or_create_wrapper(current)
        p = current_wrapper._parent()
        if arg is not None:
            arg = _pack_switch_transport(arg, p._tealet)
        try:
            return p._tealet, arg
        finally:
            arg = None

    def _parent(self):
        # Find the closest parent alive.
        p = self.parent
        while p is not None and not p:
            nxt = p.parent
            if nxt is p:
                break
            p = nxt
        return p

    def _switch_parent(self):
        # Dead-target switch walks up past dead parents but may switch into an
        # unstarted parent (which then starts), matching greenlet semantics.
        p = self.parent
        while p is not None and p.dead:
            nxt = p.parent
            if nxt is p:
                break
            p = nxt
        return p

    def _clear_current_context(self):
        current = contextvars.copy_context()
        for var in current:
            var.set(None)

    def _apply_context(self, ctx):
        current = contextvars.copy_context()
        target_vars = set(ctx)
        for var in current:
            if var not in target_vars:
                var.set(None)
        for var, val in ctx.items():
            var.set(val)

    getcurrent = staticmethod(getcurrent)
    error = error
    GreenletExit = GreenletExit


class UnswitchableGreenlet(greenlet):
    def __init__(self, run=_RUN_UNSET, parent=None):
        super().__init__(run=run, parent=parent)
        self.force_switch_error = False
        self.force_slp_switch_error = False

    def switch(self, *args, **kwds):
        if self.force_slp_switch_error:
            os.abort()
        if self.force_switch_error:
            tealet = getattr(self, "_tealet", None)
            if _is_unstarted_tealet(tealet):
                raise SystemError("Failed to switch stacks into a greenlet for the first time.")
            raise SystemError("Failed to switch stacks into a running greenlet.")
        return super().switch(*args, **kwds)


if not hasattr(_greenlet, "UnswitchableGreenlet"):
    _greenlet.UnswitchableGreenlet = UnswitchableGreenlet

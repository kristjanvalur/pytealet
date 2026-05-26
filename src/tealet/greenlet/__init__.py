# A greenlet emulation module using tealets
import contextvars
import threading
import weakref
import sys

import _tealet
from . import _greenlet


class error(Exception):
    pass
class GreenletExit(BaseException):
    pass

class ErrorWrapper(object):
    def __enter__(self):
        pass

    def __exit__(self, tp, val, tb):
        if isinstance(val, _tealet.TealetError):
            raise error(val).with_traceback(tb)


ErrorWrapper = ErrorWrapper()  # stateless singleton

tealetmap = weakref.WeakValueDictionary()
_RUN_UNSET = object()
# Keep a strong reference to wrappers while they are actively switching.
# This guards dealloc/resurrection edge cases exercised by
# tests/compat_greenlet/test_greenlet.py::TestGreenlet::test_dealloc_switch_args_not_lost.
_running_refs = {}
_running_refcounts = {}


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

def getcurrent():
    return greenlet._get_or_create_wrapper(_tealet.current())

class greenlet(object):
    # class defaults for attributes that remain safe if __init__ is skipped
    _is_running = False

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
 
    def _bootstrap(self, parent=None):
        if isinstance(parent, _tealet.tealet):
            # main greenlet for this thread
            self._tealet = parent
            self.parent = self  # main greenlets are their own parents and don't go away
            self._main = self
            self._garbage = []
        else:
            self._tealet = _tealet.tealet().stub()
            if not parent:
                parent = getcurrent()
            self.parent = parent
            self._main = parent._main
            self._main._process_garbage()
        tealetmap[self._tealet] = self
        return self._tealet

    def __del__(self):
        tealet = getattr(self, "_tealet", None)
        if tealet is not None and tealet.state == _tealet.STATE_RUN:
            if _tealet.current() == tealet:
                # Can't kill ourselves from here
                return
            tealetmap[tealet] = self  # re-insert
            old = self.parent
            self.parent = getcurrent()
            try:
                self.throw()
            except error:
                # This must be a foreign tealet.  Insert it to
                # it's main tealet's garbage heap
                self._main._garbage.append(self)
            finally:
                self.parent = old

    def _process_garbage(self):
        garbage = self._garbage
        if not garbage:
            return

        pending = garbage[:]
        del garbage[:]
        current = getcurrent()
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
        return tealet is not None and tealet.state == _tealet.STATE_EXIT

    def __bool__(self):
        tealet = getattr(self, "_tealet", None)
        return tealet is not None and tealet.state == _tealet.STATE_RUN

    def switch(self, *args, **kwds):
        return self._switch((False, args, kwds))

    def throw(self, t=None, v=None, tb=None):
        if not t:
            t = GreenletExit
        return self._switch((t, v, tb))

    def _switch(self, arg):
        with ErrorWrapper:
            run = getattr(self, "run", _RUN_UNSET)
            tealet = getattr(self, "_tealet", None) or self._bootstrap(getattr(self, "parent", None))
            is_unstarted = tealet.state == _tealet.STATE_STUB
            
            if is_unstarted:
                if run is _RUN_UNSET:
                    raise AttributeError("run")
                if "run" in getattr(self, "__dict__", {}):
                    del self.run
                _pin_running(tealet, self)
                self._is_running = True
                try:
                    # here we can tweak how we create the new stack
                    arg = tealet.run(self._greenlet_main, (run, arg))
                finally:
                    self._is_running = False
                    _unpin_running(tealet)
            else:
                if not self:
                    return self._parent()._switch(arg)
                _pin_running(tealet, self)
                self._is_running = True
                try:
                    arg = tealet.switch(arg)
                finally:
                    self._is_running = False
                    _unpin_running(tealet)
        return self._Result(arg)

    @staticmethod
    def _raise_triplet(err, val, tb):
        if not err:
            return

        if isinstance(err, BaseException):
            exc = err
        elif isinstance(val, BaseException):
            exc = val
        else:
            if val is None:
                exc = err()
            elif isinstance(val, tuple):
                exc = err(*val)
            else:
                exc = err(val)

        try:
            if tb is not None:
                raise exc.with_traceback(tb)
            raise exc
        finally:
            exc = None

    @staticmethod
    def _Result(arg):
        # The return value is stored in the current greenlet.
        if arg is None:
            return None
        err, args, kwds = arg
        if err:
            greenlet._raise_triplet(err, args, kwds)
        if args and kwds:
            return (args, kwds)
        elif kwds:
            return kwds
        elif args:
            if len(args) == 1:
                return args[0]
            return args
        return None

    @staticmethod
    def _greenlet_main(current, arg):
        run, (err, args, kwds) = arg
        try:
            if not err:
                result = _tealet.hide_frame(run, args, kwds)
                arg = (False, (result,), None)
            else:
                greenlet._raise_triplet(err, args, kwds)
        except GreenletExit as e:
            arg = (False, (e,), None)
        except BaseException:
            arg = sys.exc_info()
        p = greenlet._get_or_create_wrapper(current)._parent()
        try:
            return p._tealet, arg
        finally:
            arg = None

    def _parent(self):
        # Find the closest parent alive
        p = self.parent
        while not p:
            p = p.parent
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

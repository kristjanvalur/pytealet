# A greenlet emulation module using tealets
import weakref
import sys

import _tealet


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

def getcurrent():
    t = _tealet.current()
    try:
        return tealetmap[t]
    except KeyError:
        assert _tealet.main() is t
        return greenlet(parent=t)

class greenlet(object):
    def __init__(self, run=None, parent=None):
        # must create it on this thread, not dynamically when run
        # this will bind it to the right thread
        self.run = run
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

    def __del__(self):
        if self:
            if _tealet.current() == self._tealet:
                # Can't kill ourselves from here
                return
            tealetmap[self._tealet] = self  # re-insert
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
        if self._tealet is _tealet.current():
            return self._tealet.frame
        # tealet is paused.  Emulated greenlet by returning
        # the frame which called "switch" or "throw"
        f = self._tealet.frame
        if f:
            return f.f_back.f_back

    @property
    def dead(self):
        return self._tealet.state == _tealet.STATE_EXIT

    def __bool__(self):
        return self._tealet.state == _tealet.STATE_RUN

    def switch(self, *args, **kwds):
        return self._switch((False, args, kwds))

    def throw(self, t=None, v=None, tb=None):
        if not t:
            t = GreenletExit
        return self._switch((t, v, tb))

    def _switch(self, arg):
        with ErrorWrapper:
            run = getattr(self, "run", None)
            if run:
                del self.run
                # here we can tweak how we create the new stack
                arg = self._tealet.run(self._greenlet_main, (run, arg))
            else:
                if not self:
                    return self._parent()._switch(arg)
                arg = self._tealet.switch(arg)
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
        p = getcurrent()._parent()
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

    getcurrent = staticmethod(getcurrent)
    error = error
    GreenletExit = GreenletExit

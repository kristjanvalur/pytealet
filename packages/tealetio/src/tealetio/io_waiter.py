from __future__ import annotations

import threading
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar, cast

from .delivery import OpHandle
from .locks import CrossThreadEvent, Event


class InvalidStateError(Exception):
    """Raised when a waiter result is requested before completion."""


if TYPE_CHECKING:
    from .io_manager import ProactorIOManager

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)
_RawResult = TypeVar("_RawResult")
_OnLegCleanup = Callable[[], object]
_AdvanceHandler = Callable[[Any], object]
_FollowSubmit = Callable[["IOWaitLeg"], OpHandle | None]
_UnclaimedCleanup = Callable[[Any], object]

__all__ = [
    "IOHandle",
    "IOWaitChain",
    "IOWaitable",
    "IOWaiter",
    "IOWaiterSync",
]


class IOHandle:
    """Closeable continuous IO subscription (not a waitable).

    Used for long-lived callback streams such as ``poll_many``: readiness (or
    other chunks) arrive via the delivery callback; ``close()`` stops the
    stream. ``closed`` is true once a terminal ``!MORE`` delivery has been
    handed to the user callback (error, cancel, or natural end).

    Unlike ``IOWaitable``, there is no ``wait()`` / ``forget()`` lifecycle.
    Submit-time failures raise from the starter; async path errors surface on
    the callback (and then ``closed``). Idle multishot poll has no success CQE.
    """

    __slots__ = ("_close_requested", "_closed", "_handle", "_io")

    def __init__(self, io: ProactorIOManager, handle: OpHandle | None = None) -> None:
        self._io = io
        self._handle = handle
        self._closed = False
        self._close_requested = False

    def bind(self, handle: OpHandle) -> None:
        """Attach the ``OpHandle`` after submit. No-op if already closed."""

        if self._closed:
            return
        self._handle = handle

    def _mark_closed(self) -> None:
        self._closed = True
        self._handle = None

    @property
    def closed(self) -> bool:
        """True when the continuous stream has finished."""

        return self._closed

    def close(self) -> None:
        """Stop the stream if still open. Idempotent.

        For ``poll_many``, posts ``stop_poll``. Does not wait for the terminal
        CQE; ``closed`` becomes true when that ``!MORE`` delivery is ordered.
        """

        if self._closed or self._close_requested:
            return
        handle = self._handle
        if handle is None:
            return
        self._close_requested = True
        self._io.proactor.stop_poll(handle, lambda _result, _exc: None)

    def __enter__(self) -> IOHandle:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()


class IOWaitable(Protocol[T_co]):
    """Blocking IO handle with ``wait()`` / ``forget()``.

    Satisfied by ``IOWaiter`` (oneshots / ``accept_many``), ``IOWaiterSync``
    (already resolved value or exception), and ``IOWaitChain`` (a chain of
    legs). Continuous poll at the IO manager layer uses ``IOHandle``
    instead (``close()``, not ``wait()``).

    Resource-creating helpers are intended for ``wait()`` only; ``forget()`` on
    those handles is undefined.
    """

    def poll(self) -> bool:
        """Return ``True`` when ``wait()`` would return without parking the tealet."""

        ...

    def forget(self) -> None: ...

    def exception(self) -> BaseException | None:
        """Return the completion exception, or ``None`` on success (waitable done)."""

        ...

    def wait(self) -> T_co: ...


class IOWaiter(Generic[T]):
    """Blocking IO handle over a proactor callback and an opaque ``OpHandle``.

    One-shot ops return their payload from ``wait()``. Continuous accept
    (``accept_sockets`` and the handlers built on it) uses the same waiter:
    each socket goes to the delivery-thread handler; ``wait()`` blocks until
    a ``more=False`` delivery calls ``complete(None, exception)`` and returns
    ``None`` on success or raises the stored exception. Continuous
    ``poll_many`` at the IO manager returns ``IOHandle`` instead.

    Construct, pass ``complete`` as the submit callback, ``bind`` the opaque
    ``OpHandle`` (uring ``Completion``, selector oneshot token, or ``None``
    when the callback already ran). The waiter does not call ``done()`` /
    ``result()`` on the handle.

    The owning call site chooses exactly one disposition: ``wait()`` or
    ``forget()``. This layer does not enforce that contract; ``wait()`` after
    ``forget()`` is undefined. ``complete`` hands the pair to the scheduler.
    That callback stores it and then sets a plain ``Event``. The proactor
    calls ``complete`` once. ``wait()`` parks on the event. The event exists
    before submit, so a worker ``complete()`` reaches a later ``wait()``.

    Both ``wait()`` and ``forget()`` drop the waiter’s reference to the
    operation handle.

    An exceptional exit from ``wait()`` (for example ``KeyboardInterrupt`` or a
    parking timeout) posts ``cancel_nowait`` on the handle and re-raises.
    The cancel is hygiene for an operation that is still in flight. A result
    already stored by ``complete`` stays on the waiter; the exception is the
    outcome of this ``wait()``. A later ``wait()`` or ``result()`` can still
    claim that value. An optional ``cleanup`` hook runs from ``__del__`` when
    a success value was stored and never claimed. ``open`` uses it to close
    the fd. Selector backends terminalise a cancel immediately; on
    ``UringProactor`` an armed leg finishes from its own ``ECANCELED`` CQE.
    Continuous ``poll_many`` is not an ``IOWaiter`` — use ``IOHandle.close()``
    (``stop_poll``). ``has_pending_operations()`` may stay true briefly until
    cancel CQEs complete; pump the proactor when ring quiescence matters.

    For accept, ``wait()`` ends when the arm is disarmed (``more`` is false),
    not when accept-time ``recv`` legs or marshalled deliveries complete.
    Re-arm in a loop (as ``StreamServer`` does) on one-shot backends. The
    proactor handle is ``waiter._handle``.

    An optional ``map_result`` hook maps the completion value after ``complete``.
    """

    __slots__ = (
        "_claimed",
        "_cleanup",
        "_handle",
        "_io",
        "_map_result",
        "_ready",
        "_released",
        "_resolved",
    )

    def __init__(
        self,
        io: ProactorIOManager,
        *,
        map_result: Callable[[_RawResult], T] | None = None,
        cleanup: _UnclaimedCleanup | None = None,
    ) -> None:
        self._io = io
        self._handle: OpHandle | None = None
        self._map_result = map_result
        self._cleanup = cleanup
        self._claimed = False
        self._resolved: tuple[Any, BaseException | None] | None = None
        self._released = False
        self._ready = Event()

    def __del__(self) -> None:
        """Run ``cleanup`` when a success value was stored and never claimed."""

        cleanup = getattr(self, "_cleanup", None)
        if cleanup is None or getattr(self, "_claimed", True):
            return
        resolved = getattr(self, "_resolved", None)
        if resolved is None:
            return
        raw, exception = resolved
        if exception is not None:
            return
        self._claimed = True
        cleanup(raw)

    def complete(self, result: Any, exception: BaseException | None = None) -> None:
        """Proactor callback: hand the result to the scheduler thread.

        The proactor calls this once. The scheduler callback stores the pair
        and then sets the event, so a parked ``wait`` observes the result
        when it resumes.
        """

        scheduler = self._io._scheduler
        assert scheduler is not None
        scheduler.call_on_scheduler(self._deliver, result, exception)

    def _deliver(self, result: Any, exception: BaseException | None = None) -> None:
        assert self._resolved is None
        self._resolved = (result, exception)
        self._ready.set()

    def bind(self, handle: OpHandle) -> IOWaiter[T]:
        """Store the opaque ``OpHandle`` returned by the proactor submit."""

        self._handle = handle
        return self

    def forget(self) -> None:
        """Drop interest in the result; backend work continues to completion.

        Clears the waiter’s operation-handle reference. Does not cancel backend
        work.
        ``forget()`` on handles from resource-creating helpers (for example
        ``sock_accept``, ``sock_create`` with ``connect_to``,
        ``sock_create_streams``) is undefined — always ``wait()`` for those.
        """

        self._release()

    def done(self) -> bool:
        """Return True if the waiter has a completion (ignores ``forget()``)."""

        return self._resolved is not None

    def poll(self) -> bool:
        """Return ``True`` when ``wait()`` would return without parking."""

        if self._released:
            return False
        return self.done()

    def exception(self) -> BaseException | None:
        """Return the completion exception, or ``None`` on success.

        Raises ``InvalidStateError`` when the waiter has not finished.
        """

        resolved = self._resolved
        if resolved is None:
            raise InvalidStateError("IOWaiter is not finished")
        return resolved[1]

    def result(self) -> T:
        """Return the mapped result, or raise. Does not park."""

        return self._mapped_result()

    def wait(self) -> T:
        self._wait_self()
        try:
            return self._mapped_result()
        finally:
            self._release()

    def _release(self) -> None:
        """Mark consumed and drop the operation handle."""

        self._released = True
        self._handle = None

    def _cancel_nowait(self) -> None:
        """Cancel the bound op, if any, without a teardown waitable."""

        handle = self._handle
        if handle is not None:
            self._io.cancel_nowait(handle)

    def _wait_self(self) -> None:
        if self._released:
            return
        if self._resolved is not None:
            return
        if self._handle is None:
            return
        self._wait_resolved()

    def _wait_resolved(self) -> None:
        try:
            self._ready.swait()
        except BaseException:
            self._cancel_nowait()
            raise

    def _mapped_result(self) -> T:
        resolved = self._resolved
        assert resolved is not None
        raw, exception = resolved
        if exception is not None:
            raise exception
        # map_result owns a failure after this. __del__ must not close it too.
        self._claimed = True
        if self._map_result is not None:
            return self._map_result(raw)
        return cast(T, raw)


class IOWaiterSync(Generic[T]):
    """Already-resolved ``IOWaitable`` for work that never parks.

    Holds a success value or an exception without parking.
    Used when an IO helper finishes synchronously (for example direct socket
    creation in ``ProactorIOManager.sock_create``).
    """

    __slots__ = ("_exception", "_result")

    def __init__(self, result: T) -> None:
        self._result = result
        self._exception: BaseException | None = None

    @classmethod
    def failed(cls, exception: BaseException) -> IOWaiterSync[Any]:
        """Build a waitable that raises ``exception`` from ``wait()``."""

        self = object.__new__(cls)
        self._result = None  # ty: ignore[invalid-assignment]
        self._exception = exception
        return self

    def poll(self) -> bool:
        return True

    def exception(self) -> BaseException | None:
        """Return the stored exception, or ``None`` on success."""

        return self._exception

    def forget(self) -> None:
        """No-op: there is no backend work to drop interest in."""

    def wait(self) -> T:
        if self._exception is not None:
            raise self._exception
        return self._result


class IOWaitLeg:
    """One leg of an ``IOWaitChain``.

    The proactor calls ``complete`` once. The owning thread ``bind``s the
    first leg's handle after its own proactor call. A later leg is started
    with ``IOWaitChain.follow``, which submits under the chain lock.
    ``IOWaitChain.wait`` and ``forget`` drop that handle. A success is passed
    to ``advance``. An optional ``on_cleanup`` hook runs when the leg fails.
    """

    __slots__ = (
        "_advance",
        "_chain",
        "_handle",
        "_on_cleanup",
    )

    def __init__(
        self,
        chain: IOWaitChain[Any],
        *,
        on_cleanup: _OnLegCleanup | None = None,
        advance: _AdvanceHandler | None = None,
    ) -> None:
        self._chain = chain
        self._handle: OpHandle | None = None
        self._on_cleanup = on_cleanup
        self._advance = advance

    def bind(self, handle: OpHandle) -> None:
        """Store the opaque handle returned by the proactor submit."""

        self._handle = handle

    def complete(self, result: Any, exception: BaseException | None = None) -> None:
        """Proactor callback. Pass a success to ``advance``, or fail the chain.

        The proactor calls this once.
        """

        if exception is not None:
            self._fail(exception)
            return
        self._succeed(result)

    def _forget(self) -> None:
        self._handle = None

    def _fail(self, exc: BaseException) -> None:
        on_cleanup = self._on_cleanup
        if on_cleanup is not None:
            on_cleanup()
        self._chain._complete_error(exc)

    def _succeed(self, value: Any) -> None:
        advance = self._advance
        if advance is None:
            return
        try:
            advance(value)
        except BaseException as exc:
            self._chain._complete_error(exc)

    def _cancel_inflight(self) -> None:
        # same lock follow() holds across a later-leg submit, so this waits until the op is armed
        with self._chain._lock:
            handle = self._handle
            self._handle = None
        if handle is not None:
            self._chain._io.cancel_nowait(handle)


class IOWaitChain(Generic[T]):
    """Chain of IO legs with one ``CrossThreadEvent`` for the composition.

    Each leg is a proactor operation whose callback is ``complete``. Leg
    completion runs on worker threads. The chain creates a ``CrossThreadEvent``
    with itself. ``finish()`` stores the result and sets that event. ``wait()``
    parks on it. The event exists before any leg is submitted, so a worker
    ``finish()`` reaches a later ``wait()``. An abandoned ``wait()`` cancels
    the last leg, which is the one still in flight. ``finish()`` does not
    cancel. Resource-creating
    compose helpers (``sock_create`` with ``connect_to``, ``sock_connect`` with
    ``initial``, ``sock_accept`` with ``recv_size``, ``sock_create_streams``, and
    similar) are intended to be driven to completion via ``wait()`` only;
    ``forget()`` on those handles is undefined. A leg's ``advance`` hook
    receives that leg's result.
    """

    # one lock for every chain. follow() holds it across a later-leg submit so
    # cancel waits until that op is armed. RLock: a same-stack completion
    # re-enters to finish the chain or start the next leg.
    _lock = threading.RLock()

    __slots__ = ("_closed", "_completion", "_io", "_legs", "_ready")

    def __init__(
        self,
        io: ProactorIOManager,
    ) -> None:
        self._io = io
        self._closed = False
        self._completion: tuple[bool, Any] | None = None
        self._ready = CrossThreadEvent(io._scheduler)
        # the chain, in submit order. the tail is the in-flight leg.
        # wait() and forget() drop every handle.
        self._legs: list[IOWaitLeg] = []

    def new_leg(
        self,
        *,
        on_cleanup: _OnLegCleanup | None = None,
        advance: _AdvanceHandler | None = None,
    ) -> IOWaitLeg:
        """Track a leg before its proactor submit.

        ``leg.complete`` is the submit callback. The owning thread then
        ``bind``s the returned handle. The leg is already on the chain, so an
        eager completion runs ``advance`` while that leg is tracked. A later
        leg uses ``follow`` instead of ``new_leg`` plus ``bind``.
        """

        with self._lock:
            if self._closed or self._completion is not None:
                raise RuntimeError("IOWaitChain is closed")
            leg = IOWaitLeg(
                self,
                on_cleanup=on_cleanup,
                advance=advance,
            )
            self._legs.append(leg)
        return leg

    def follow(
        self,
        submit: _FollowSubmit,
        *,
        on_cleanup: _OnLegCleanup | None = None,
        advance: _AdvanceHandler | None = None,
    ) -> None:
        """Start the next leg from a completion.

        Creates the leg and runs ``submit(leg)`` while the chain lock is held,
        then stores the returned handle. A cancel from another thread waits
        until that returns. If the chain was already closed, cancel that
        handle once the lock is released. ``on_cleanup`` also runs when this
        raises: a closed chain refuses the leg before it exists, and a
        proactor call that raises never calls ``complete``.
        """

        to_cancel: OpHandle | None = None
        try:
            leg = self.new_leg(on_cleanup=on_cleanup, advance=advance)
            with self._lock:
                handle = submit(leg)
                if not self._closed:
                    leg._handle = handle
                    return
                # wait() already looked. leave the handle unset so it does not cancel too.
                to_cancel = handle
        except BaseException:
            if on_cleanup is not None:
                on_cleanup()
            raise
        if to_cancel is None:
            return
        proactor = self._io._proactor
        if proactor is None:
            return
        proactor.cancel_nowait(to_cancel)

    def finish(self, result: T) -> bool:
        """Mark the chain successful and wake a blocked ``wait()``.

        Returns ``False`` when ``wait()`` has already ended or delivery was
        rejected (for example after an interrupted wait); the caller must
        discard ``result`` (close sockets, streams, and similar).
        """

        return self._complete(ok=True, value=result)

    def _complete_error(self, exc: BaseException) -> None:
        self._complete(ok=False, value=exc)

    def _complete(self, *, ok: bool, value: Any) -> bool:
        with self._lock:
            if self._closed or self._completion is not None:
                return False
            self._completion = (ok, value)
        self._ready.set()
        return True

    def _cancel_leg(self, leg: IOWaitLeg) -> None:
        if self._io._proactor is None:
            return
        leg._cancel_inflight()

    def forget(self) -> None:
        """Drop interest in the chain's result; backend compose work keeps running.

        Drops every leg's op handle. Does not set ``_closed`` or cancel the
        in-flight leg. A later advance hook may still call ``new_leg()``.

        Undefined for resource-creating compose handles — always ``wait()`` for
        those (see ``IOWaitChain`` class docstring).
        """

        self._release_legs()

    def _release_legs(self) -> None:
        """Drop every leg's op handle. ``wait`` and ``forget`` both end here."""

        with self._lock:
            legs = self._legs
            self._legs = []
        for leg in legs:
            leg._forget()

    def poll(self) -> bool:
        """Return ``True`` when the chain has finished."""

        return self._completion is not None

    def exception(self) -> BaseException | None:
        """Return the completion exception, or ``None`` on success.

        Call after the chain has finished.
        """

        completion = self._completion
        assert completion is not None
        ok, value = completion
        if ok:
            return None
        return cast(BaseException, value)

    def wait(self) -> T:
        """Block until the chain completes.

        Call once per handle. A second successful ``wait()`` returns the same
        result (compose helpers may hand back sockets or streams — avoid
        double-close). Exceptional exit cancels the in-flight leg; ``forget()``
        does not. Either disposition drops the legs' op handles.
        """

        try:
            return self._wait_once()
        finally:
            self._release_legs()

    def _wait_once(self) -> T:
        completion = self._completion
        if completion is not None:
            ok, value = completion
            if not ok:
                raise value
            return cast(T, value)

        try:
            self._ready.swait()
        except BaseException:
            with self._lock:
                if self._completion is not None:
                    completion = self._completion
                    tail = None
                else:
                    self._closed = True
                    completion = None
                    tail = self._legs[-1] if self._legs else None
            if completion is None:
                if tail is not None:
                    self._cancel_leg(tail)
                raise
            completion = self._completion
        else:
            completion = self._completion

        assert completion is not None
        ok, value = completion
        if not ok:
            raise value
        return cast(T, value)

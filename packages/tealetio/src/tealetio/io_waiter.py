from __future__ import annotations

import threading
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Protocol, TypeVar, cast

from .locks import CrossThreadEvent
from .operations import InvalidStateError, OpHandle, SupportsOperation, is_io_cancellation

_VoidDoneCallback = Callable[[], object]

if TYPE_CHECKING:
    from .io_manager import ProactorIOManager

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)
_RawResult = TypeVar("_RawResult")
_OnLegCleanup = Callable[[bool, Any], object]
_AdvanceHandler = Callable[["IOWaitGroupChild[Any]"], object]


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

    Satisfied by ``IOWaiter`` (callback-mode oneshots / ``accept_many``),
    ``IOWaiterSync`` (already resolved value or exception), and ``IOWaitGroup``
    (composed multi-leg work). Continuous poll at the IO manager layer uses
    ``IOHandle`` instead (``close()``, not ``wait()``).

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

    def add_done_callback(self, callback: _VoidDoneCallback) -> None:
        """Register ``callback`` to run when the waitable completes."""

        ...

    def wait(self) -> T_co: ...


class IOWaitGroupChildProtocol(Protocol[T_co]):
    """Grouped leg handle; exposes this step's result to advance handlers."""

    def value(self) -> T_co: ...


class IOOperation(Protocol[T_co]):
    """User-facing IO handle; call ``wait()`` to block for the result."""

    def done(self) -> bool: ...

    def cancelled(self) -> bool: ...

    def wait(self) -> T_co: ...


class IOWaiter(Generic[T]):
    """Blocking IO handle over a proactor callback and an opaque ``OpHandle``.

    One-shot ops return their payload from ``wait()``. Continuous
    ``accept_many`` uses the same waiter: chunks go to the user callback;
    ``wait()`` blocks until ``CountFinalizer`` settles this waiter
    (``accept(None, exception)``) and returns ``None`` on success or raises
    the stored exception. Continuous ``poll_many`` at the IO manager returns
    ``IOHandle`` instead.

    Construct, pass ``accept`` as the submit callback, ``bind`` the opaque
    ``OpHandle`` (uring ``Completion``, selector ``Operation``, or
    ``None`` when the callback already ran). The waiter does not call
    ``done()`` / ``result()`` on the handle.

    The owning call site chooses exactly one disposition: ``wait()`` or
    ``forget()``. This layer does not enforce that contract; ``wait()`` after
    ``forget()`` is undefined.

    Both ``wait()`` and ``forget()`` drop the waiter’s reference to the
    operation handle.

    An exceptional exit from ``wait()`` (for example ``KeyboardInterrupt`` or a
    parking timeout) posts ``cancel_nowait`` on the handle: selector backends
    terminalise immediately; on ``UringProactor`` armed legs finish from
    their own ``ECANCELED`` CQE. Continuous ``poll_many`` is not an
    ``IOWaiter`` — use ``IOHandle.close()`` (``stop_poll``).
    ``has_pending_operations()`` may stay true briefly until cancel CQEs
    complete; pump the proactor when ring quiescence matters.

    For ``accept_many``, ``wait()`` ends when the accept **stream** finishes,
    not when accept-time ``recv`` legs or marshalled deliveries complete.
    Re-arm in a loop (as ``StreamServer`` does) on one-shot backends. The
    proactor handle is ``waiter._handle``.

    An optional ``map_result`` hook maps the completion value after ``accept``.
    """

    __slots__ = ("_callbacks", "_handle", "_io", "_map_result", "_released", "_resolved")
    _lock: ClassVar[threading.Lock] = threading.Lock()

    def __init__(
        self,
        io: ProactorIOManager,
        *,
        map_result: Callable[[_RawResult], T] | None = None,
    ) -> None:
        self._io = io
        self._handle: OpHandle | None = None
        self._map_result = map_result
        self._resolved: tuple[Any, BaseException | None] | None = None
        self._callbacks: list[_VoidDoneCallback] = []
        self._released = False

    def accept(self, result: Any, exception: BaseException | None = None) -> None:
        """Proactor callback: finish this waiter. Safe on a worker thread."""

        with self._lock:
            if self._resolved is not None:
                return
            self._resolved = (result, exception)
            callbacks = self._callbacks
            self._callbacks = []
        for callback in callbacks:
            callback()

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

    def cancelled(self) -> bool:
        """Return ``True`` when the operation completed by cancellation."""

        resolved = self._resolved
        if resolved is None:
            raise InvalidStateError("IOWaiter is not finished")
        return is_io_cancellation(resolved[1])

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

    def add_done_callback(self, callback: _VoidDoneCallback) -> None:
        """Register ``callback`` to run when the waiter completes.

        Call after the IO helper returns the waiter so completion cannot run
        before the caller holds the handle. If already done, ``callback``
        runs before ``add_done_callback`` returns.
        """

        with self._lock:
            if self._resolved is not None:
                run_now = True
            else:
                self._callbacks.append(callback)
                run_now = False
        if run_now:
            callback()

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

    def _wait_self(self) -> None:
        if self._released:
            return
        if self._resolved is not None:
            return
        if self._handle is None:
            return
        self._wait_resolved()

    def _wait_resolved(self) -> None:
        with self._lock:
            if self._resolved is not None:
                return
            ready = CrossThreadEvent(self._io._scheduler)  # type: ignore[arg-type]

            def wake() -> None:
                ready.set()

            self._callbacks.append(wake)
        try:
            ready.swait()
        except BaseException:
            with self._lock:
                done = self._resolved is not None
                if not done:
                    try:
                        self._callbacks.remove(wake)
                    except ValueError:
                        pass
            if done:
                return
            handle = self._handle
            if handle is not None:
                self._io.cancel_nowait(handle)
            raise

    def _mapped_result(self) -> T:
        resolved = self._resolved
        assert resolved is not None
        raw, exception = resolved
        if exception is not None:
            raise exception
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

    def cancelled(self) -> bool:
        """Return ``False``; sync waitables never complete by cancellation."""

        return False

    def exception(self) -> BaseException | None:
        """Return the stored exception, or ``None`` on success."""

        return self._exception

    def forget(self) -> None:
        """No-op: there is no backend work to drop interest in."""

    def add_done_callback(self, callback: _VoidDoneCallback) -> None:
        """Run ``callback`` immediately on the caller's stack.

        Unlike proactor completions (which usually hop via the scheduler), this
        re-enters the callback synchronously. Handlers that submit more IO must
        tolerate nested completion on the same stack.
        """

        callback()

    def wait(self) -> T:
        if self._exception is not None:
            raise self._exception
        return self._result


class IOWaitGroupChild(Generic[T]):
    """One leg of a grouped wait; links a waitable back to the parent group.

    ``value()`` is one-shot: it returns this leg's resolved result and clears the
    cached copy. An optional ``on_cleanup(fail, value)`` hook runs when the
    operation fails on a worker thread (``fail=True``, ``value=None``) or when a
    still-unreleased success result is dropped on exceptional ``wait()`` exit or
    from ``__del__`` (``fail=False``).
    """

    __slots__ = (
        "_advance",
        "_group",
        "_on_cleanup",
        "_operation",
        "_resolved_value",
    )

    def __init__(
        self,
        group: IOWaitGroup[Any],
        operation: SupportsOperation[Any] | IOWaiter[Any],
        *,
        on_cleanup: _OnLegCleanup | None = None,
        advance: _AdvanceHandler | None = None,
    ) -> None:
        self._group = group
        self._operation: SupportsOperation[Any] | IOWaiter[Any] | None = operation
        self._on_cleanup = on_cleanup
        self._advance = advance
        self._resolved_value: tuple[T] | None = None

    def _arm(self) -> None:
        """Register the done callback after the leg is tracked on the parent group."""

        operation = self._operation
        if operation is None:
            return
        if isinstance(operation, IOWaiter):
            operation.add_done_callback(lambda: self._on_done(operation))
            return
        operation.add_done_callback(self._on_done)

    def value(self) -> T:
        """Return this leg's result once; clears the cached copy."""

        cached = self._resolved_value
        if cached is not None:
            self._resolved_value = None
            return cached[0]
        operation = self._operation
        if operation is not None and not operation.done():
            raise InvalidStateError("IOWaitGroupChild value is not ready")
        raise InvalidStateError("IOWaitGroupChild value already consumed")

    def _notify_cleanup(self, *, fail: bool, value: Any | None) -> None:
        on_cleanup = self._on_cleanup
        if on_cleanup is not None:
            on_cleanup(fail, value)

    def _cleanup_unresolved_value(self) -> None:
        cached = self._resolved_value
        if cached is None:
            return
        self._resolved_value = None
        self._notify_cleanup(fail=False, value=cached[0])

    def __del__(self) -> None:
        self._cleanup_unresolved_value()

    def _forget(self) -> None:
        self._operation = None

    def _on_done(self, operation: SupportsOperation[Any] | IOWaiter[Any]) -> None:
        try:
            self._resolved_value = (cast(T, operation.result()),)
        except BaseException as exc:
            self._notify_cleanup(fail=True, value=None)
            self._group._complete_error(exc)
            return
        self._operation = None
        advance = self._advance
        if advance is None:
            return
        try:
            advance(self)
        except BaseException as exc:
            self._group._complete_error(exc)


class IOWaitGroup(Generic[T]):
    """Grouped IO wait with a single ``CrossThreadEvent`` park for the composition.

    Active work is tracked as ``IOWaitGroupChild`` legs and/or bare waitables
    (``SupportsOperation``). Leg completion runs on worker threads; ``finish()``
    unblocks one ``wait()`` on the group. Resource-creating compose helpers
    (``sock_create`` with ``connect_to``, ``sock_connect`` with ``initial``,
    ``sock_accept`` with ``recv_size``, ``sock_create_streams``, and similar) are
    intended to be driven to completion via ``wait()`` only; ``forget()`` on
    those handles is undefined. Child legs expose ``value()`` for one-shot
    handoff of raw operation results into advance handlers.
    """

    __slots__ = ("_closed", "_completion", "_done_callbacks", "_io", "_lock", "_members", "_ready")

    def __init__(
        self,
        io: ProactorIOManager,
    ) -> None:
        self._io = io
        self._lock = threading.Lock()
        self._closed = False
        self._completion: tuple[bool, Any] | None = None
        self._ready: CrossThreadEvent | None = None
        self._members: set[IOWaitGroupChild[Any]] = set()
        self._done_callbacks: list[_VoidDoneCallback] = []

    def attach(
        self,
        operation: SupportsOperation[Any] | IOWaiter[Any],
        *,
        on_cleanup: _OnLegCleanup | None = None,
        advance: _AdvanceHandler | None = None,
    ) -> IOWaitGroupChild[Any]:
        """Register an operation or callback-mode ``IOWaiter`` leg."""

        with self._lock:
            if self._closed or self._completion is not None:
                proactor = self._io._proactor
                if proactor is not None:
                    self._io.cancel_nowait(operation)
                raise RuntimeError("IOWaitGroup is closed")
            child = IOWaitGroupChild(
                self,
                operation,
                on_cleanup=on_cleanup,
                advance=advance,
            )
            self._members.add(child)
        child._arm()
        return child

    def finish(self, result: T) -> bool:
        """Mark the grouped composition successful and wake a blocked ``wait()``.

        Returns ``False`` when ``wait()`` has already ended or delivery was
        rejected (for example after an interrupted wait); the caller must
        discard ``result`` (close sockets, streams, and similar).
        """

        return self._complete(ok=True, value=result)

    def _complete_error(self, exc: BaseException) -> None:
        with self._lock:
            if self._closed or self._completion is not None:
                return
            members = tuple(self._members)
        self._cleanup_members(members)
        self._complete(ok=False, value=exc)

    def _cleanup_members(self, members: tuple[IOWaitGroupChild[Any], ...] | None = None) -> None:
        if members is None:
            with self._lock:
                members = tuple(self._members)
        for member in members:
            member._cleanup_unresolved_value()

    def _complete(self, *, ok: bool, value: Any) -> bool:
        ready: CrossThreadEvent | None
        cancel_members: tuple[IOWaitGroupChild[Any], ...] = ()
        callbacks: list[_VoidDoneCallback]
        with self._lock:
            if self._closed or self._completion is not None:
                return False
            self._completion = (ok, value)
            if not ok:
                cancel_members = tuple(self._members)
            self._members.clear()
            ready = self._ready
            callbacks = self._done_callbacks
            self._done_callbacks = []
        if cancel_members:
            self._cancel_members(cancel_members)
        if ready is not None:
            ready.set()
        for callback in callbacks:
            callback()
        return True

    def _cancel_members(self, members: tuple[IOWaitGroupChild[Any], ...]) -> None:
        proactor = self._io._proactor
        if proactor is None:
            return
        for member in members:
            operation = member._operation
            if operation is not None and not operation.done():
                self._io.cancel_nowait(operation)

    def forget(self) -> None:
        """Drop interest in the grouped result; backend compose work keeps running.

        Clears member tracking and breaks waiter references so the chain can
        continue without a blocked ``wait()``. Does not set ``_closed`` or
        cancel in-flight legs — later advance hooks may still ``attach()``
        successfully.

        Undefined for resource-creating compose handles — always ``wait()`` for
        those (see ``IOWaitGroup`` class docstring).
        """

        for member in self._members:
            member._forget()
        self._members.clear()

    def poll(self) -> bool:
        """Return ``True`` when the grouped composition has finished."""

        return self._completion is not None

    def exception(self) -> BaseException | None:
        """Return the completion exception, or ``None`` on success.

        Only call after the group is done (for example from a done callback).
        """

        completion = self._completion
        assert completion is not None
        ok, value = completion
        if ok:
            return None
        return cast(BaseException, value)

    def add_done_callback(self, callback: _VoidDoneCallback) -> None:
        """Register ``callback`` to run when the grouped composition completes."""

        with self._lock:
            if self._completion is not None:
                run_now = True
            else:
                self._done_callbacks.append(callback)
                run_now = False
        if run_now:
            callback()

    def wait(self) -> T:
        """Block until the grouped composition completes.

        Call once per handle. A second successful ``wait()`` returns the same
        result (compose helpers may hand back sockets or streams — avoid
        double-close). Exceptional exit cancels tracked legs; ``forget()`` does
        not.
        """

        completion = self._completion
        if completion is not None:
            ok, value = completion
            if not ok:
                raise value
            return cast(T, value)

        ready = CrossThreadEvent(self._io._scheduler)  # type: ignore[arg-type]
        # lazy ready: publish _ready under lock and re-check completion so a
        # racing finish() cannot complete before the waiter is armed
        with self._lock:
            completion = self._completion
            if completion is None:
                self._ready = ready
            else:
                ok, value = completion
                if not ok:
                    raise value
                return cast(T, value)

        try:
            ready.swait()
        except BaseException:
            with self._lock:
                if self._completion is not None:
                    completion = self._completion
                    members = ()
                else:
                    self._closed = True
                    members = tuple(self._members)
            if completion is None:
                self._cancel_members(members)
                self._cleanup_members(members)
                raise
            completion = self._completion
        else:
            completion = self._completion

        assert completion is not None
        ok, value = completion
        if not ok:
            raise value
        return cast(T, value)

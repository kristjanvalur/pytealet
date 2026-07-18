from __future__ import annotations

import threading
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar, cast

from .locks import CrossThreadEvent
from .operations import InvalidStateError, Operation, SupportsOperation

_VoidDoneCallback = Callable[[], object]

if TYPE_CHECKING:
    from .io_manager import ProactorIOManager

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)
_RawResult = TypeVar("_RawResult")
_OnLegCleanup = Callable[[bool, Any], object]
_AdvanceHandler = Callable[["IOWaitGroupChild[Any]"], object]


class IOWaitable(Protocol[T_co]):
    """Blocking IO handle with ``wait()`` / ``forget()``.

    Satisfied by ``IOWaiter`` (proactor ``Operation``), ``IOWaiterSync`` (already
    resolved value or exception), and ``IOWaitGroup`` (composed multi-leg work).

    ``IOWaiter`` wraps one-shot and continuous ``Operation`` objects (including
    ``ContinuousOperation`` backends that complete with ``None`` after streaming
    results through their result callback).

    Resource-creating helpers are intended for ``wait()`` only; ``forget()`` on
    those handles is undefined.
    """

    def poll(self) -> bool:
        """Return ``True`` when ``wait()`` would return without parking the tealet."""

        ...

    def forget(self) -> None: ...

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
    """Blocking IO handle backed by a proactor waitable (``SupportsOperation``).

    One-shot ops return their payload from ``wait()``. Continuous ops (``recv_many``,
    ``accept_many``, ``poll_many``, and similar) stream chunks through the operation
    result callback; ``wait()`` blocks until the continuous op finishes and returns
    ``None`` on success or raises the stored exception. Backends may return
    ``Operation`` / ``ContinuousOperation`` or a duck-typed equivalent.

    The owning call site chooses exactly one disposition: ``wait()`` or
    ``forget()``. This layer does not enforce that contract; ``wait()`` after
    ``forget()`` is undefined.

    Both ``wait()`` and ``forget()`` drop the waiter’s reference to the
    underlying waitable. When the waitable is already terminal, ``UringProactor``
    may freelist one-shot and most continuous ops. ``poll_many`` is never pooled
    (late CQEs after stop). In-flight ops left by ``forget()`` are not recycled
    here; that is acceptable.

    An exceptional exit from ``wait()`` (for example ``KeyboardInterrupt`` or a
    parking timeout) routes cancellation through
    ``ProactorIOManager._cancel_operation(...).forget()``: selector backends
    terminalise the target immediately; on ``UringProactor`` armed recv/accept
    legs finish from their own ``ECANCELED`` CQE, while multishot ``poll_many``
    terminalises as soon as ``submit_poll_remove`` is posted. The teardown leg
    is not awaited. ``has_pending_operations()`` may stay true briefly until
    cancel / poll_remove CQEs complete; pump the proactor or ``wait()`` on the
    teardown operation when ring quiescence matters.

    For ``accept_many`` / ``poll_many``, ``wait()`` ends when the underlying
    accept or poll **stream** finishes, not when accept-time ``recv`` legs or
    marshalled deliveries complete. Re-arm in a loop (as ``StreamServer`` does) on
    one-shot backends; use ``waiter.operation`` when the raw waitable handle
    is needed (only while the waiter still holds it — before ``wait`` / ``forget``).

    An optional ``map_result`` hook maps the operation result after completion.
    """

    __slots__ = ("_io", "_operation", "_map_result")

    def __init__(
        self,
        io: ProactorIOManager,
        operation: SupportsOperation[_RawResult],
        *,
        map_result: Callable[[_RawResult], T] | None = None,
    ) -> None:
        self._io = io
        self._operation: SupportsOperation[Any] | None = operation
        self._map_result = map_result

    @property
    def operation(self) -> SupportsOperation[Any] | None:
        """Underlying proactor waitable, when the waiter still holds a reference."""

        return self._operation

    def forget(self) -> None:
        """Drop interest in the result; backend work continues to completion.

        Clears the waiter’s waitable reference. If the waitable is already
        finished, the proactor may recycle it. Does not cancel backend work.
        ``forget()`` on handles from resource-creating helpers (for example
        ``sock_accept``, ``sock_create`` with ``connect_to``,
        ``sock_create_streams``) is undefined — always ``wait()`` for those.
        """

        self._release_operation()

    def poll(self) -> bool:
        """Return ``True`` when the underlying operation has completed."""

        operation = self._operation
        if operation is None:
            return False
        return operation.done()

    def cancelled(self) -> bool:
        """Return ``True`` when the operation completed by cancellation."""

        operation = self._operation
        if operation is None:
            raise InvalidStateError("IOWaiter has no operation")
        return operation.cancelled()

    def exception(self) -> BaseException | None:
        """Return the completion exception, or ``None`` on success.

        Raises ``InvalidStateError`` when the operation has not finished.
        """

        operation = self._operation
        if operation is None:
            raise InvalidStateError("IOWaiter has no operation")
        return operation.exception()

    def add_done_callback(self, callback: _VoidDoneCallback) -> None:
        """Register ``callback`` to run when the operation completes.

        Call after the IO helper returns the waiter so completion cannot run
        before the caller holds the handle. If the operation is already done,
        ``callback`` runs before ``add_done_callback`` returns.
        """

        operation = self._operation
        if operation is None:
            raise InvalidStateError("IOWaiter has no operation")
        operation.add_done_callback(lambda _op: callback())

    def wait(self) -> T:
        self._wait_self()
        try:
            return self._resolved()
        finally:
            self._release_operation()

    def _release_operation(self) -> None:
        """Drop the waitable ref; recycle into the proactor freelist when terminal."""

        operation = self._operation
        self._operation = None
        if operation is None:
            return
        # ProactorBase no-ops; UringProactor freelists finished one-shot and
        # non-poll_many continuous ops when terminal and not ring-live.
        self._io.proactor.recycle_operation(operation)

    def _wait_self(self) -> None:
        operation = self._operation
        if operation is None:
            return
        if operation.done():
            return
        ready = CrossThreadEvent(self._io._scheduler)  # type: ignore[arg-type]

        def wake(_op: Operation[Any]) -> None:
            ready.set()

        operation.add_done_callback(wake)
        try:
            ready.swait()
        except BaseException:
            operation.remove_done_callback(wake)
            if operation.done():
                return
            self._io._cancel_operation(operation).forget()
            raise

    def _resolved(self) -> T:
        operation = self._operation
        assert operation is not None
        raw = operation.result()
        if self._map_result is not None:
            return self._map_result(raw)
        return cast(T, raw)


class IOWaiterSync(Generic[T]):
    """Already-resolved ``IOWaitable`` for work that never parks.

    Holds a success value or an exception without a proactor ``Operation``.
    Used when an IO helper finishes synchronously (for example direct socket
    creation in ``ProactorIOManager.sock_create``).
    """

    __slots__ = ("_result", "_exception")

    def __init__(self, result: T) -> None:
        self._result = result
        self._exception: BaseException | None = None

    @classmethod
    def failed(cls, exception: BaseException) -> IOWaiterSync[Any]:
        """Build a waitable that raises ``exception`` from ``wait()``."""

        self = object.__new__(cls)
        self._result = None
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
        """Run ``callback`` immediately; this waitable is already complete."""

        callback()

    def wait(self) -> T:
        if self._exception is not None:
            raise self._exception
        return self._result


class IOWaitGroupChild(Generic[T]):
    """One leg of a grouped wait; links an ``Operation`` back to the parent group.

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
        group: "IOWaitGroup[Any]",
        operation: Operation[Any],
        *,
        on_cleanup: _OnLegCleanup | None = None,
        advance: _AdvanceHandler | None = None,
    ) -> None:
        self._group = group
        self._operation = operation
        self._on_cleanup = on_cleanup
        self._advance = advance
        self._resolved_value: tuple[T] | None = None

    def _arm(self) -> None:
        """Register the done callback after the leg is tracked on the parent group."""

        operation = self._operation
        if operation is not None:
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

    def _on_done(self, operation: Operation[Any]) -> None:
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

    Active work is tracked as ``IOWaitGroupChild`` legs and/or bare ``Operation``
    objects. Leg completion runs on worker threads; ``finish()`` unblocks one
    ``wait()`` on the group. Resource-creating compose helpers (``sock_create``
    with ``connect_to``, ``sock_connect`` with ``initial``, ``sock_accept`` with
    ``recv_size``, ``sock_create_streams``, and similar) are intended to be
    driven to completion via ``wait()`` only; ``forget()`` on those handles is
    undefined. Child legs expose ``value()`` for one-shot handoff of raw
    operation results into advance handlers.
    """

    __slots__ = ("_closed", "_completion", "_done_callbacks", "_io", "_lock", "_members", "_ready")

    def __init__(
        self,
        io: "ProactorIOManager",
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
        operation: Operation[Any],
        *,
        on_cleanup: _OnLegCleanup | None = None,
        advance: _AdvanceHandler | None = None,
    ) -> IOWaitGroupChild[Any]:
        """Register an operation leg that may expose a ``value()`` to advance hooks."""

        with self._lock:
            if self._closed or self._completion is not None:
                self._io._cancel_operation(operation).forget()
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
        for member in members:
            operation = member._operation
            if operation is not None and not operation.done():
                self._io._cancel_operation(operation).forget()

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
        except BaseException as exc:
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
                raise exc
            completion = self._completion
        else:
            completion = self._completion

        assert completion is not None
        ok, value = completion
        if not ok:
            raise value
        return cast(T, value)

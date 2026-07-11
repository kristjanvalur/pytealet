from __future__ import annotations

import threading
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar, cast

from .locks import ThreadsafeEvent
from .operations import InvalidStateError, Operation

if TYPE_CHECKING:
    from .io_manager import ProactorIOManager

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)
_RawResult = TypeVar("_RawResult")
_OnLegCleanup = Callable[[bool, Any], object]
_AdvanceHandler = Callable[["IOWaitGroupChild[Any]"], object]


class IOWaitable(Protocol[T_co]):
    """Blocking IO handle with ``wait()`` / ``forget()``; satisfied by ``IOWaiter`` and ``IOWaitGroup``.

    Resource-creating helpers are intended for ``wait()`` only; ``forget()`` on
    those handles is undefined.
    """

    def forget(self) -> None: ...

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
    """Single-shot IO completion backed by a proactor ``Operation``.

    The owning call site chooses exactly one disposition: ``wait()`` or
    ``forget()``. This layer does not enforce that contract; ``wait()`` after
    ``forget()`` is undefined. An exceptional exit from ``wait()`` (for example
    a timeout) cancels the underlying operation.

    An optional ``map_result`` hook maps the operation result after completion.
    """

    __slots__ = ("_io", "_operation", "_map_result")

    def __init__(
        self,
        io: ProactorIOManager,
        operation: Operation[_RawResult],
        *,
        map_result: Callable[[_RawResult], T] | None = None,
    ) -> None:
        self._io = io
        self._operation: Operation[Any] | None = operation
        self._map_result = map_result

    def forget(self) -> None:
        """Drop interest in the result; backend work continues to completion.

        Mostly breaks reference cycles with completion callbacks by nulling
        ``_operation``. Does not cancel backend work. ``forget()`` on handles
        from resource-creating helpers (for example ``sock_accept``,
        ``sock_create`` with ``connect_to``, ``sock_create_streams``) is
        undefined — always ``wait()`` for those.
        """

        self._operation = None

    def wait(self) -> T:
        self._wait_self()
        try:
            return self._resolved()
        finally:
            self._operation = None

    def _wait_self(self) -> None:
        operation = self._operation
        if operation is None:
            return
        if operation.done():
            return
        ready = ThreadsafeEvent(self._io._scheduler)  # type: ignore[arg-type]

        def wake(_op: Operation[Any]) -> None:
            ready.set()

        operation.add_done_callback(wake)
        try:
            ready.swait()
        except BaseException:
            operation.remove_done_callback(wake)
            if operation.done():
                return
            operation.cancel()
            raise

    def _resolved(self) -> T:
        operation = self._operation
        assert operation is not None
        raw = operation.result()
        if self._map_result is not None:
            return self._map_result(raw)
        return cast(T, raw)


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
    """Grouped IO wait with a single ``ThreadsafeEvent`` park for the composition.

    Active work is tracked as ``IOWaitGroupChild`` legs and/or bare ``Operation``
    objects. Leg completion runs on worker threads; ``finish()`` unblocks one
    ``wait()`` on the group. Resource-creating compose helpers (``sock_create``
    with ``connect_to``, ``sock_connect`` with ``initial``, ``sock_accept`` with
    ``recv_size``, ``sock_create_streams``, and similar) are intended to be
    driven to completion via ``wait()`` only; ``forget()`` on those handles is
    undefined. Child legs expose ``value()`` for one-shot handoff of raw
    operation results into advance handlers.
    """

    __slots__ = ("_closed", "_completion", "_io", "_lock", "_members", "_ready")

    def __init__(
        self,
        io: "ProactorIOManager",
    ) -> None:
        self._io = io
        self._lock = threading.Lock()
        self._closed = False
        self._completion: tuple[bool, Any] | None = None
        self._ready: ThreadsafeEvent | None = None
        self._members: set[IOWaitGroupChild[Any]] = set()

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
                operation.cancel()
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
        ready: ThreadsafeEvent | None
        cancel_members: tuple[IOWaitGroupChild[Any], ...] = ()
        with self._lock:
            if self._closed or self._completion is not None:
                return False
            self._completion = (ok, value)
            if not ok:
                cancel_members = tuple(self._members)
            self._members.clear()
            ready = self._ready
        if cancel_members:
            self._cancel_members(cancel_members)
        if ready is not None:
            ready.set()
        return True

    def _cancel_members(self, members: tuple[IOWaitGroupChild[Any], ...]) -> None:
        for member in members:
            operation = member._operation
            if operation is not None and not operation.done():
                operation.cancel()

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

    def wait(self) -> T:
        with self._lock:
            completion = self._completion

        if completion is not None:
            ok, value = completion
            if not ok:
                raise value
            return cast(T, value)

        ready = ThreadsafeEvent(self._io._scheduler)  # type: ignore[arg-type]
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

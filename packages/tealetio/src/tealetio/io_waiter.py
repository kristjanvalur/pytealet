from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar, cast

from .locks import ThreadsafeEvent
from .operations import Operation

if TYPE_CHECKING:
    from .io_manager import ProactorIOManager

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)
_RawResult = TypeVar("_RawResult")
_CreateNext = Callable[["IOWaiterChainableProtocol[Any]"], "IOWaiterProtocol[Any]"]
_OnCleanup = Callable[[T], None]
_OnLegCleanup = Callable[[bool, Any], object]
_AdvanceHandler = Callable[["IOWaitGroupChild[Any]"], object]
_OnCompleteHandler = Callable[[], object]
_ActiveMember = Operation[Any] | "IOWaitGroupChild[Any]"


class IOWaiterProtocol(Protocol[T_co]):
    """Blocking IO handle returned by one-shot helpers and chain links."""

    def forget(self) -> None: ...

    def wait(self) -> T_co: ...


class IOWaiterChainableProtocol(IOWaiterProtocol[T_co], Protocol[T_co]):
    """Chain parent handle; exposes this step's result to ``create_next``."""

    def value(self) -> T_co: ...


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

        Caveat emptor: mostly breaks reference cycles with completion callbacks
        by nulling ``_operation``. Does not cancel backend work. Forgetting
        handles for resource-creating operations (for example connect or stream
        setup) may leak resources.
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
            operation.cancel()
            raise

    def _resolved(self) -> T:
        operation = self._operation
        assert operation is not None
        raw = operation.result()
        if self._map_result is not None:
            return self._map_result(raw)
        return cast(T, raw)


class IOWaiterChainable(IOWaiter[T]):
    """``IOWaiter`` that primes a successor via ``create_next`` on completion.

    ``create_next`` runs from the parent operation's done callback so the chain
    can advance before anyone calls ``wait()``. ``wait()`` on the head blocks
    through the tail once results are needed. ``value()`` is one-shot: it hands
    this step's resolved result to ``create_next`` and clears the cached copy.

    An optional ``on_cleanup`` hook receives any still-unreleased resolved value
    when ``wait()`` fails (hook exceptions propagate) or from ``__del__`` as a
    best-effort fallback (hook exceptions there are logged by Python as
    ``Exception ignored in:``). ``create_next`` should call ``value()`` to take
    ownership of resource-bearing results; failures after that must clean up
    locally.
    """

    __slots__ = ("_next", "_create_next", "_chain_error", "_on_cleanup", "_resolved_value")

    def __init__(
        self,
        io: ProactorIOManager,
        operation: Operation[_RawResult],
        *,
        map_result: Callable[[_RawResult], T] | None = None,
        create_next: _CreateNext,
        on_cleanup: _OnCleanup | None = None,
    ) -> None:
        super().__init__(io, operation, map_result=map_result)
        self._next: IOWaiterProtocol[Any] | None = None
        self._create_next = create_next
        self._chain_error: BaseException | None = None
        self._on_cleanup = on_cleanup
        self._resolved_value: tuple[T] | None = None
        operation.add_done_callback(lambda _op: self._prime_next())

    def value(self) -> T:
        """Return this step's result once; clears the cached copy."""

        cached = self._resolved_value
        assert cached is not None
        self._resolved_value = None
        return cached[0]

    def _cleanup_unresolved_value(self) -> None:
        cached = self._resolved_value
        if cached is None:
            return
        self._resolved_value = None
        if self._on_cleanup is not None:
            self._on_cleanup(cached[0])

    def __del__(self) -> None:
        self._cleanup_unresolved_value()

    def _prime_next(self) -> None:
        try:
            self._resolved_value = (self._resolved(),)
            self._next = self._create_next(self)
        except BaseException as exc:
            self._chain_error = exc
        finally:
            self._operation = None

    def forget(self) -> None:
        """Unsupported for chain heads; may leak resources if the tail was primed."""

        super().forget()
        next_link = self._next
        if next_link is not None:
            next_link.forget()

    def wait(self) -> T:
        self._wait_self()
        try:
            if self._chain_error is not None:
                raise self._chain_error
            next_link = self._next
            if next_link is not None:
                return next_link.wait()
            return self.value()
        except BaseException:
            self._cleanup_unresolved_value()
            raise


class IOWaiterFake(Generic[T]):
    """Pre-resolved chain tail that returns a wrapped value from ``wait()``."""

    __slots__ = ("_value",)

    def __init__(self, value: T) -> None:
        self._value = value

    def forget(self) -> None:
        return None

    def wait(self) -> T:
        return self._value


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
        operation.add_done_callback(self._on_done)

    def value(self) -> T:
        """Return this leg's result once; clears the cached copy."""

        cached = self._resolved_value
        assert cached is not None
        self._resolved_value = None
        return cached[0]

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
        if self._group._completion is not None:
            return
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
    ``wait()`` on the group. Call exactly one of ``wait()`` or ``forget()``;
    this type does not track that contract. Child legs expose ``value()`` for
    one-shot handoff of raw operation results into advance handlers.
    """

    __slots__ = ("_completion", "_io", "_members", "_ready")

    def __init__(
        self,
        io: "ProactorIOManager",
    ) -> None:
        self._io = io
        self._completion: tuple[bool, Any] | None = None
        self._ready: ThreadsafeEvent | None = None
        self._members: set[_ActiveMember] = set()

    def attach(
        self,
        operation: Operation[Any],
        *,
        on_cleanup: _OnLegCleanup | None = None,
        advance: _AdvanceHandler | None = None,
    ) -> IOWaitGroupChild[Any]:
        """Register an operation leg that may expose a ``value()`` to advance hooks."""

        if self._completion is not None:
            operation.cancel()
            raise RuntimeError("IOWaitGroup is closed")
        child = IOWaitGroupChild(
            self,
            operation,
            on_cleanup=on_cleanup,
            advance=advance,
        )
        self._members.add(child)
        return child

    def attach_operation(
        self,
        operation: Operation[Any],
        *,
        on_complete: _OnCompleteHandler | None = None,
    ) -> None:
        """Register a bare operation leg without a ``value()`` handoff."""

        if self._completion is not None:
            operation.cancel()
            raise RuntimeError("IOWaitGroup is closed")

        def on_done(op: Operation[Any]) -> None:
            if self._completion is not None:
                return
            try:
                op.result()
            except BaseException as exc:
                self._complete_error(exc)
                return
            if on_complete is not None:
                try:
                    on_complete()
                except BaseException as exc:
                    self._complete_error(exc)

        operation.add_done_callback(on_done)
        self._members.add(operation)

    def finish(self, result: T) -> None:
        """Mark the grouped composition successful and wake a blocked ``wait()``."""

        self._complete(ok=True, value=result)

    def _complete_error(self, exc: BaseException) -> None:
        if self._completion is not None:
            return
        self._cleanup_members()
        self._complete(ok=False, value=exc)

    def _cleanup_members(self) -> None:
        for member in tuple(self._members):
            if isinstance(member, IOWaitGroupChild):
                member._cleanup_unresolved_value()

    def _complete(self, *, ok: bool, value: Any) -> None:
        if self._completion is not None:
            return
        self._completion = (ok, value)
        if not ok:
            self._cancel_pending()
        self._members.clear()
        ready = self._ready
        if ready is not None:
            ready.set()

    def _cancel_pending(self) -> None:
        for member in tuple(self._members):
            if isinstance(member, IOWaitGroupChild):
                operation = member._operation
                if operation is not None and not operation.done():
                    operation.cancel()
                continue
            if not member.done():
                member.cancel()

    def forget(self) -> None:
        """Drop interest in the grouped result; active backend work continues."""

        for member in self._members:
            if isinstance(member, IOWaitGroupChild):
                member._forget()
        self._members.clear()

    def wait(self) -> T:
        if self._completion is None:
            ready = ThreadsafeEvent(self._io._scheduler)  # type: ignore[arg-type]
            self._ready = ready
            if self._completion is None:
                try:
                    ready.swait()
                except BaseException:
                    self._cancel_pending()
                    self._cleanup_members()
                    raise
        completion = self._completion
        assert completion is not None
        ok, value = completion
        if not ok:
            raise value
        return cast(T, value)

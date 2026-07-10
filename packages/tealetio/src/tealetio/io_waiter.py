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


class IOWaiterProtocol(Protocol[T_co]):
    """Blocking IO handle returned by one-shot helpers and chain links."""

    def forget(self) -> None: ...

    def wait(self) -> T_co: ...


class IOWaiterChainableProtocol(IOWaiterProtocol[T_co], Protocol[T_co]):
    """Chain parent handle; exposes this step's result to ``create_next``."""

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
        """Drop interest in the result; backend work continues to completion."""

        self._operation = None

    def wait(self) -> T:
        self._wait_self()
        return self._resolved()

    def _wait_self(self) -> None:
        operation = self._operation
        assert operation is not None
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

    ``wait()`` on the head blocks through the tail. Like ``IOWaiter``, the owner
    calls either ``wait()`` or ``forget()``; completion callbacks may still run
    after ``forget()`` because backend work is not cancelled.
    """

    __slots__ = ("_next", "_create_next", "_chain_error")

    def __init__(
        self,
        io: ProactorIOManager,
        operation: Operation[_RawResult],
        *,
        map_result: Callable[[_RawResult], T] | None = None,
        create_next: _CreateNext,
    ) -> None:
        super().__init__(io, operation, map_result=map_result)
        self._next: IOWaiterProtocol[Any] | None = None
        self._create_next = create_next
        self._chain_error: BaseException | None = None
        operation.add_done_callback(lambda _op: self._prime_next())

    def value(self) -> T:
        """Return this step's result without waiting on chained successors."""

        return self._resolved()

    def _prime_next(self) -> None:
        try:
            self._next = self._create_next(self)
        except BaseException as exc:
            self._chain_error = exc

    def forget(self) -> None:
        super().forget()
        next_link = self._next
        if next_link is not None:
            next_link.forget()

    def wait(self) -> T:
        self._wait_self()
        assert self._operation is not None
        self._operation.result()
        if self._chain_error is not None:
            raise self._chain_error
        next_link = self._next
        if next_link is not None:
            return next_link.wait()
        return self._resolved()


class IOWaiterFake(Generic[T]):
    """Pre-resolved chain tail that returns a wrapped value from ``wait()``."""

    __slots__ = ("_value",)

    def __init__(self, value: T) -> None:
        self._value = value

    def forget(self) -> None:
        return None

    def wait(self) -> T:
        return self._value

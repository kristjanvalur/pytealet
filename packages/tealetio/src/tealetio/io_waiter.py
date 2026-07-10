from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar, cast, runtime_checkable

from .locks import ThreadsafeEvent
from .operations import Operation

if TYPE_CHECKING:
    from .io_manager import ProactorIOManager

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)
_RawResult = TypeVar("_RawResult")


@runtime_checkable
class IOOperation(Protocol[T_co]):
    """User-facing IO handle; call ``wait()`` to block for the result."""

    def done(self) -> bool: ...

    def cancelled(self) -> bool: ...

    def cancel(self) -> None: ...

    def forget(self) -> None: ...

    def wait(self) -> T_co: ...


class IOWaiter(Generic[T]):
    """Single-shot IO completion backed by a proactor ``Operation``.

    Call ``wait()`` to block for the result, ``forget()`` to drop interest while
    backend work continues, or ``cancel()`` to request backend cancellation.

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

    def cancel(self) -> None:
        """Cancel backend work if the operation is still active."""

        operation = self._operation
        if operation is not None:
            operation.cancel()

    def forget(self) -> None:
        """Drop interest in the result; backend work continues to completion."""

        self._operation = None

    def wait(self) -> T:
        operation = self._operation
        if operation is None:
            raise RuntimeError("IO operation was forgotten")
        if not operation.done():
            ready = ThreadsafeEvent(self._io._scheduler)  # type: ignore[arg-type]
            operation.add_done_callback(lambda _op: ready.set())
            ready.swait()
        raw = operation.result()
        if self._map_result is not None:
            return self._map_result(raw)
        return cast(T, raw)


class IOWaiterComposite(Generic[T]):
    """IO handle backed by caller-supplied ``wait``, ``cancel``, and ``forget`` hooks.

    For composite work over multiple underlying operations (for example a chunked
    send) where each disposition needs its own continuation logic.
    """

    __slots__ = ("_wait", "_cancel", "_forget")

    def __init__(
        self,
        *,
        wait: Callable[[], T],
        cancel: Callable[[], None],
        forget: Callable[[], None],
    ) -> None:
        self._wait = wait
        self._cancel = cancel
        self._forget = forget

    def wait(self) -> T:
        return self._wait()

    def cancel(self) -> None:
        self._cancel()

    def forget(self) -> None:
        self._forget()
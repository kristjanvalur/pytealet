from __future__ import annotations

import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Generic, TypeVar, cast

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)


class InvalidStateError(Exception):
    """Raised when an operation result is requested before completion."""


_DoneCallback = Callable[["Operation[Any]"], object]
_ResultCallback = Callable[[T_co], object]
_ProactorRef = Any


@dataclass
class ContinuousStepResult:
    progressed: bool = False
    done: bool = False


class Operation(Generic[T]):
    """Future-shaped IO operation owned by a proactor backend."""

    def __init__(
        self,
        *,
        kind: str,
        fileobj: object | None = None,
    ) -> None:
        self.kind = kind
        self.fileobj = fileobj
        self._lock = threading.Lock()
        self._done = False
        self._cancelled = False
        self._result: T | None = None
        self._exception: BaseException | None = None
        self._callbacks: list[_DoneCallback] = []

    def done(self) -> bool:
        """Return True if the operation has completed."""

        return self._done

    def cancelled(self) -> bool:
        """Return True if the operation completed by cancellation."""

        return self._cancelled

    def result(self) -> T:
        """Return the operation result, or raise its completion exception."""

        if not self._done:
            raise InvalidStateError("operation result is not ready")
        exception = self._exception
        result = self._result
        if exception is not None:
            raise exception
        return cast(T, result)

    def exception(self) -> BaseException | None:
        """Return the operation exception, or None for successful completion."""

        if not self._done:
            raise InvalidStateError("operation exception is not ready")
        return self._exception

    def add_done_callback(self, callback: _DoneCallback) -> None:
        """Register `callback` to run when the operation completes."""

        with self._lock:
            if self._done:
                run_now = True
            else:
                self._callbacks.append(callback)
                run_now = False
        if run_now:
            callback(self)

    def remove_done_callback(self, callback: _DoneCallback) -> int:
        """Remove matching done callbacks and return the number removed."""

        with self._lock:
            removed = 0
            kept: list[_DoneCallback] = []
            for stored_callback in self._callbacks:
                if stored_callback is callback:
                    removed += 1
                else:
                    kept.append(stored_callback)
            self._callbacks = kept
            return removed

    def deliver(
        self,
        proactor: _ProactorRef,
        *,
        result: Any = None,
        exception: BaseException | None = None,
    ) -> None:
        """Accept one backend completion on a worker thread."""

        with self._lock:
            if self._done:
                return
        if exception is not None:
            self._finish(exception=exception)
        else:
            self._finish(result=cast(T, result))

    def _finish(
        self,
        *,
        result: T | None = None,
        exception: BaseException | None = None,
        cancelled: bool = False,
    ) -> bool:
        if cancelled:
            if self._done:
                return False

        with self._lock:
            if self._done:
                if cancelled:
                    return False
                raise InvalidStateError("operation already done")
            self._result = result
            self._exception = exception
            self._cancelled = cancelled
            self._done = True
            callbacks = self._callbacks
            self._callbacks = []

        for callback in callbacks:
            callback(self)
        return True


class ContinuousOperation(Operation[None], Generic[T_co]):
    """Long-lived IO operation that emits multiple results before finishing.

    Result callbacks may run on any backend worker thread. Callers that need
    thread affinity must marshal from the callback into the desired thread or
    event loop themselves.

    Callbacks that submit nested ``Operation`` objects must not block waiting on
    them. Delivery-spawned work is independent of the parent continuous op.
    """

    def __init__(
        self,
        *,
        kind: str,
        fileobj: object | None = None,
        result_callback: _ResultCallback[T_co] | None = None,
    ) -> None:
        super().__init__(kind=kind, fileobj=fileobj)
        self._result_callback = result_callback

    def _emit_result(self, result: T_co) -> bool:
        """Deliver one result when the operation is still active.

        Returns ``True`` when delivery was accepted and the operation is still
        active afterwards, ``False`` when the operation was already done or became
        done during the callback (including cancellation).
        """

        with self._lock:
            if self._done:
                return False
            callback = self._result_callback
        if callback is not None:
            callback(result)
        with self._lock:
            return not self._done

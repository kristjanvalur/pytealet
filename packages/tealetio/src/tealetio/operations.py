from __future__ import annotations

import threading
from collections.abc import Callable
from concurrent.futures import CancelledError
from dataclasses import dataclass
from typing import Any, Generic, TypeVar, cast

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)


class InvalidStateError(Exception):
    """Raised when an operation result is requested before completion."""


_DoneCallback = Callable[["Operation[Any]"], object]
_ResultCallback = Callable[[T_co], object]
_CancelHook = Callable[[], None]


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
        self._callbacks: list[_DoneCallback] | None = []
        self._cancel: _CancelHook | None = None

    def done(self) -> bool:
        """Return True if the operation has completed."""

        return self._done

    def cancelled(self) -> bool:
        """Return True if the operation completed by cancellation."""

        return self._cancelled

    def set_cancel(self, cancel: _CancelHook | None) -> None:
        """Install or clear the backend cancel hook for this operation."""

        self._cancel = cancel

    def cancel(self) -> None:
        """Cancel the operation if it has not completed yet."""

        if self._done:
            return
        cancel = self._cancel
        if cancel is not None:
            cancel()
            return
        self._set_cancelled()

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
                assert self._callbacks is not None
                self._callbacks.append(callback)
                run_now = False
        if run_now:
            callback(self)

    def remove_done_callback(self, callback: _DoneCallback) -> int:
        """Remove matching done callbacks and return the number removed."""

        with self._lock:
            if self._callbacks is None:
                return 0
            removed = 0
            kept: list[_DoneCallback] = []
            for stored_callback in self._callbacks:
                if stored_callback is callback:
                    removed += 1
                else:
                    kept.append(stored_callback)
            self._callbacks = kept
            return removed

    def _set_result(self, result: T) -> None:
        self._finish(result=result)

    def _set_exception(self, exc: BaseException) -> None:
        self._finish(exception=exc)

    def _set_cancelled(self) -> bool:
        return self._finish(exception=CancelledError(), cancelled=True)

    def _finish(
        self,
        *,
        result: T | None = None,
        exception: BaseException | None = None,
        cancelled: bool = False,
    ) -> bool:
        with self._lock:
            if self._done:
                if cancelled:
                    return False
                raise InvalidStateError("operation already done")
            self._result = result
            self._exception = exception
            self._cancelled = cancelled
            self._done = True
            self._cancel = None
            callbacks = self._callbacks
            self._callbacks = None
        assert callbacks is not None
        for callback in callbacks:
            callback(self)
        return True


class ContinuousOperation(Operation[None], Generic[T_co]):
    """Long-lived IO operation that emits multiple results before finishing.

    Result callbacks may run on any backend worker thread. Callers that need
    thread affinity must marshal from the callback into the desired thread or
    event loop themselves.
    """

    def __init__(
        self,
        *,
        kind: str,
        fileobj: object | None = None,
        result_callback: _ResultCallback[T_co] | None = None,
    ) -> None:
        super().__init__(kind=kind, fileobj=fileobj)
        self._result_callbacks: list[_ResultCallback[T_co]] = []
        if result_callback is not None:
            self._result_callbacks.append(result_callback)

    def add_result_callback(self, callback: _ResultCallback[T_co]) -> None:
        """Register `callback` for each result produced by the operation."""

        with self._lock:
            if self._done:
                raise InvalidStateError("continuous operation is already done")
            self._result_callbacks.append(callback)

    def _try_emit_result(self, result: T_co) -> bool:
        """Deliver one result when the operation is still active.

        Returns ``True`` when callbacks ran, ``False`` when the operation was
        already done (including cancelled).
        """

        with self._lock:
            if self._done:
                return False
            callbacks = list(self._result_callbacks)
        for callback in callbacks:
            callback(result)
        return True

    def _emit_result(self, result: T_co) -> None:
        self._try_emit_result(result)

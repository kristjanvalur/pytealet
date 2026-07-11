from __future__ import annotations

import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from .tasks import CancelledError
from dataclasses import dataclass
from typing import Any, Generic, TypeVar, cast

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)


class InvalidStateError(Exception):
    """Raised when an operation result is requested before completion."""


_DoneCallback = Callable[["Operation[Any]"], object]
_ResultCallback = Callable[[T_co], object]
_CancelHook = Callable[[], None]
_ProactorRef = Any
OperationFactory = Callable[[str, object | None], "Operation[Any]"]


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
        self._cancel_hook: _CancelHook | None = None
        self._active_suboperations: set[Operation[Any]] = set()

    def done(self) -> bool:
        """Return True if the operation has completed."""

        return self._done

    def cancelled(self) -> bool:
        """Return True if the operation completed by cancellation."""

        return self._cancelled

    def set_cancel(self, cancel: _CancelHook | None) -> None:
        """Install or clear the backend cancel hook for this operation."""

        self._cancel_hook = cancel

    def attach_suboperation(self, suboperation: Operation[Any]) -> bool:
        """Register a child for ``cancel()`` propagation.

        Returns ``False`` when the parent is already done.
        """

        with self._lock:
            if self._done:
                return False
            self._active_suboperations.add(suboperation)
            return True

    def detach_suboperation(self, suboperation: Operation[Any]) -> None:
        with self._lock:
            self._active_suboperations.discard(suboperation)

    @contextmanager
    def track_suboperation(self, suboperation: Operation[Any]) -> Iterator[Operation[Any]]:
        """Register ``suboperation`` until the context exits or ``cancel()`` runs."""

        if not self.attach_suboperation(suboperation):
            suboperation.cancel()
            yield suboperation
            return
        try:
            yield suboperation
        finally:
            self.detach_suboperation(suboperation)

    def cancel(self) -> None:
        """Cancel the operation if it has not completed yet.

        Cancellation always races in-flight backend completions on worker
        threads. ``cancel_hook`` is best-effort IO teardown only; a late
        ``deliver()`` may still finish the operation successfully, in which
        case cancel is abandoned after the hook runs.
        """

        self._finish(exception=CancelledError(), cancelled=True)

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

    def complete(self, result: T) -> None:
        """Finish the operation from a chained suboperation callback."""

        with self._lock:
            if self._done:
                return
        try:
            self._finish(result=result)
        except InvalidStateError:
            return

    def complete_error(self, exc: BaseException) -> None:
        """Fail the operation from a chained suboperation callback."""

        with self._lock:
            if self._done:
                return
        try:
            self._finish(exception=exc)
        except InvalidStateError:
            return

    def _finish(
        self,
        *,
        result: T | None = None,
        exception: BaseException | None = None,
        cancelled: bool = False,
    ) -> bool:
        if cancelled:
            # Best-effort backend teardown, then terminalise unless a worker
            # thread completed the operation during the hook.
            if self._done:
                return False
            with self._lock:
                cancel_hook = self._cancel_hook
                self._cancel_hook = None
            if cancel_hook is not None:
                cancel_hook()
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
            self._cancel_hook = None
            callbacks = self._callbacks
            self._callbacks = []
            suboperations = tuple(self._active_suboperations) if cancelled else ()

        for suboperation in suboperations:
            suboperation.cancel()

        for callback in callbacks:
            callback(self)
        return True


class ContinuousOperation(Operation[None], Generic[T_co]):
    """Long-lived IO operation that emits multiple results before finishing.

    Result callbacks may run on any backend worker thread. Callers that need
    thread affinity must marshal from the callback into the desired thread or
    event loop themselves.

    Callbacks that submit nested ``Operation`` objects must not block waiting on
    them. Register each child with ``attach_suboperation()`` (or
    ``chain_suboperation()`` in ``operation_callbacks`` / ``continuous_callbacks``)
    so ``cancel()`` can reach in-flight child work.
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

    def set_result_callback(self, callback: _ResultCallback[T_co] | None) -> None:
        """Install or replace the result callback before the operation finishes."""

        with self._lock:
            if self._done:
                raise InvalidStateError("continuous operation is already done")
            self._result_callback = callback

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

    def _finish(
        self,
        *,
        result: Any = None,
        exception: BaseException | None = None,
        cancelled: bool = False,
    ) -> bool:
        # non-cancel finish leaves _active_suboperations alone: children started
        # from result callbacks keep running after finish or error finish.
        return super()._finish(result=result, exception=exception, cancelled=cancelled)

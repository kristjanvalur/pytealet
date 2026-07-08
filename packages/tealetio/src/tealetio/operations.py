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
_ProactorRef = Any
# ``result`` and ``exception`` are mutually exclusive; one is always ``None``.
DeliveryHandler = Callable[[_ProactorRef, "Operation[Any]", Any, BaseException | None], None]
ChainAdvanceHandler = Callable[[_ProactorRef, "Operation[Any]", Any, BaseException | None], None]
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
        self._delivery: DeliveryHandler | None = None
        self._lock = threading.Lock()
        self._done = False
        self._cancelled = False
        self._result: T | None = None
        self._exception: BaseException | None = None
        self._callbacks: list[_DoneCallback] | None = []
        self._cancel_hook: _CancelHook | None = None
        self._cancel_forward: Operation[Any] | None = None
        self._chain_parent: Operation[Any] | None = None
        self._chain_advance: ChainAdvanceHandler | None = None

    def done(self) -> bool:
        """Return True if the operation has completed."""

        return self._done

    def cancelled(self) -> bool:
        """Return True if the operation completed by cancellation."""

        return self._cancelled

    def set_cancel(self, cancel: _CancelHook | None) -> None:
        """Install or clear the backend cancel hook for this operation."""

        self._cancel_hook = cancel

    def set_cancel_forward(self, operation: "Operation[Any] | None") -> None:
        """Forward ``cancel()`` to a chained child operation."""

        self._cancel_forward = operation

    def set_chain_parent(self, parent: "Operation[Any] | None") -> None:
        """Record the parent operation that receives bubbled chain completions."""

        self._chain_parent = parent

    def set_chain_advance(self, handler: ChainAdvanceHandler | None) -> None:
        """Install the handler that receives bubbled child-chain completions."""

        self._chain_advance = handler

    def set_delivery(self, handler: DeliveryHandler | None) -> None:
        """Install the proactor completion handler for this operation."""

        self._delivery = handler

    def cancel(self) -> None:
        """Cancel the operation if it has not completed yet."""

        if self._done:
            return
        forward = self._cancel_forward
        if forward is not None:
            forward.cancel()
        cancel_hook = self._cancel_hook
        if cancel_hook is not None:
            cancel_hook()
        if not self._done:
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

    def deliver(
        self,
        proactor: _ProactorRef,
        *,
        result: Any = None,
        exception: BaseException | None = None,
    ) -> None:
        """Accept one backend completion on a worker thread.

        When a delivery handler was installed, it runs instead of finishing
        the operation. Otherwise this completes immediately.
        """

        if self._done:
            return
        delivery = self._delivery
        if delivery is not None:
            delivery(proactor, self, result, exception)
            return
        if exception is not None:
            self._set_exception(exception)
        else:
            self._set_result(cast(T, result))

    def advance(
        self,
        proactor: _ProactorRef,
        *,
        result: Any = None,
        exception: BaseException | None = None,
    ) -> None:
        """Accept a bubbled completion from a chained child operation.

        Unlike ``deliver()``, this does not re-enter the proactor delivery
        handler. When ``_chain_advance`` is installed the handler runs local
        link logic and should finish by calling ``advance_up()`` on this
        operation. With no handler installed, ``advance_up()`` runs
        immediately.
        """

        if self._done:
            return
        handler = self._chain_advance
        if handler is not None:
            handler(proactor, self, result, exception)
            return
        self.advance_up(proactor, result=result, exception=exception)

    def advance_up(
        self,
        proactor: _ProactorRef,
        *,
        result: Any = None,
        exception: BaseException | None = None,
    ) -> None:
        """Pass a chain completion to the parent, or finish at the chain root."""

        if self._done:
            return
        parent = self._chain_parent
        if parent is not None:
            parent.advance(proactor, result=result, exception=exception)
            return
        if exception is not None:
            self._set_exception(exception)
        else:
            self._set_result(cast(T, result))

    def complete(self, result: T) -> None:
        """Finish the operation from a delivery handler."""

        self._set_result(result)

    def complete_error(self, exc: BaseException) -> None:
        """Fail the operation from a delivery handler."""

        self._set_exception(exc)

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
            self._cancel_hook = None
            self._cancel_forward = None
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

    def _emit_result(self, result: T_co) -> bool:
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

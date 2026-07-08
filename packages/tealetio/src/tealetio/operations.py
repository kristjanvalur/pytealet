from __future__ import annotations

import threading
import weakref
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
AdvanceHook = Callable[["Operation[Any]", Any, BaseException | None], None]
OperationFactory = Callable[[str, object | None], "Operation[Any]"]
_CancelForwardRef = weakref.ReferenceType["Operation[Any]"]


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
        self._cancel_forward: _CancelForwardRef | None = None
        self._chain_parent: Operation[Any] | None = None
        self._advance_hook: AdvanceHook | None = None

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

        self._cancel_forward = None if operation is None else weakref.ref(operation)

    def attach_child(self, child: "Operation[Any]") -> bool:
        """Publish ``child`` on the cancel/advance spine.

        Returns ``False`` when this operation is already done, for example
        because ``cancel()`` won before the downlink was installed.
        """

        with self._lock:
            if self._done:
                return False
            child.set_chain_parent(self)
            self._cancel_forward = weakref.ref(child)
            return True

    def may_extend_chain(self) -> bool:
        """Return whether a new chained leg may be started on this link."""

        with self._lock:
            return not self._done

    def set_chain_parent(self, parent: "Operation[Any] | None") -> None:
        """Record the parent operation that receives bubbled chain completions."""

        self._chain_parent = parent

    def set_advance_hook(self, handler: AdvanceHook | None) -> None:
        """Install the handler invoked when a chain completion reaches this link."""

        self._advance_hook = handler

    def set_delivery(self, handler: DeliveryHandler | None) -> None:
        """Install the proactor completion handler for this operation."""

        self._delivery = handler

    def cancel(self) -> None:
        """Cancel the operation if it has not completed yet."""

        with self._lock:
            if self._done:
                return
            forward = self._cancel_forward_target()
            is_leaf = forward is None
        if forward is not None:
            forward.cancel()
        if is_leaf:
            with self._lock:
                bubble_cancelled = self._chain_parent is not None and not self._done
            if bubble_cancelled:
                # Bubble before this leg is terminalised so chain_parent links
                # remain intact and advance hooks (for example fd-close) run.
                self.advance(exception=CancelledError())
        cancel_hook = self._cancel_hook
        if cancel_hook is not None:
            cancel_hook()
        with self._lock:
            if self._done:
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
            self._finish(exception=exception)
        else:
            self._finish(result=cast(T, result))

    def advance(
        self,
        *,
        result: Any = None,
        exception: BaseException | None = None,
    ) -> None:
        """Accept a bubbled completion from a chained child operation.

        Unlike ``deliver()``, this does not re-enter the proactor delivery
        handler. Walks the parent chain from this link, invoking the first
        ``_advance_hook`` found (each hook runs at most once per leg and is
        cleared before it runs). When no hook is installed on a link the walk
        continues to its parent. At the chain root with no hook the operation
        completes. Hooks should finish propagation by calling
        ``advance_continue()`` on the link that owns the hook.
        """

        op: Operation[Any] | None = self
        while op is not None:
            if op._done:
                return
            handler = op._advance_hook
            if handler is not None:
                op._advance_hook = None
                handler(op, result, exception)
                return
            parent = op._chain_parent
            if parent is None:
                if exception is not None:
                    op._finish(
                        exception=exception,
                        cancelled=isinstance(exception, CancelledError),
                    )
                else:
                    op._finish(result=result)
                return
            op = parent

    def advance_continue(
        self,
        *,
        result: Any = None,
        exception: BaseException | None = None,
    ) -> None:
        """Resume propagation after local advance-hook work on this link.

        Delegates to ``advance()`` without re-entering this link's hook. The
        hook is already cleared when ``advance()`` dispatched it; this call is
        a no-op clear plus bubble to the parent chain. Intended for use from
        advance-hook handlers only.
        """

        self._advance_hook = None
        self.advance(result=result, exception=exception)

    def complete(self, result: T) -> None:
        """Finish the operation from a delivery handler."""

        self._finish(result=result)

    def complete_error(self, exc: BaseException) -> None:
        """Fail the operation from a delivery handler."""

        self._finish(exception=exc)

    def _set_cancelled(self) -> bool:
        return self._finish(exception=CancelledError(), cancelled=True)

    def _cancel_forward_target(self) -> Operation[Any] | None:
        ref = self._cancel_forward
        if ref is None:
            return None
        return ref()

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
            self._advance_hook = None
            parent = self._chain_parent
            self._chain_parent = None
            if parent is not None:
                forward_ref = parent._cancel_forward
                if forward_ref is not None and forward_ref() is self:
                    parent._cancel_forward = None
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

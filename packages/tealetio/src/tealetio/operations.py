from __future__ import annotations

import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Generic, NamedTuple, TypeVar, cast

from .tasks import CancelledError

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)


class InvalidStateError(Exception):
    """Raised when an operation result is requested before completion."""


_DoneCallback = Callable[["Operation[Any]"], object]
_ProactorRef = Any


class MultishotDelivery(NamedTuple):
    """One multishot leg delivery to a continuous operation callback.

    ``(index, value, exception, more, operation)``. For ``recv_many``, ``accept_many``,
    and ``poll_many``, ``index`` is the stream ordinal from the backend
    (``completion.sequence`` on uring multishot, or a per-operation counter on
    selector and one-shot fallbacks). ``index=None`` opts out of reordering
    (for example emulated-path cancel terminals). ``value`` carries successful chunk data
    when present. ``exception`` carries transport failures the consumer may
    interpret (for example ``errno.ENOBUFS`` or a negative io_uring CQE).
    Terminal failures are emitted through the result callback; consumers such as
    ``ProactorIOManager`` and ``RecvIterBuffer`` call ``finish_operation()`` on
    terminal deliveries. ``more``
    mirrors ``IORING_CQE_F_MORE`` on uring backends. For ``recv_many``,
    ``more=False`` with empty data signals EOF; ``more=False`` with non-empty
    data means the leg stopped before EOF and consumers should start a fresh
    ``recv_many()``.
    """

    index: int | None = 0
    value: Any = None
    exception: BaseException | None = None
    more: bool = True
    operation: "ContinuousOperation[Any] | None" = None


@dataclass
class ContinuousStepResult:
    progressed: bool = False
    done: bool = False


class Operation(Generic[T]):
    """Future-shaped IO operation owned by a proactor backend.

    Cancellation is not on ``Operation`` itself. Call
    ``scheduler.proactor.cancel(operation)`` (or ``scheduler.io`` /
    ``SelectorScheduler.cancel_operation()`` wrappers). The proactor returns a
    teardown ``Operation[None]``; ``wait()`` on it when ring cancel must settle
    before shutdown, or ``forget()`` when only the target's terminal state matters.
    """

    def __init__(
        self,
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
            self._finish(exception=exception, cancelled=isinstance(exception, CancelledError))
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

    Owner-thread multishot delivery handlers (for example ``poll_many`` and
    ``accept_many`` in ``ProactorIOManager``) must call ``finish_operation`` on
    terminal deliveries (``not delivery.more``) so ``add_done_callback``
    waiters observe completion on the scheduler thread.

    Callbacks that submit nested ``Operation`` objects must not block waiting on
    them. Delivery-spawned work is independent of the parent continuous op.
    """

    def __init__(
        self,
        kind: str,
        fileobj: object | None = None,
        result_callback: Callable[[MultishotDelivery], object] | None = None,
    ) -> None:
        super().__init__(kind, fileobj)
        self._result_callback = result_callback

    def finish_operation(self, delivery: MultishotDelivery) -> None:
        """Finish the operation from one terminal owner-thread delivery.

        Multishot delivery callbacks that marshal onto the scheduler must call
        this when ``not delivery.more``. When the proactor already finished
        the operation (for example via ``_finish_with_terminal_delivery`` on a
        worker thread), this only asserts terminal state and completion.
        """

        assert not delivery.more
        if not self._done:
            exc = delivery.exception
            if exc is not None:
                self._finish(exception=exc, cancelled=isinstance(exc, CancelledError))
            else:
                self._finish(result=None)
        assert self._done

    def _emit_delivery(self, delivery: MultishotDelivery) -> bool:
        """Deliver one multishot chunk when the operation is still active.

        Returns ``True`` when the callback ran (or there is no callback).
        Returns ``False`` when the operation was already done and the delivery
        was skipped.
        """

        with self._lock:
            if self._done:
                return False
            callback = self._result_callback
        if callback is not None:
            callback(delivery._replace(operation=self))
        return True

    def _emit_result(
        self,
        result: T_co,
        *,
        index: int | None = 0,
        exception: BaseException | None = None,
        more: bool = True,
    ) -> bool:
        """Deliver one successful chunk wrapped in ``MultishotDelivery``."""

        return self._emit_delivery(MultishotDelivery(index, result, exception, more))

    def _finish_with_terminal_delivery(
        self,
        delivery: MultishotDelivery,
        *,
        cancelled: bool = False,
    ) -> None:
        """Emit one terminal ``MultishotDelivery`` for the result callback.

        The consumer must call ``finish_operation`` on the owner thread when it
        marshals deliveries (``ProactorIOManager``, ``RecvIterBuffer``, and
        similar). ``cancelled`` is accepted for call-site compatibility; cancel
        state is applied in ``finish_operation`` from ``delivery.exception``.
        """

        del cancelled
        assert not delivery.more
        with self._lock:
            if self._done:
                return
            callback = self._result_callback
        if callback is not None:
            callback(delivery._replace(operation=self))

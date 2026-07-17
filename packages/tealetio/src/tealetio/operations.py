from __future__ import annotations

import errno
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, ClassVar, Generic, NamedTuple, TypeVar, cast

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)


class InvalidStateError(Exception):
    """Raised when an operation result is requested before completion."""


def io_cancellation_error() -> OSError:
    """Return the standard exception for proactor-requested IO cancellation."""

    return OSError(
        errno.ECANCELED,
        errno.errorcode.get(errno.ECANCELED, "io_uring operation failed"),
    )


def is_io_cancellation(exc: BaseException | None) -> bool:
    """Return True when ``exc`` represents IO cancellation (not task cancellation)."""

    return isinstance(exc, OSError) and exc.errno == errno.ECANCELED


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

    # Shared ClassVar lock: done-callback registration is rare vs completion.
    # Pending-op accounting is not process-global; see ``pending_bucket``.
    _lock: ClassVar[threading.Lock] = threading.Lock()

    def __init__(
        self,
        kind: str,
        fileobj: object | None = None,
        *,
        pending_bucket: list[None] | None = None,
    ) -> None:
        # Optional shared list owned by the submitting proactor (append/pop under
        # the GIL is the unfinished-op counter). Selector/sync ops leave this None.
        if pending_bucket is not None:
            pending_bucket.append(None)
        self._pending_bucket = pending_bucket
        self.kind = kind
        self.fileobj = fileobj
        self._resolved: tuple[T | None, BaseException | None] | None = None
        self._callbacks: list[_DoneCallback] = []

    def done(self) -> bool:
        """Return True if the operation has completed."""

        return self._resolved is not None

    def cancelled(self) -> bool:
        """Return True if the operation completed by IO cancellation."""

        resolved = self._resolved
        if resolved is None:
            return False
        return is_io_cancellation(resolved[1])

    def result(self) -> T:
        """Return the operation result, or raise its completion exception."""

        resolved = self._resolved
        if resolved is None:
            raise InvalidStateError("operation result is not ready")
        result, exception = resolved
        if exception is not None:
            raise exception
        return cast(T, result)

    def exception(self) -> BaseException | None:
        """Return the operation exception, or None for successful completion."""

        resolved = self._resolved
        if resolved is None:
            raise InvalidStateError("operation exception is not ready")
        return resolved[1]

    def add_done_callback(self, callback: _DoneCallback) -> None:
        """Register `callback` to run when the operation completes."""

        with self._lock:
            if self.done():
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

        self._finish(result=cast(T, result), exception=exception)

    def _finish(
        self,
        *,
        result: T | None = None,
        exception: BaseException | None = None,
    ) -> None:
        with self._lock:
            assert self._resolved is None
            self._resolved = (result, exception)
            callbacks = self._callbacks
            self._callbacks = []

        pending_bucket = self._pending_bucket
        if pending_bucket is not None:
            pending_bucket.pop()

        for callback in callbacks:
            callback(self)


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
        *,
        pending_bucket: list[None] | None = None,
    ) -> None:
        super().__init__(kind, fileobj, pending_bucket=pending_bucket)
        self._result_callback = result_callback

    def finish_operation(self, delivery: MultishotDelivery) -> None:
        """Finish the operation from one terminal owner-thread delivery.

        Multishot delivery callbacks that marshal onto the scheduler must call
        this when ``not delivery.more``. When the proactor already finished
        the operation (for example via ``_finish_with_terminal_delivery`` on a
        worker thread), this only asserts terminal state and completion.
        """

        assert not delivery.more
        if not self.done():
            self._finish(result=None, exception=delivery.exception)
        assert self.done()

    def _emit_delivery(self, delivery: MultishotDelivery) -> None:
        """Deliver one multishot chunk to the result callback."""

        callback = self._result_callback
        if callback is not None:
            callback(delivery._replace(operation=self))

    def _emit_result(
        self,
        result: T_co,
        *,
        index: int | None = 0,
        exception: BaseException | None = None,
        more: bool = True,
    ) -> None:
        """Deliver one successful chunk wrapped in ``MultishotDelivery``."""

        self._emit_delivery(MultishotDelivery(index, result, exception, more))

    def _finish_with_terminal_delivery(
        self,
        delivery: MultishotDelivery,
    ) -> None:
        """Emit one terminal ``MultishotDelivery`` for the result callback.

        The consumer must call ``finish_operation`` on the owner thread when it
        marshals deliveries (``ProactorIOManager``, ``RecvIterBuffer``, and
        similar). Cancel state is applied in ``finish_operation`` from
        ``delivery.exception``.
        """

        assert not delivery.more
        callback = self._result_callback
        if callback is not None:
            callback(delivery._replace(operation=self))

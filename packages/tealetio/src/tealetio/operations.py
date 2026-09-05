from __future__ import annotations

import errno
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, ClassVar, Generic, NamedTuple, Protocol, TypeAlias, TypeVar, cast

from .stream_diag import worker_completion_mark_emit_end, worker_completion_mark_emit_start

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


# ---------------------------------------------------------------------------
# Waitable surface (duck-typed)
#
# ``IOWaitGroup`` parks on ``IOWaiter`` legs. Selector oneshots use
# ``Operation`` as an ``OpHandle`` (internal); uring oneshots are
# ``Completion``. Prefer these protocols in annotations over hard-coding
# a single concrete class hierarchy.
# ---------------------------------------------------------------------------

# Parameter is effectively SupportsOperation; typed as Any so concrete callbacks
# annotated with Operation[...] remain assignable (Callable is contravariant in args).
_DoneCallback = Callable[[Any], object]
_ProactorRef = Any


class SupportsOperation(Protocol[T_co]):
    """Duck-typed one-shot IO waitable returned by proactor backends.

    Cancellation stays on the proactor (``proactor.cancel(handle, callback)``
    or ``proactor.stop_poll(handle, callback)`` for ``poll_many``), not on
    the waitable itself.
    """

    kind: str
    fileobj: object | None

    def done(self) -> bool:
        """Return True if the operation has completed."""
        ...

    def cancelled(self) -> bool:
        """Return True if the operation completed by IO cancellation."""
        ...

    def result(self) -> T_co:
        """Return the operation result, or raise its completion exception."""
        ...

    def exception(self) -> BaseException | None:
        """Return the operation exception, or None for successful completion."""
        ...

    def add_done_callback(self, callback: _DoneCallback) -> None:
        """Register ``callback`` to run when the operation completes."""
        ...

    def remove_done_callback(self, callback: _DoneCallback) -> int:
        """Remove matching done callbacks and return the number removed."""
        ...


class MultishotDelivery(NamedTuple):
    """One multishot leg delivery to a continuous stream callback.

    ``(index, value, exception, more)``. For ``recv_many``, ``accept_many``,
    and ``poll_many``, ``index`` is the stream ordinal from the backend
    (``completion.sequence`` on uring, or ``_next_index`` on selector
    ``SelectorCancelHandle`` and uring one-shot poll fallbacks, including
    local cancel terminals).
    ``value`` carries successful chunk data
    when present. ``exception`` carries transport failures the consumer may
    interpret (for example ``errno.ENOBUFS`` or a negative io_uring CQE).
    Terminal failures are emitted through the result callback. ``more``
    mirrors ``IORING_CQE_F_MORE`` on uring backends. For ``recv_many``,
    ``more=False`` with empty data signals EOF; ``more=False`` with non-empty
    data means the leg stopped before EOF and consumers should start a fresh
    ``recv_many()``. ``accept_many`` terminals (``more=False``) are stream-end
    for that arm; oneshot backends finish after each accept. ``poll_many``
    terminals are stream-end (stop or error).
    """

    index: int = 0
    value: Any = None
    exception: BaseException | None = None
    more: bool = True


@dataclass
class ContinuousStepResult:
    progressed: bool = False
    done: bool = False


class Operation(Generic[T]):
    """Future-shaped IO operation used by selector-backed proactors.

    Satisfies ``SupportsOperation``. Uring backends may use a separate concrete
    type with the same duck-typed surface; see ``SupportsOperation``.

    Cancellation is not on the waitable itself. Call
    ``scheduler.proactor.cancel(handle, callback)`` (or ``stop_poll`` for
    ``poll_many``). Pass a callback to observe the cancel *request*; use
    ``cancel_nowait`` when only the target's terminal state matters.
    """

    __slots__ = ("__weakref__", "_callback", "_callbacks", "_resolved", "fileobj", "kind")
    # Shared ClassVar lock: done-callback registration is rare vs completion.
    _lock: ClassVar[threading.Lock] = threading.Lock()

    def __init__(
        self,
        kind: str,
        fileobj: object | None = None,
    ) -> None:
        self.kind = kind
        self.fileobj = fileobj
        self._callback = None
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

        if callbacks:
            worker_completion_mark_emit_start()
            try:
                for callback in callbacks:
                    callback(self)
            finally:
                worker_completion_mark_emit_end()


# Opaque handle to a submitted proactor operation (oneshot or stream).
# Concrete values:
# - uring: armed ``Completion``, or ``None`` when the callback already ran
# - selector oneshot: ``Operation`` (internal waitable, used only as the handle)
# - selector recv/accept/poll-many: ``SelectorCancelHandle``
# - emulated oneshot poll_many: reverse-link holder
# Cancel with ``proactor.cancel`` / ``cancel_nowait``; stop poll with
# ``proactor.stop_poll``. Do not call ``done()`` / ``result()`` on the handle.
OpHandle: TypeAlias = Any
RecvManyHandle: TypeAlias = OpHandle
AcceptManyHandle: TypeAlias = OpHandle
PollManyHandle: TypeAlias = OpHandle


class _DeliveryHandle:
    """Internal stream token: emit ``MultishotDelivery`` to a result callback.

    Selector streams and emulated oneshot poll_many inherit this. Not a
    waitable; not the public ``OpHandle`` alias.
    """

    __slots__ = ("_result_callback",)

    def __init__(
        self,
        result_callback: Callable[[MultishotDelivery], object] | None = None,
    ) -> None:
        self._result_callback = result_callback

    def _emit_delivery(self, delivery: MultishotDelivery) -> None:
        """Deliver one multishot chunk to the result callback."""

        callback = self._result_callback
        if callback is None:
            return
        worker_completion_mark_emit_start()
        try:
            callback(delivery)
        finally:
            worker_completion_mark_emit_end()

    def _emit_result(
        self,
        result: Any,
        *,
        index: int = 0,
        exception: BaseException | None = None,
        more: bool = True,
    ) -> None:
        """Deliver one successful chunk wrapped in ``MultishotDelivery``."""

        self._emit_delivery(MultishotDelivery(index, result, exception, more))

    def _finish_with_terminal_delivery(self, delivery: MultishotDelivery) -> None:
        """Emit one terminal ``MultishotDelivery`` for the result callback."""

        assert not delivery.more
        self._emit_delivery(delivery)


class SelectorCancelHandle(_DeliveryHandle):
    """Selector recv-many / accept-many / poll-many cancel token: next stream ordinal.

    Selector has no ``completion.sequence``. Local cancel and unexpected step
    errors emit ``ECANCELED`` at ``_next_index``. Uring handles do not carry
    this field; they use the reverse ``Completion`` sequence instead.
    """

    __slots__ = ("_next_index",)

    def __init__(
        self,
        result_callback: Callable[[MultishotDelivery], object] | None = None,
        *,
        base_sequence: int = 0,
    ) -> None:
        super().__init__(result_callback)
        self._next_index = base_sequence

"""Delivery tokens, stream helpers, and cancellation for proactor IO.

Submit returns an opaque ``OpHandle``. Continuous streams deliver
``MultishotDelivery`` chunks. Selector stream tokens and uring emulated
poll inherit ``_DeliveryHandle``.
"""

from __future__ import annotations

import errno
import heapq
import socket
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, NamedTuple, TypeAlias, TypeVar

from .socket_helpers import abortive_close
from .stream_diag import worker_completion_mark_emit_end, worker_completion_mark_emit_start

if TYPE_CHECKING:
    from .scheduler import BaseScheduler

T = TypeVar("T")

__all__ = [
    "MultishotDelivery",
    "OpHandle",
    "io_cancellation_error",
    "is_cancellation_delivery",
    "is_io_cancellation",
]


def io_cancellation_error() -> OSError:
    """Return the standard exception for proactor-requested IO cancellation."""

    return OSError(
        errno.ECANCELED,
        errno.errorcode.get(errno.ECANCELED, "io_uring operation failed"),
    )


def is_io_cancellation(exc: BaseException | None) -> bool:
    """Return True when ``exc`` represents IO cancellation (not task cancellation)."""

    return isinstance(exc, OSError) and exc.errno == errno.ECANCELED


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


# Opaque handle to a submitted proactor operation (oneshot or stream).
# Concrete values:
# - uring: armed ``Completion``, or ``None`` when the callback already ran
# - selector oneshot: private token in ``proactor.py``
# - selector recv/accept/poll-many: ``SelectorCancelHandle``
# - emulated oneshot poll_many: reverse-link holder
# Cancel with ``proactor.cancel`` / ``cancel_nowait``; stop poll with
# ``proactor.stop_poll``. Do not call ``done()`` / ``result()`` on the handle.
OpHandle: TypeAlias = Any


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


AcceptReadResult = tuple[socket.socket, bytes | None, BaseException | None]
AcceptDelivery = tuple[socket.socket, bytes | None]
AcceptStreamsDelivery: TypeAlias = tuple[Any, Any]
AcceptRecvErrorCallback = Callable[[socket.socket, BaseException], object]
_MAX_ACCEPT_RECV_SIZE = 2**16


def normalize_accept_recv_size(recv_size: int | None) -> int | None:
    if recv_size is None:
        return None
    if recv_size <= 0:
        raise ValueError("recv_size must be positive when provided")
    if recv_size > _MAX_ACCEPT_RECV_SIZE:
        return _MAX_ACCEPT_RECV_SIZE
    return recv_size


def finalize_accept_recv_error(
    conn: socket.socket,
    recv_error: BaseException,
    on_recv_error: AcceptRecvErrorCallback | None,
) -> None:
    """Invoke ``on_recv_error`` when provided, then close ``conn``."""

    hook_error: BaseException | None = None
    if on_recv_error is not None:
        try:
            on_recv_error(conn, recv_error)
        except BaseException as exc:
            hook_error = exc
    abortive_close(conn)
    if hook_error is not None:
        raise hook_error


DeliveryCallback = Callable[[MultishotDelivery], object]


class ReorderBuffer:
    """Deliver ``MultishotDelivery`` callbacks in strict index order.

    ``_delivered`` is the next leg index to hand off. Each ``deliver`` call runs
    the constructor callback immediately when ``delivery.index`` matches;
    otherwise the delivery is queued on a min-heap until earlier indices have
    been delivered. ``delivery.index`` must be a numeric stream ordinal.
    """

    def __init__(self, callback: DeliveryCallback, *, start: int = 0) -> None:
        self._callback = callback
        self._delivered = start
        self._heap: list[MultishotDelivery] = []

    def deliver(self, delivery: MultishotDelivery) -> None:
        if delivery.index == self._delivered:
            self._deliver_now(delivery)
            return
        heapq.heappush(self._heap, delivery)

    def _deliver_now(self, delivery: MultishotDelivery) -> None:
        self._callback(delivery)
        self._delivered += 1
        while self._heap and self._heap[0].index == self._delivered:
            pending = heapq.heappop(self._heap)
            self._callback(pending)
            self._delivered += 1

    @property
    def next_index(self) -> int:
        return self._delivered

    @property
    def pending(self) -> bool:
        return bool(self._heap)

    def drain(self) -> Iterator[MultishotDelivery]:
        """Remove and yield all pending deliveries in any order.

        Does not invoke the constructor callback. Callers that hold leased
        buffer values must release them from the yielded deliveries.
        """

        pending = self._heap
        self._heap = []
        return iter(pending)

    def reset(self, *, start: int = 0) -> None:
        self._heap.clear()
        self._delivered = start

    def arm_next_index(self, index: int) -> None:
        """Prepare for the next leg whose first delivery uses ``index``.

        ``deliver`` increments ``_delivered`` after each callback; arm one below
        the next leg's first index so the increment lands on ``index``.
        """

        self._delivered = index - 1


class CountFinalizer:
    """Deliver every sequenced leg immediately; finish when all indices through the terminal have been observed.

    Unlike ``ReorderBuffer``, this does not heap or preserve index order. It is
    for independent legs (accept, later poll) where the only invariant is that
    the arm must not finish until every leg of the shot has been handed to
    ``callback``. ``delivery.index`` must be a numeric stream ordinal.

    ``finish`` is optional. Manager ``accept_many`` passes a closer that
    settles the ``IOWaiter``.
    """

    def __init__(
        self,
        callback: DeliveryCallback,
        *,
        start: int = 0,
        finish: Callable[[MultishotDelivery], object] | None = None,
    ) -> None:
        self._callback = callback
        self._finish = finish
        self._start = start
        self._delivered_count = 0
        self._max_count: int | None = None
        self._final_delivery: MultishotDelivery | None = None

    def deliver(self, delivery: MultishotDelivery) -> None:
        assert delivery.index >= self._start
        if not delivery.more:
            self._max_count = delivery.index - self._start + 1
            self._final_delivery = delivery

        try:
            self._callback(delivery)
        finally:
            self._delivered_count += 1
            if self._max_count is not None and self._delivered_count == self._max_count:
                assert self._final_delivery is not None
                if self._finish is not None:
                    self._finish(self._final_delivery)
                self._final_delivery = None


def is_cancellation_delivery(delivery: MultishotDelivery) -> bool:
    """Return True when ``delivery`` ends a continuous op by IO cancellation.

    Proactor cancel surfaces ``OSError(errno.ECANCELED)``. Accept and receive
    callbacks should treat this as "no further chunks" rather than a transport
    failure to surface to callers.
    """

    return is_io_cancellation(delivery.exception)


def marshal_to_scheduler(
    scheduler: BaseScheduler,
    callback: Callable[[T], object],
) -> Callable[[T], None]:
    """Wrap ``callback`` so each result is delivered on the scheduler thread."""

    def deliver(result: T) -> None:
        scheduler.call_soon_threadsafe(callback, result, immediate=True)

    return deliver

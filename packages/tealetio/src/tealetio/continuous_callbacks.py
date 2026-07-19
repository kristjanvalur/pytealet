"""Composition helpers for continuous proactor operation callbacks."""

from __future__ import annotations

import heapq
import socket
from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING, Any, TypeAlias, TypeVar

from .operations import MultishotDelivery, is_io_cancellation
from .socket_helpers import abortive_close

T = TypeVar("T")

AcceptReadResult = tuple[socket.socket, bytes | None, BaseException | None]
AcceptDelivery = tuple[socket.socket, bytes | None]
AcceptStreamsDelivery: TypeAlias = tuple[Any, Any]
AcceptRecvErrorCallback = Callable[[socket.socket, BaseException], object]
_MAX_ACCEPT_RECV_SIZE = 2**16

if TYPE_CHECKING:
    from .scheduler import BaseScheduler


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


def finish_continuous_delivery(delivery: MultishotDelivery) -> None:
    """Finish a continuous operation from one terminal owner-thread delivery."""

    if not delivery.more:
        operation = delivery.operation
        assert operation is not None
        operation.finish_operation(delivery)


DeliveryCallback = Callable[[MultishotDelivery], object]


class ReorderBuffer:
    """Deliver ``MultishotDelivery`` callbacks in strict index order.

    ``_delivered`` is the next leg index to hand off. Each ``deliver`` call runs
    the constructor callback immediately when ``delivery.index`` matches;
    otherwise the delivery is queued on a min-heap until earlier indices have
    been delivered.

    ``index=None`` opts out of sequence order (local cancel terminals) and is
    delivered immediately without waiting for gaps. That does **not** flush the
    heap: ``recv_many`` must not surface out-of-order chunks across a cancel.
    Accept/poll paths that own sockets call ``flush_pending()`` before such a
    terminal so heaped connections are not stranded. After that flush, late
    legs for gap indices pass through immediately (cancels are rare; normal
    sequenced delivery is unchanged).
    """

    def __init__(self, callback: DeliveryCallback, *, start: int = 0) -> None:
        self._callback = callback
        self._delivered = start
        self._heap: list[MultishotDelivery] = []
        # set by flush_pending (accept/poll cancel only); zero cost when false
        self._late_passthrough = False

    def deliver(self, delivery: MultishotDelivery) -> None:
        if delivery.index is None or self._late_passthrough:
            self._callback(delivery)
            return
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

    def flush_pending(self) -> None:
        """Deliver every heaped leg in index order, even across missing gaps.

        For accept/poll cancel: hand off sockets/stream pairs before an
        unsequenced terminal finishes the continuous op. Enables late
        passthrough afterward so gap-skipped indices that arrive after the
        flush still reach the callback instead of re-heaping forever. Do not
        use for ``recv_many`` — that would reorder stream data past a cancel.

        Pops one entry at a time so a raising callback leaves remaining heap
        entries intact for a later retry.
        """

        while self._heap:
            item = heapq.heappop(self._heap)
            try:
                self._callback(item)
            except BaseException:
                heapq.heappush(self._heap, item)
                raise
            if item.index is not None and item.index >= self._delivered:
                self._delivered = item.index + 1
        self._late_passthrough = True

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


def is_cancellation_delivery(delivery: MultishotDelivery) -> bool:
    """Return True when ``delivery`` ends a continuous op by IO cancellation.

    Proactor cancel surfaces ``OSError(errno.ECANCELED)``. Accept and receive
    callbacks should treat this as "no further chunks" rather than a transport
    failure to surface to callers.
    """

    return is_io_cancellation(delivery.exception)


def wrap_accept_delivery(
    deliver: Callable[[AcceptReadResult], object],
) -> Callable[[MultishotDelivery], None]:
    """Adapt proactor ``accept_many`` deliveries to io_manager accept tuples."""

    def on_conn(delivery: MultishotDelivery) -> None:
        if is_cancellation_delivery(delivery):
            return
        if delivery.exception is not None:
            raise delivery.exception
        if delivery.value is None:
            return
        deliver((delivery.value, None, None))

    return on_conn


def marshal_to_scheduler(
    scheduler: BaseScheduler,
    callback: Callable[[T], object],
) -> Callable[[T], None]:
    """Wrap ``callback`` so each result is delivered on the scheduler thread."""

    def deliver(result: T) -> None:
        scheduler.call_soon_threadsafe(callback, result, immediate=True)

    return deliver

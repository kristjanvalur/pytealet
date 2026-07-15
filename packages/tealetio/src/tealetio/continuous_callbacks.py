"""Composition helpers for continuous proactor operation callbacks."""

from __future__ import annotations

import heapq
import socket
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, TypeAlias, TypeVar

from .operations import MultishotDelivery
from .socket_helpers import abortive_close
from .tasks import CancelledError

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
    """

    def __init__(self, callback: DeliveryCallback, *, start: int = 0) -> None:
        self._callback = callback
        self._delivered = start
        self._heap: list[MultishotDelivery] = []

    def deliver(self, delivery: MultishotDelivery) -> None:
        if delivery.index is None:
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


class TerminalReorderBuffer:
    """Invoke per-delivery callbacks in leg order; defer early terminals.

    ``_counter`` is the next leg index to hand off (from ``start``). In-order
    deliveries run the constructor callback immediately; an out-of-order
    terminal is held on the heap until a later in-order non-terminal advances
    the counter, then its callback runs after that delivery's callback.
    """

    def __init__(self, callback: DeliveryCallback, *, start: int = 0) -> None:
        self._callback = callback
        self._counter = start
        self._heap: list[MultishotDelivery] = []

    def deliver(self, delivery: MultishotDelivery) -> None:
        if delivery.index is None:
            self._callback(delivery)
            return

        if not delivery.more and delivery.index != self._counter:
            heapq.heappush(self._heap, delivery)
            return

        if delivery.more and delivery.index == self._counter:
            self._counter += 1
            self._callback(delivery)
            self._flush_terminals()
            return

        self._callback(delivery)

    def _flush_terminals(self) -> None:
        while self._heap and self._heap[0].index == self._counter:
            pending = heapq.heappop(self._heap)
            self._callback(pending)


def is_cancellation_delivery(delivery: MultishotDelivery) -> bool:
    """Return True when ``delivery`` ends a continuous op by cancellation.

    Proactor cancel currently surfaces ``CancelledError``; backends may use
    other terminal exceptions later. Accept and receive callbacks should treat
    this as "no further chunks" rather than a transport failure.
    """

    return isinstance(delivery.exception, CancelledError)


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
        scheduler.call_soon_threadsafe(callback, result)

    return deliver

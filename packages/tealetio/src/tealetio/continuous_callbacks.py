"""Composition helpers for continuous proactor operation callbacks."""

from __future__ import annotations

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


class ContinuousLegFinishGate:
    """Defer ``finish_operation`` until all lower leg indices have been delivered.

    Multishot ``accept_many`` (and similar indexed legs) may deliver a terminal
    chunk before earlier indices arrive. Call ``note_delivery`` for each
    ``MultishotDelivery``; the gate finishes only once every index below the
    pending terminal index has been seen.
    """

    def __init__(self) -> None:
        self._pending_terminal: MultishotDelivery | None = None
        self._seen_indices: set[int] = set()

    def note_delivery(self, delivery: MultishotDelivery) -> None:
        if delivery.more:
            self._seen_indices.add(delivery.index)
            self._try_finish()
            return
        if is_cancellation_delivery(delivery) or delivery.exception is not None:
            finish_continuous_delivery(delivery)
            return
        self._pending_terminal = delivery
        self._try_finish()

    def _try_finish(self) -> None:
        delivery = self._pending_terminal
        if delivery is None:
            return
        if all(i in self._seen_indices for i in range(delivery.index)):
            finish_continuous_delivery(delivery)
            self._pending_terminal = None


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

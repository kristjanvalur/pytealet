"""Composition helpers for continuous proactor operation callbacks."""

from __future__ import annotations

import socket
from collections.abc import Callable
from .tasks import CancelledError
from typing import TYPE_CHECKING, TypeVar

from .operation_callbacks import chain_suboperation
from .operations import ContinuousOperation, Operation

AcceptManyDelivery = tuple[socket.socket, bytes | None, BaseException | None]
_MAX_ACCEPT_RECV_SIZE = 2**16

if TYPE_CHECKING:
    from .proactor import Proactor
    from .scheduler import BaseScheduler

T = TypeVar("T")


def normalize_accept_recv_size(recv_size: int | None) -> int | None:
    if recv_size is None:
        return None
    if recv_size <= 0:
        raise ValueError("recv_size must be positive when provided")
    if recv_size > _MAX_ACCEPT_RECV_SIZE:
        return _MAX_ACCEPT_RECV_SIZE
    return recv_size


def wrap_accept_delivery(
    deliver: Callable[[AcceptManyDelivery], object],
) -> Callable[[socket.socket], None]:
    """Adapt a delivery callback to the proactor's bare-socket ``accept_many`` results."""

    def on_conn(conn: socket.socket) -> None:
        deliver((conn, None, None))

    return on_conn


def marshal_to_scheduler(
    scheduler: BaseScheduler,
    callback: Callable[[T], object],
) -> Callable[[T], None]:
    """Wrap ``callback`` so each result is delivered on the scheduler thread."""

    def deliver(result: T) -> None:
        scheduler.call_soon_threadsafe(callback, result)

    return deliver


def accept_read_delivery(
    proactor: Proactor,
    parent: ContinuousOperation[socket.socket],
    deliver: Callable[[AcceptManyDelivery], object],
    *,
    recv_size: int,
) -> Callable[[socket.socket], None]:
    """Read initial bytes on each accepted socket before ``deliver`` runs.

    The proactor emits the accepted ``socket``; this handler submits a nested
    ``recv`` and delivers ``(conn, initial_data, recv_error)`` tuples. Empty reads
    close the connection without delivery. Recv failures are delivered as
    ``(conn, None, recv_error)``.

    ``deliver`` may run after the parent ``ContinuousOperation`` has finished
    (for example terminal multishot accept) when the connection was handed off
    while the parent was still active. Only ``CancelledError`` suppresses
    delivery. If the nested recv cannot be attached, the socket is closed.
    """

    normalized_recv_size = normalize_accept_recv_size(recv_size)
    assert normalized_recv_size is not None

    def on_conn(conn: socket.socket) -> None:
        recv_op = proactor.recv(conn, normalized_recv_size)

        def on_recv_complete(op: Operation[bytes]) -> None:
            exc = op.exception()
            if exc is not None:
                if isinstance(exc, CancelledError):
                    conn.close()
                    return
                deliver((conn, None, exc))
                return
            data = op.result()
            if not data:
                conn.close()
                return
            deliver((conn, data, None))

        if not chain_suboperation(parent, recv_op, on_recv_complete):
            conn.close()

    return on_conn

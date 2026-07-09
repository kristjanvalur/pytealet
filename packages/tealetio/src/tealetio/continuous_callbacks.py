"""Composition helpers for continuous proactor operation callbacks."""

from __future__ import annotations

import socket
from collections.abc import Callable
from .tasks import CancelledError
from typing import TYPE_CHECKING, Any, TypeAlias, TypeVar

from .operation_callbacks import chain_suboperation  # re-exported for continuous call sites
from .operations import ContinuousOperation, Operation

T = TypeVar("T")

AcceptManyDelivery = tuple[socket.socket, bytes | None, BaseException | None]
AcceptStreamsDelivery: TypeAlias = tuple[Any, Any]
_MAX_ACCEPT_RECV_SIZE = 2**16

if TYPE_CHECKING:
    from .proactor import Proactor
    from .scheduler import BaseScheduler


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
    deliver: Callable[[Any], object],
    *,
    recv_size: int,
    deliver_conn_data: bool = False,
) -> Callable[[socket.socket], None]:
    """Read initial bytes on each accepted socket before ``deliver`` runs.

    The proactor emits the accepted ``socket``; this handler submits a nested
    ``recv``. By default ``deliver`` receives ``(conn, initial_data,
    recv_error)`` tuples. With ``deliver_conn_data=True``, a successful read
    calls ``deliver(conn, initial_data)`` so callers can marshal and map the
    socket on the scheduler thread. Recv failures close the socket when
    ``deliver_conn_data`` is set.

    Empty reads close the connection without delivery.

    ``deliver`` may run after the parent ``ContinuousOperation`` has finished
    (for example terminal multishot accept) when the connection was handed off
    while the parent was still active. Only ``CancelledError`` suppresses
    delivery. If the nested recv cannot be attached, the socket is closed.
    """

    normalized_recv_size = normalize_accept_recv_size(recv_size)
    assert normalized_recv_size is not None

    def on_conn(conn: socket.socket) -> None:
        def on_recv_complete(op: Operation[bytes]) -> None:
            exc = op.exception()
            if exc is not None:
                if isinstance(exc, CancelledError):
                    conn.close()
                    return
                if deliver_conn_data:
                    conn.close()
                else:
                    deliver((conn, None, exc))
                return
            data = op.result()
            if not data:
                conn.close()
                return
            if deliver_conn_data:
                try:
                    deliver(conn, data)
                except BaseException:
                    conn.close()
                return
            deliver((conn, data, None))

        if not chain_suboperation(
            parent,
            lambda: proactor.recv(conn, normalized_recv_size),
            on_recv_complete,
        ):
            conn.close()

    return on_conn

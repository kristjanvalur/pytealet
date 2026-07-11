"""Composition helpers for continuous proactor operation callbacks."""

from __future__ import annotations

import socket
from collections.abc import Callable
from .tasks import CancelledError
from typing import TYPE_CHECKING, Any, TypeAlias, TypeVar

from .operation_callbacks import chain_suboperation  # re-exported for continuous call sites
from .operations import ContinuousOperation, Operation
from .socket_helpers import abortive_close

T = TypeVar("T")

AcceptReadResult = tuple[socket.socket, bytes | None, BaseException | None]
AcceptDelivery = tuple[socket.socket, bytes | None]
AcceptStreamsDelivery: TypeAlias = tuple[Any, Any]
AcceptRecvErrorCallback = Callable[[socket.socket, BaseException], object]
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


def wrap_accept_delivery(
    deliver: Callable[[AcceptReadResult], object],
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
    deliver: Callable[[AcceptReadResult], object],
    *,
    recv_size: int,
) -> Callable[[socket.socket], None]:
    """Read initial bytes on each accepted socket before ``deliver`` runs.

    The proactor emits the accepted ``socket``; this handler submits a nested
    ``recv`` and delivers ``AcceptReadResult`` tuples. Recv failures are
    delivered as ``(conn, None, recv_error)``; ``ProactorIOManager`` routes
    those through ``on_recv_error`` instead of the user accept callback. An
    empty successful read is EOF and is delivered as ``(conn, b"", None)``.

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
                    abortive_close(conn)
                    return
                deliver((conn, None, exc))
                return
            data = op.result()
            deliver((conn, data, None))

        if not chain_suboperation(
            parent,
            lambda: proactor.recv(conn, normalized_recv_size),
            on_recv_complete,
        ):
            abortive_close(conn)

    return on_conn

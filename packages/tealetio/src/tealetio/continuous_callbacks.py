"""Composition helpers for continuous proactor operation callbacks."""

from __future__ import annotations

import socket
from collections.abc import Callable
from .tasks import CancelledError
from typing import TYPE_CHECKING, Any, TypeVar, cast

from .operations import ContinuousOperation, Operation
from .recv_iter import RECV_MANY_BUFFER_PRESSURE, _RecvManyResult

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


def before_delivery(
    handler: Callable[[T], T],
    deliver: Callable[[T], object],
) -> Callable[[T], None]:
    """Run ``handler`` on the delivery thread, then pass its result to ``deliver``."""

    def wrapped(result: T) -> None:
        deliver(handler(result))

    return wrapped


def marshal_to_scheduler(
    scheduler: BaseScheduler,
    callback: Callable[[T], object],
) -> Callable[[T], None]:
    """Wrap ``callback`` so each result is delivered on the scheduler thread."""

    def deliver(result: T) -> None:
        scheduler.call_soon_threadsafe(callback, result)

    return deliver


def chain_suboperation(
    parent: ContinuousOperation[Any],
    suboperation: Operation[T],
    on_complete: Callable[[Operation[T]], object],
) -> None:
    """Track ``suboperation`` and run ``on_complete`` from its done callback."""

    if not parent.attach_suboperation(suboperation):
        suboperation.cancel()
        return

    def complete(op: Operation[T]) -> None:
        try:
            on_complete(op)
        finally:
            parent.detach_suboperation(op)

    suboperation.add_done_callback(complete)


def recv_many_echo_delivery(
    proactor: Proactor,
    parent: ContinuousOperation[_RecvManyResult],
    deliver: Callable[[_RecvManyResult], object],
    *,
    fire_and_forget: bool = False,
) -> Callable[[_RecvManyResult], None]:
    """Echo each data chunk via a nested send operation.

    By default ``deliver`` runs only after the nested send succeeds. When
    ``fire_and_forget`` is true, the send is submitted and tracked for
    cancellation, but ``deliver`` runs immediately without waiting for echo
    completion. Send failures are always swallowed; ``recv_many`` keeps running.
    """

    sock = cast(socket.socket, parent.fileobj)

    def on_result(result: _RecvManyResult) -> None:
        index, payload = result
        if index == RECV_MANY_BUFFER_PRESSURE:
            deliver(result)
            return
        if isinstance(payload, memoryview) and payload:
            send_op = proactor.send(sock, payload.tobytes())
            if fire_and_forget:

                def on_send_complete(_op: Operation[Any]) -> None:
                    return

                chain_suboperation(parent, send_op, on_send_complete)
                deliver(result)
                return

            def on_send_complete(op: Operation[Any]) -> None:
                if op.exception() is not None:
                    return
                deliver(result)

            chain_suboperation(parent, send_op, on_send_complete)
            return
        deliver(result)

    return on_result


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

        chain_suboperation(parent, recv_op, on_recv_complete)

    return on_conn

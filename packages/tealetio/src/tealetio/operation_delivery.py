"""Operation delivery handlers for proactor-level chaining."""

from __future__ import annotations

import socket
from typing import Protocol, cast

from .operations import DeliveryHandler, Operation


class _RecvSubmitProactor(Protocol):
    def recv(
        self,
        sock: socket.socket,
        n: int,
        *,
        delivery: DeliveryHandler | None = None,
    ) -> Operation[bytes]: ...


def double_recv_delivery(size: int) -> DeliveryHandler:
    """Chain two consecutive ``recv`` operations into one ``bytes`` result."""

    def delivery(
        proactor: _RecvSubmitProactor,
        operation: Operation[bytes],
        result: object,
        exception: BaseException | None,
    ) -> None:
        if exception is not None:
            operation.complete_error(exception)
            return
        first = cast(bytes, result)
        sock = cast(socket.socket, operation.fileobj)
        second = proactor.recv(sock, size)

        def on_second(second_operation: Operation[bytes]) -> None:
            second_exc = second_operation.exception()
            if second_exc is not None:
                operation.complete_error(second_exc)
            else:
                operation.complete(first + second_operation.result())

        second.add_done_callback(on_second)

    return delivery
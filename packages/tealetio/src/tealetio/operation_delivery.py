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

        def second_delivery(
            _proactor: _RecvSubmitProactor,
            _second_operation: Operation[bytes],
            second_result: object,
            second_exception: BaseException | None,
        ) -> None:
            # Inner recv legs need not call complete(); backends release the fd
            # or uring entry before deliver() runs on the worker thread.
            if second_exception is not None:
                operation.complete_error(second_exception)
                return
            operation.complete(first + cast(bytes, second_result))

        proactor.recv(sock, size, delivery=second_delivery)

    return delivery
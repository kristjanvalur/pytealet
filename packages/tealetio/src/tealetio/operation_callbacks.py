"""Callback composition for one-shot proactor operations."""

from __future__ import annotations

import socket
import weakref
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, TypeVar, cast

from .operations import DeliveryHandler, Operation, OperationFactory
from .types import SocketSendBuffer

if TYPE_CHECKING:
    from .proactor import Proactor

T = TypeVar("T")


def chain_suboperation(
    parent: Operation[Any],
    suboperation: Operation[T],
    on_complete: Callable[[Operation[T]], object],
) -> bool:
    """Track ``suboperation`` and run ``on_complete`` from its done callback.

    Returns ``False`` when the parent is already done or cancelling (the
    suboperation is cancelled and no completion handler is registered).
    """

    if not parent.attach_suboperation(suboperation):
        suboperation.cancel()
        return False

    suboperation._suboperation_parent = weakref.ref(parent)

    def complete(op: Operation[T]) -> None:
        try:
            on_complete(op)
        finally:
            parent.detach_suboperation(op)

    suboperation.add_done_callback(complete)
    return True


def connect_initial_send_delivery(
    proactor: Proactor,
    initial: SocketSendBuffer,
) -> DeliveryHandler:
    """After connect succeeds, send ``initial`` bytes then complete with ``None``."""

    payload = memoryview(initial) if initial is not None else None

    def delivery(
        _proactor: object,
        operation: Operation[Any],
        _result: object,
        exception: BaseException | None,
    ) -> None:
        if exception is not None:
            operation.complete_error(exception)
            return
        if payload is None or not payload:
            operation.complete(None)
            return
        sock = cast(socket.socket, operation.fileobj)

        def on_send_complete(send_op: Operation[Any]) -> None:
            send_exc = send_op.exception()
            if send_exc is not None:
                operation.complete_error(send_exc)
                return
            operation.complete(None)

        send_op = Operation(kind="send", fileobj=sock)
        if not chain_suboperation(operation, send_op, on_send_complete):
            return

        def send_factory(kind: str, fileobj: object | None) -> Operation[Any]:
            return send_op

        proactor.send(sock, payload, operation_factory=send_factory)

    return delivery


def connect_initial_send_operation_factory(
    proactor: Proactor,
    initial: SocketSendBuffer,
) -> OperationFactory:
    """Factory for ``proactor.connect`` when ``initial`` send composition is needed."""

    handler = connect_initial_send_delivery(proactor, initial)

    def factory(kind: str, fileobj: object | None) -> Operation[Any]:
        operation = Operation(kind=kind, fileobj=fileobj)
        operation.set_delivery(handler)
        return operation

    return factory

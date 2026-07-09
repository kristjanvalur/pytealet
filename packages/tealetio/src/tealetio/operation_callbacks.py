"""Callback composition for one-shot proactor operations."""

from __future__ import annotations

import socket
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, TypeVar, cast

from .operations import DeliveryHandler, Operation, OperationFactory
from .types import SocketSendBuffer

if TYPE_CHECKING:
    from .proactor import Proactor

T = TypeVar("T")


def _register_suboperation(
    parent: Operation[Any],
    suboperation: Operation[T],
    on_complete: Callable[[Operation[T]], object],
) -> bool:
    """Register a child completion handler; caller must hold ``parent._lock``."""

    if not parent.attach_suboperation(suboperation):
        return False

    def complete(op: Operation[T]) -> None:
        try:
            on_complete(op)
        finally:
            parent.detach_suboperation(op)

    suboperation.add_done_callback(complete)
    return True


def chain_suboperation(
    parent: Operation[Any],
    spawn: Callable[[], Operation[T]],
    on_complete: Callable[[Operation[T]], object],
) -> bool:
    """Spawn a child under ``parent._lock`` and run ``on_complete`` on completion.

    Serialises against ``parent.cancel()`` so an in-flight backend submit
    cannot outrun ``attach_suboperation()``. Returns ``False`` when the parent
    is already done or cancelling (the suboperation is cancelled when spawned).
    If the child is already done when registered, ``add_done_callback`` runs
    ``on_complete`` immediately.
    """

    with parent._lock:
        if parent._done or parent._cancelling:
            return False
        child = spawn()
        if not _register_suboperation(parent, child, on_complete):
            child.cancel()
            return False
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

        if not chain_suboperation(
            operation,
            lambda: proactor.send(sock, payload),
            on_send_complete,
        ):
            return

    return delivery


def _close_socket(sock: socket.socket) -> None:
    try:
        sock.close()
    except OSError:
        pass


def create_connect_delivery(
    proactor: Proactor,
    connect_to: Any,
) -> DeliveryHandler:
    """After create_socket succeeds, connect then complete with the socket."""

    def delivery(
        _proactor: object,
        operation: Operation[Any],
        result: object,
        exception: BaseException | None,
    ) -> None:
        if exception is not None:
            operation.complete_error(exception)
            return
        sock = cast(socket.socket, result)

        def on_connect_complete(op: Operation[Any]) -> None:
            connect_exc = op.exception()
            if connect_exc is not None:
                _close_socket(sock)
                operation.complete_error(connect_exc)
                return
            operation.complete(sock)

        try:
            if not chain_suboperation(
                operation,
                lambda: proactor.connect(sock, connect_to),
                on_connect_complete,
            ):
                _close_socket(sock)
        except BaseException as exc:
            _close_socket(sock)
            operation.complete_error(exc)

    return delivery


def create_connect_operation_factory(
    proactor: Proactor,
    connect_to: Any,
) -> OperationFactory:
    """Factory for ``proactor.create_socket`` when ``connect_to`` is set."""

    handler = create_connect_delivery(proactor, connect_to)

    def factory(kind: str, fileobj: object | None) -> Operation[Any]:
        operation = Operation(kind=kind, fileobj=fileobj)
        operation.set_delivery(handler)
        return operation

    return factory


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

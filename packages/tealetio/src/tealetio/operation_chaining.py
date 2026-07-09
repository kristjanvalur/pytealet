"""Operation chaining factories and link handlers for proactor IO."""

from __future__ import annotations

import socket
from collections.abc import Callable
from .tasks import CancelledError
from typing import Any, cast

from .types import SocketSendBuffer
from .operations import AdvanceHook, DeliveryHandler, Operation, OperationFactory
from .proactor import Proactor

NextOperation = Callable[[Operation[Any], Any | None], Operation[Any] | None]


def operation_factory(
    *,
    parent: Operation[Any] | None = None,
    delivery: DeliveryHandler | None = None,
    advance_hook: AdvanceHook | None = None,
) -> OperationFactory:
    """Build an ``Operation`` with optional chain parent, delivery, and hook.

    Omit ``parent`` for a chain root. When ``parent`` is set the parent also
    forwards ``cancel()`` to the new child.
    """

    def factory(kind: str, fileobj: object | None) -> Operation[Any]:
        child = Operation(kind=kind, fileobj=fileobj)
        if parent is not None and not parent.attach_child(child):
            child._set_cancelled()
        if delivery is not None:
            child.set_delivery(delivery)
        if advance_hook is not None:
            child.set_advance_hook(advance_hook)
        return child

    return factory


def _abort_chain_extension(operation: Operation[Any]) -> None:
    """Bubble cancellation upward when a success path cannot extend the chain."""

    operation.advance(exception=CancelledError())


def _chain_next_operation(
    parent: Operation[Any],
    next_operation: NextOperation | None,
    *,
    link_result: Any | None = None,
) -> None:
    if not parent.may_extend_chain():
        _abort_chain_extension(parent)
        return
    try:
        if next_operation is not None:
            next_operation(parent, link_result)
            return
        parent.advance(result=link_result)
    except BaseException as exc:
        parent.advance(exception=exc)


def _close_socket(sock: socket.socket) -> None:
    try:
        sock.close()
    except OSError:
        pass


def chained_fdclose_link(
    *,
    next_operation: NextOperation,
    on_socket: Callable[[socket.socket], None] | None = None,
    shape_success: Callable[[socket.socket], Any] | None = None,
) -> OperationFactory:
    """Forward a delivered socket into the next chained operation.

    Top-level decorator for socket-delivery completions. The proactor must
    ``deliver()`` a ``socket.socket`` in ``result``. Delivery handlers only
    start the chain and propagate failures via ``advance()``. The advance hook
    owns cleanup: it closes the captured socket on bubbled errors and shapes
    the root success result via ``shape_success`` before ``advance_continue()``.
    """

    sock: socket.socket | None = None

    def advance(
        advance_operation: Operation[Any],
        advance_result: object,
        advance_exception: BaseException | None,
    ) -> None:
        nonlocal sock
        if advance_exception is not None:
            if sock is not None:
                _close_socket(sock)
                sock = None
            advance_operation.advance_continue(exception=advance_exception)
            return
        if shape_success is not None:
            assert sock is not None
            advance_operation.advance_continue(result=shape_success(sock))
            return
        advance_operation.advance_continue(result=advance_result)

    def delivery(
        _proactor: object,
        operation: Operation[Any],
        result: object,
        exception: BaseException | None,
    ) -> None:
        nonlocal sock
        if exception is not None:
            operation.advance(exception=exception)
            return
        delivered = cast(socket.socket, result)
        if not operation.may_extend_chain():
            _close_socket(delivered)
            _abort_chain_extension(operation)
            return
        if on_socket is not None:
            on_socket(delivered)
        sock = delivered
        _chain_next_operation(
            operation,
            next_operation,
            link_result=delivered,
        )

    return operation_factory(delivery=delivery, advance_hook=advance)


def chained_connect_link(
    *,
    next_operation: NextOperation | None = None,
) -> DeliveryHandler:
    """Spawn the next chained leg after a backend connect succeeds."""

    def delivery(
        _proactor: object,
        operation: Operation[Any],
        result: object,
        exception: BaseException | None,
    ) -> None:
        if exception is not None:
            operation.advance(exception=exception)
            return
        if not operation.may_extend_chain():
            _abort_chain_extension(operation)
            return
        _chain_next_operation(operation, next_operation, link_result=result)

    return delivery


def chained_send_link(
    proactor: Proactor,
    data: SocketSendBuffer | None,
    *,
    next_operation: NextOperation | None = None,
) -> DeliveryHandler:
    """Append a sendall leg after a parent socket operation succeeds."""

    def start_send_link(parent: Operation[Any]) -> Operation[None] | None:
        payload = memoryview(data) if data is not None else None
        if payload is None or not payload:
            _chain_next_operation(parent, next_operation, link_result=None)
            return None

        sock = cast(socket.socket, parent.fileobj)

        def send_delivery(
            _proactor: object,
            send_operation: Operation[None],
            _result: object,
            send_exception: BaseException | None,
        ) -> None:
            if send_exception is not None:
                send_operation.advance(exception=send_exception)
                return
            if next_operation is not None:
                _chain_next_operation(parent, next_operation, link_result=_result)
                return
            send_operation.advance(result=_result)

        return proactor.send(
            sock,
            payload,
            operation_factory=operation_factory(parent=parent, delivery=send_delivery),
        )

    def delivery(
        _proactor: object,
        operation: Operation[Any],
        result: object,
        exception: BaseException | None,
    ) -> None:
        if exception is not None:
            operation.advance(exception=exception)
            return
        if not operation.may_extend_chain():
            _abort_chain_extension(operation)
            return
        start_send_link(operation)

    return delivery


def connect_send_chain_factory(
    proactor: Proactor,
    initial: SocketSendBuffer | None,
    *,
    parent: Operation[Any] | None = None,
    advance_hook: AdvanceHook | None = None,
) -> OperationFactory:
    """Factory for connect → send chaining.

    Delivery starts the send leg on connect success and propagates backend
    failures via ``advance()``. As a chain root (``parent`` omitted), installs
    an advance hook that completes with ``None``. As a child, omits the hook
    so successes and errors bubble to the parent link via ``advance()``.
    """

    if parent is None and advance_hook is None:

        def advance(
            advance_operation: Operation[Any],
            _advance_result: object,
            advance_exception: BaseException | None,
        ) -> None:
            if advance_exception is not None:
                advance_operation.advance_continue(exception=advance_exception)
                return
            advance_operation.advance_continue(result=None)

        advance_hook = advance

    return operation_factory(
        parent=parent,
        delivery=chained_send_link(proactor, initial, next_operation=None),
        advance_hook=advance_hook,
    )


def connect_initial_send_factory(proactor: Proactor, initial: SocketSendBuffer) -> OperationFactory:
    """Factory for connect + initial send (delegates to ``operation_callbacks``)."""

    from .operation_callbacks import connect_initial_send_operation_factory

    return connect_initial_send_operation_factory(proactor, initial)


def create_socket_chain_factory(
    proactor: Proactor,
    connect_to: Any,
    initial_data: SocketSendBuffer | None,
    *,
    on_socket: Callable[[socket.socket], None] | None = None,
) -> OperationFactory:
    """Build create → connect → send for ``ProactorIOManager.sock_create``.

    Only the root ``create_socket`` operation returns the socket; connect and
    send legs complete with ``None``.
    """

    def next_operation(
        parent: Operation[socket.socket],
        link_result: Any | None,
    ) -> Operation[Any] | None:
        sock = cast(socket.socket, link_result)
        return proactor.connect(
            sock,
            connect_to,
            operation_factory=connect_send_chain_factory(
                proactor,
                initial_data,
                parent=parent,
            ),
        )

    return chained_fdclose_link(
        next_operation=next_operation,
        on_socket=on_socket,
        shape_success=lambda sock: sock,
    )

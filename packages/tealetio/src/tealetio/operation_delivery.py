"""Operation delivery handlers for proactor-level chaining."""

from __future__ import annotations

import socket
from collections.abc import Callable
from typing import Any, Protocol, cast

from .io_manager import SocketSendBuffer
from .operations import AdvanceHook, DeliveryHandler, Operation, OperationFactory

_DeliveryFail = Callable[[BaseException], None]
NextOperation = Callable[[Any, Operation[Any], Any | None], Operation[Any] | None]


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
        if parent is not None:
            child.set_chain_parent(parent)
            parent.set_cancel_forward(child)
        if delivery is not None:
            child.set_delivery(delivery)
        if advance_hook is not None:
            child.set_advance_hook(advance_hook)
        return child

    return factory


class _RecvSubmitProactor(Protocol):
    def recv(
        self,
        sock: socket.socket,
        n: int,
        *,
        operation_factory: OperationFactory | None = None,
    ) -> Operation[bytes]: ...


class _SendSubmitProactor(Protocol):
    def send(
        self,
        sock: socket.socket,
        data: SocketSendBuffer,
        *,
        operation_factory: OperationFactory | None = None,
    ) -> Operation[None]: ...


def _chain_next_operation(
    proactor: Any,
    parent: Operation[Any],
    next_operation: NextOperation | None,
    *,
    link_result: Any | None = None,
) -> None:
    if next_operation is not None:
        next_operation(proactor, parent, link_result)
        return
    parent.advance(proactor)


def _close_socket(sock: socket.socket) -> None:
    try:
        sock.close()
    except OSError:
        pass


def chained_fdclose_link(
    *,
    fail: _DeliveryFail,
    next_operation: NextOperation,
    on_socket: Callable[[socket.socket], None] | None = None,
    shape_success: Callable[[socket.socket], Any] | None = None,
) -> OperationFactory:
    """Forward a delivered socket into the next chained operation.

    Top-level decorator for socket-delivery completions. The proactor must
    ``deliver()`` a ``socket.socket`` in ``result``. The socket is captured when
    delivery runs and shared with the advance hook. Child successes bubble
    through ``advance()``; the hook shapes the root result via ``shape_success``
    and calls ``advance_continue()``. Child failures close the created socket
    before ``advance_continue()``. ``fail`` handles proactor failures before the
    chain starts.
    """

    sock_ref: list[socket.socket | None] = [None]

    def advance(
        advance_proactor: object,
        advance_operation: Operation[Any],
        advance_result: object,
        advance_exception: BaseException | None,
    ) -> None:
        sock = sock_ref[0]
        if advance_exception is not None:
            if sock is not None:
                _close_socket(sock)
            advance_operation.advance_continue(
                advance_proactor,
                exception=advance_exception,
            )
            return
        if shape_success is not None:
            assert sock is not None
            advance_operation.advance_continue(
                advance_proactor,
                result=shape_success(sock),
            )
            return
        advance_operation.advance_continue(
            advance_proactor,
            result=advance_result,
        )

    def delivery(
        proactor: object,
        operation: Operation[Any],
        result: object,
        exception: BaseException | None,
    ) -> None:
        if exception is not None:
            fail(exception)
            return
        sock = cast(socket.socket, result)
        if operation.done():
            _close_socket(sock)
            return
        if on_socket is not None:
            on_socket(sock)
        sock_ref[0] = sock
        try:
            _chain_next_operation(
                proactor,
                operation,
                next_operation,
                link_result=sock,
            )
        except BaseException as exc:
            _close_socket(sock)
            sock_ref[0] = None
            fail(exc)

    return operation_factory(delivery=delivery, advance_hook=advance)


def chained_connect_link(
    *,
    next_operation: NextOperation | None = None,
) -> DeliveryHandler:
    """Spawn the next chained leg after a backend connect succeeds."""

    def delivery(
        proactor: object,
        operation: Operation[Any],
        result: object,
        exception: BaseException | None,
    ) -> None:
        if exception is not None:
            operation.advance(proactor, exception=exception)
            return
        _chain_next_operation(proactor, operation, next_operation)

    return delivery


def chained_send_link(
    data: SocketSendBuffer | None,
    *,
    next_operation: NextOperation | None = None,
) -> DeliveryHandler:
    """Append a sendall leg after a parent socket operation succeeds."""

    def start_send_link(
        proactor: _SendSubmitProactor,
        parent: Operation[Any],
    ) -> Operation[None] | None:
        payload = memoryview(data) if data is not None else None
        if payload is None or not payload:
            _chain_next_operation(proactor, parent, next_operation)
            return None

        sock = cast(socket.socket, parent.fileobj)

        def send_delivery(
            _proactor: _SendSubmitProactor,
            send_operation: Operation[None],
            _result: object,
            send_exception: BaseException | None,
        ) -> None:
            if send_exception is not None:
                send_operation.advance(proactor, exception=send_exception)
                return
            if next_operation is not None:
                _chain_next_operation(proactor, parent, next_operation)
                return
            send_operation.advance(proactor)

        return proactor.send(
            sock,
            payload,
            operation_factory=operation_factory(parent=parent, delivery=send_delivery),
        )

    def delivery(
        proactor: _SendSubmitProactor,
        operation: Operation[Any],
        result: object,
        exception: BaseException | None,
    ) -> None:
        if exception is not None:
            operation.advance(proactor, exception=exception)
            return
        start_send_link(proactor, operation)

    return delivery


def connect_initial_send_factory(initial: SocketSendBuffer) -> OperationFactory:
    """Factory for ``ProactorIOManager.sock_connect(..., initial=...)``."""

    def advance(
        advance_proactor: object,
        advance_operation: Operation[Any],
        _advance_result: object,
        advance_exception: BaseException | None,
    ) -> None:
        if advance_exception is not None:
            advance_operation.advance_continue(advance_proactor, exception=advance_exception)
            return
        advance_operation.advance_continue(advance_proactor, result=True)

    return operation_factory(
        delivery=chained_send_link(initial),
        advance_hook=advance,
    )


def double_recv_delivery(size: int) -> DeliveryHandler:
    """Chain two consecutive ``recv`` operations into one ``bytes`` result."""

    def delivery(
        proactor: _RecvSubmitProactor,
        operation: Operation[bytes],
        result: object,
        exception: BaseException | None,
    ) -> None:
        if exception is not None:
            if not operation.done():
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
            if operation.done():
                return
            if second_exception is not None:
                operation.complete_error(second_exception)
                return
            operation.complete(first + cast(bytes, second_result))

        proactor.recv(
            sock,
            size,
            operation_factory=operation_factory(
                parent=operation,
                delivery=second_delivery,
            ),
        )

    return delivery


def double_recv_factory(size: int) -> OperationFactory:
    """Factory for the root leg of a double ``recv`` chain."""

    return operation_factory(delivery=double_recv_delivery(size))
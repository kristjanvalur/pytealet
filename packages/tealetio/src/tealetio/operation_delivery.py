"""Operation delivery handlers for proactor-level chaining."""

from __future__ import annotations

import socket
from collections.abc import Callable
from typing import Any, Protocol, cast

from .io_manager import SocketSendBuffer
from .operations import DeliveryHandler, Operation, OperationFactory

CreateSocketResult = tuple[socket.socket, bool, bool]
_DeliverySucceed = Callable[[], None]
_DeliveryFail = Callable[[BaseException], None]
NextOperation = Callable[[Any, Operation[Any], Any | None], Operation[Any] | None]


def operation_factory(
    *,
    parent: Operation[Any] | None = None,
    delivery: DeliveryHandler | None = None,
) -> OperationFactory:
    """Build a chained child ``Operation`` with parent and delivery wired."""

    def factory(kind: str, fileobj: object | None) -> Operation[Any]:
        child = Operation(kind=kind, fileobj=fileobj)
        if parent is not None:
            child.set_chain_parent(parent)
        if delivery is not None:
            child.set_delivery(delivery)
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


class _ConnectSubmitProactor(_SendSubmitProactor, Protocol):
    def connect(
        self,
        sock: socket.socket,
        address: Any,
        *,
        operation_factory: OperationFactory | None = None,
    ) -> Operation[None] | Operation[bool]: ...


def _chain_next_operation(
    proactor: Any,
    parent: Operation[Any],
    next_operation: NextOperation | None,
    *,
    link_result: Any | None = None,
    succeed: _DeliverySucceed | None = None,
    terminal_result: Any = None,
) -> None:
    if next_operation is not None:
        child = next_operation(proactor, parent, link_result)
        if child is not None:
            parent.set_cancel_forward(child)
        return
    if succeed is not None:
        succeed()
        return
    parent.advance_up(proactor, result=terminal_result)


def _close_socket(sock: socket.socket) -> None:
    try:
        sock.close()
    except OSError:
        pass


def _start_send_link(
    proactor: _SendSubmitProactor,
    parent: Operation[Any],
    data: SocketSendBuffer | None,
    *,
    succeed: _DeliverySucceed | None = None,
    fail: _DeliveryFail | None = None,
    next_operation: NextOperation | None = None,
    terminal_result: Any = None,
) -> Operation[None] | None:
    payload = memoryview(data) if data is not None else None
    if payload is None or not payload:
        _chain_next_operation(
            proactor,
            parent,
            next_operation,
            succeed=succeed,
            terminal_result=terminal_result,
        )
        return None

    sock = cast(socket.socket, parent.fileobj)

    def send_delivery(
        _proactor: _SendSubmitProactor,
        send_operation: Operation[None],
        _result: object,
        send_exception: BaseException | None,
    ) -> None:
        if send_exception is not None:
            if fail is not None:
                fail(send_exception)
            else:
                send_operation.advance_up(proactor, exception=send_exception)
            return
        if next_operation is not None:
            _chain_next_operation(
                proactor,
                parent,
                next_operation,
                succeed=succeed,
            )
            return
        if succeed is not None:
            succeed()
            return
        send_operation.advance_up(proactor, result=terminal_result)

    return proactor.send(
        sock,
        payload,
        operation_factory=operation_factory(parent=parent, delivery=send_delivery),
    )


def chained_fdclose_link(
    *,
    fail: _DeliveryFail,
    next_operation: NextOperation | None = None,
    succeed: _DeliverySucceed | None = None,
    on_socket: Callable[[socket.socket], None] | None = None,
) -> DeliveryHandler:
    """Forward a delivered socket into the next chained operation.

    Top-level link for ``create_socket``-style completions. The proactor must
    ``deliver()`` a ``socket.socket`` in ``result``. The socket is captured in
    this link's ``advance`` closure. Child failures bubble through
    ``advance()``; this link closes the created socket before calling
    ``advance_up()``. One of ``next_operation`` or ``succeed`` is required.
    """

    if next_operation is None and succeed is None:
        raise ValueError("chained_fdclose_link requires next_operation or succeed")

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

        def advance(
            advance_proactor: object,
            advance_operation: Operation[Any],
            advance_result: object,
            advance_exception: BaseException | None,
        ) -> None:
            if advance_exception is not None:
                _close_socket(sock)
            advance_operation.advance_up(
                advance_proactor,
                result=advance_result,
                exception=advance_exception,
            )

        operation.set_chain_advance(advance)
        try:
            _chain_next_operation(
                proactor,
                operation,
                next_operation,
                link_result=sock,
                succeed=succeed,
            )
        except BaseException as exc:
            _close_socket(sock)
            fail(exc)

    return delivery


def chained_connect_link(
    *,
    succeed: _DeliverySucceed | None = None,
    fail: _DeliveryFail | None = None,
    next_operation: NextOperation | None = None,
    terminal_result: Any = None,
) -> DeliveryHandler:
    """Advance a connect ``Operation`` after the backend connect succeeds."""

    if succeed is None and next_operation is None and terminal_result is None:
        raise ValueError("chained_connect_link requires succeed, next_operation, or terminal_result")

    def delivery(
        proactor: object,
        operation: Operation[Any],
        result: object,
        exception: BaseException | None,
    ) -> None:
        if exception is not None:
            if fail is not None:
                fail(exception)
            else:
                operation.advance_up(proactor, exception=exception)
            return
        _chain_next_operation(
            proactor,
            operation,
            next_operation,
            succeed=succeed,
            terminal_result=terminal_result,
        )

    return delivery


def chained_send_link(
    data: SocketSendBuffer | None,
    *,
    succeed: _DeliverySucceed | None = None,
    fail: _DeliveryFail | None = None,
    next_operation: NextOperation | None = None,
    terminal_result: Any = None,
) -> DeliveryHandler:
    """Append a sendall leg after a parent socket operation succeeds."""

    if succeed is None and next_operation is None and terminal_result is None:
        raise ValueError("chained_send_link requires succeed, next_operation, or terminal_result")

    def delivery(
        proactor: _SendSubmitProactor,
        operation: Operation[Any],
        result: object,
        exception: BaseException | None,
    ) -> None:
        if exception is not None:
            if fail is not None:
                fail(exception)
            else:
                operation.advance_up(proactor, exception=exception)
            return
        child = _start_send_link(
            proactor,
            operation,
            data,
            succeed=succeed,
            fail=fail,
            next_operation=next_operation,
            terminal_result=terminal_result,
        )
        if child is not None:
            operation.set_cancel_forward(child)

    return delivery


def connect_initial_send_factory(initial: SocketSendBuffer) -> OperationFactory:
    """Factory for ``connect(..., initial=...)`` on backends that support it."""

    return operation_factory(delivery=connect_initial_send_delivery(initial))


def connect_initial_send_delivery(initial: SocketSendBuffer) -> DeliveryHandler:
    """Delivery handler for ``connect(..., initial=...)``."""

    def next_operation(
        proactor: _SendSubmitProactor,
        parent: Operation[bool],
        _link_result: Any | None,
    ) -> Operation[None] | None:
        return _start_send_link(
            proactor,
            parent,
            initial,
            terminal_result=True,
        )

    return chained_connect_link(next_operation=next_operation)


def create_socket_delivery(
    connect_to: Any | None,
    initial_data: SocketSendBuffer | None,
    *,
    succeed: Callable[[socket.socket, bool, bool], None],
    fail: _DeliveryFail,
    on_socket: Callable[[socket.socket], None] | None = None,
) -> DeliveryHandler:
    """Assemble create → connect → send for ``create_socket``."""

    def terminal_result(sock: socket.socket) -> CreateSocketResult:
        return (sock, True, initial_data is not None)

    def next_operation(
        proactor: _ConnectSubmitProactor,
        parent: Operation[CreateSocketResult],
        link_result: Any | None,
    ) -> Operation[Any] | None:
        sock = cast(socket.socket, link_result)

        if connect_to is None or sock.family == socket.AF_UNIX:
            parent.advance_up(proactor, result=(sock, False, False))
            return None

        def send_next(
            connect_proactor: _ConnectSubmitProactor,
            connect_parent: Operation[Any],
            _link_result: Any | None,
        ) -> Operation[None] | None:
            return _start_send_link(
                connect_proactor,
                connect_parent,
                initial_data,
                terminal_result=terminal_result(sock),
            )

        return proactor.connect(
            sock,
            connect_to,
            operation_factory=operation_factory(
                parent=parent,
                delivery=chained_connect_link(next_operation=send_next),
            ),
        )

    return chained_fdclose_link(
        fail=fail,
        next_operation=next_operation,
        on_socket=on_socket,
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

        second = proactor.recv(
            sock,
            size,
            operation_factory=operation_factory(delivery=second_delivery),
        )
        operation.set_cancel_forward(second)

    return delivery


def double_recv_factory(size: int) -> OperationFactory:
    """Factory for the root leg of a double ``recv`` chain."""

    return operation_factory(delivery=double_recv_delivery(size))
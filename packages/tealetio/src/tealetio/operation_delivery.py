"""Operation delivery handlers for proactor-level chaining."""

from __future__ import annotations

import socket
from collections.abc import Callable
from typing import Any, Protocol, cast

from .io_manager import SocketSendBuffer
from .operations import DeliveryHandler, Operation

CreateSocketResult = tuple[socket.socket, bool, bool]
_DeliverySucceed = Callable[[], None]
_DeliveryFail = Callable[[BaseException], None]


class _RecvSubmitProactor(Protocol):
    def recv(
        self,
        sock: socket.socket,
        n: int,
        *,
        delivery: DeliveryHandler | None = None,
    ) -> Operation[bytes]: ...


class _SendSubmitProactor(Protocol):
    def send(
        self,
        sock: socket.socket,
        data: SocketSendBuffer,
        *,
        delivery: DeliveryHandler | None = None,
    ) -> Operation[None]: ...


class _ConnectSubmitProactor(_SendSubmitProactor, Protocol):
    def connect(
        self,
        sock: socket.socket,
        address: Any,
        *,
        delivery: DeliveryHandler | None = None,
    ) -> Operation[None] | Operation[bool]: ...


def _advance_chain(
    proactor: object,
    operation: Operation[Any],
    advance_result: object,
    *,
    succeed: _DeliverySucceed | None,
    next_link: DeliveryHandler | None,
) -> None:
    if next_link is not None:
        next_link(proactor, operation, advance_result, None)
        return
    assert succeed is not None
    succeed()


def _close_socket(sock: socket.socket) -> None:
    try:
        sock.close()
    except OSError:
        pass


def chained_fdclose_link(
    *,
    fail: _DeliveryFail,
    next_link: DeliveryHandler,
    on_socket: Callable[[socket.socket], None] | None = None,
) -> DeliveryHandler:
    """Forward a delivered socket to ``next_link`` and close it on error.

    Top-level link for ``create_socket``-style completions. The proactor must
    ``deliver()`` a ``socket.socket`` in ``result``. That socket is passed to
    ``next_link``. Backend errors, failures raised from ``next_link``, and late
    deliveries after cancel close the socket from ``result`` before ``fail()``
    runs. ``on_socket`` is an optional side hook (for example proactor cancel
    teardown) invoked once the socket is accepted.
    """

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
        try:
            next_link(proactor, operation, sock, None)
        except BaseException as exc:
            _close_socket(sock)
            fail(exc)

    return delivery


def chained_connect_link(
    *,
    succeed: _DeliverySucceed | None = None,
    fail: _DeliveryFail,
    next_link: DeliveryHandler | None = None,
) -> DeliveryHandler:
    """Advance a connect ``Operation`` after the backend connect succeeds.

    Use as the ``delivery`` handler on a ``connect`` operation (or pass to
    ``proactor.connect(..., delivery=...)``). On success the handler forwards
    to ``next_link`` or calls ``succeed()``; connect errors route to ``fail()``.

    Compose with ``chained_send_link`` via ``next_link`` for connect-time
    sendall. One of ``succeed`` or ``next_link`` is required.
    """

    if succeed is None and next_link is None:
        raise ValueError("chained_connect_link requires succeed or next_link")

    def delivery(
        proactor: object,
        operation: Operation[Any],
        result: object,
        exception: BaseException | None,
    ) -> None:
        if exception is not None:
            fail(exception)
            return
        _advance_chain(
            proactor,
            operation,
            result,
            succeed=succeed,
            next_link=next_link,
        )

    return delivery


def chained_send_link(
    data: SocketSendBuffer | None,
    *,
    succeed: _DeliverySucceed | None = None,
    fail: _DeliveryFail,
    next_link: DeliveryHandler | None = None,
) -> DeliveryHandler:
    """Append a sendall leg after a parent socket operation succeeds.

    Use as the ``delivery`` handler on any socket ``Operation`` whose
    ``fileobj`` is the connected socket. When the parent backend completion
    arrives without error, this optionally submits ``proactor.send()`` to
    drain ``data``. The send result is not merged into the parent; the parent
    either advances to ``next_link``, finishes via ``succeed()``, or fails via
    ``fail()``.

    ``data`` is ``None`` to skip the send leg. An empty buffer skips ``send``
    submission. Provide ``next_link`` to compose further delivery handlers on
    the same parent operation after this leg (or immediately when send is
    skipped). The terminal link should call ``succeed()``; intermediate links
    normally pass ``next_link`` only.

    One of ``succeed`` or ``next_link`` is required.
    """

    if succeed is None and next_link is None:
        raise ValueError("chained_send_link requires succeed or next_link")

    payload = memoryview(data) if data is not None else None

    def delivery(
        proactor: _SendSubmitProactor,
        operation: Operation[Any],
        result: object,
        exception: BaseException | None,
    ) -> None:
        if exception is not None:
            fail(exception)
            return
        if payload is None or not payload:
            _advance_chain(
                proactor,
                operation,
                result,
                succeed=succeed,
                next_link=next_link,
            )
            return
        sock = cast(socket.socket, operation.fileobj)

        def send_delivery(
            _proactor: _SendSubmitProactor,
            _send_operation: Operation[None],
            _result: object,
            send_exception: BaseException | None,
        ) -> None:
            if send_exception is not None:
                fail(send_exception)
                return
            _advance_chain(
                proactor,
                operation,
                None,
                succeed=succeed,
                next_link=next_link,
            )

        send_operation = proactor.send(sock, payload, delivery=send_delivery)
        operation.set_cancel_forward(send_operation)

    return delivery


def connect_initial_send_delivery(initial: SocketSendBuffer) -> DeliveryHandler:
    """Delivery handler for ``connect(..., initial=...)``.

    Composes ``chained_connect_link`` and ``chained_send_link``, completing
    with ``True`` when the chained sendall succeeds (including an empty
    ``initial`` buffer).
    """

    def delivery(
        proactor: _SendSubmitProactor,
        operation: Operation[bool],
        result: object,
        exception: BaseException | None,
    ) -> None:
        chained_connect_link(
            fail=operation.complete_error,
            next_link=chained_send_link(
                initial,
                succeed=lambda: operation.complete(True),
                fail=operation.complete_error,
            ),
        )(proactor, operation, result, exception)

    return delivery


def create_socket_delivery(
    connect_to: Any | None,
    initial_data: SocketSendBuffer | None,
    *,
    succeed: Callable[[socket.socket, bool, bool], None],
    fail: _DeliveryFail,
    on_socket: Callable[[socket.socket], None] | None = None,
) -> DeliveryHandler:
    """Assemble create → connect → send for ``create_socket``.

    Expects the proactor to ``deliver()`` a ``socket.socket`` into
    ``chained_fdclose_link``, then optionally chains ``chained_connect_link``
    and ``chained_send_link`` when ``connect_to`` is set on an inet socket.
    """

    def after_socket_created(
        proactor: _ConnectSubmitProactor,
        operation: Operation[CreateSocketResult],
        result: object,
        exception: BaseException | None,
    ) -> None:
        sock = cast(socket.socket, result)
        if connect_to is None or sock.family == socket.AF_UNIX:
            succeed(sock, False, False)
            return

        def connect_fail(exc: BaseException) -> None:
            _close_socket(sock)
            fail(exc)

        connect_delivery = chained_connect_link(
            fail=connect_fail,
            next_link=chained_send_link(
                initial_data,
                succeed=lambda: succeed(sock, True, initial_data is not None),
                fail=connect_fail,
            ),
        )
        connect_operation = proactor.connect(sock, connect_to, delivery=connect_delivery)
        operation.set_cancel_forward(connect_operation)

    return chained_fdclose_link(
        fail=fail,
        next_link=after_socket_created,
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
            # Inner recv legs need not call complete(); backends release the fd
            # or uring entry before deliver() runs on the worker thread.
            if operation.done():
                return
            if second_exception is not None:
                operation.complete_error(second_exception)
                return
            operation.complete(first + cast(bytes, second_result))

        second = proactor.recv(sock, size, delivery=second_delivery)
        operation.set_cancel_forward(second)

    return delivery
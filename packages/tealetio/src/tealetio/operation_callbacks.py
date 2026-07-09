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


def operation_factory(*, delivery: DeliveryHandler | None = None) -> OperationFactory:
    """Build an ``Operation`` with an optional delivery handler for proactor hooks."""

    def factory(kind: str, fileobj: object | None) -> Operation[Any]:
        operation = Operation(kind=kind, fileobj=fileobj)
        if delivery is not None:
            operation.set_delivery(delivery)
        return operation

    return factory


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
        except BaseException as exc:
            parent.complete_error(exc)
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
    cannot outrun ``attach_suboperation()``. ``spawn()`` runs while holding
    ``parent._lock``, which can defer another thread's ``cancel()`` until a
    synchronous backend path (for example ``AF_UNIX`` connect) returns from
    ``spawn()``; the lock is an ``RLock`` so same-thread spawn does not
    self-deadlock.

    Returns ``False`` only when the parent is already ``_done`` (attach uses the
    same check under the lock). Callers need not finish the parent on ``False``.
    If the child was spawned, a failed attach cancels it.

    If the child is already done when registered, ``add_done_callback`` runs
    ``on_complete`` immediately.
    """

    with parent._lock:
        if parent._done:
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

        try:
            # False means parent is already _done; no cleanup needed here.
            chain_suboperation(
                operation,
                lambda: proactor.send(sock, payload),
                on_send_complete,
            )
        except BaseException as exc:
            operation.complete_error(exc)

    return delivery


def _close_socket(sock: socket.socket) -> None:
    try:
        sock.close()
    except OSError:
        pass


def _close_wrapper_result(result: object) -> None:
    if not isinstance(result, tuple) or len(result) != 2:
        return
    _reader, writer = result
    close = getattr(writer, "close", None)
    if close is None:
        return
    try:
        close()
    except BaseException:
        pass


def _complete_connect_result(
    operation: Operation[Any],
    sock: socket.socket,
    result_wrapper: Callable[[socket.socket], T] | None,
) -> None:
    if result_wrapper is None:
        operation.complete(sock)
        return
    try:
        result = result_wrapper(sock)
    except BaseException as exc:
        _close_socket(sock)
        operation.complete_error(exc)
        return
    try:
        operation.complete(result)
    except BaseException as exc:
        _close_wrapper_result(result)
        operation.complete_error(exc)


def create_connect_delivery(
    proactor: Proactor,
    connect_to: Any,
    initial: SocketSendBuffer | None = None,
    result_wrapper: Callable[[socket.socket], T] | None = None,
) -> DeliveryHandler:
    """After create_socket succeeds, connect and optionally send ``initial`` bytes.

    When ``result_wrapper`` is set, it runs on the connected socket before the
    root operation completes (for example to build a stream pair).
    """

    payload = memoryview(initial) if initial is not None else None

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
            if payload is None or not payload:
                _complete_connect_result(operation, sock, result_wrapper)
                return

            def on_send_complete(send_op: Operation[Any]) -> None:
                send_exc = send_op.exception()
                if send_exc is not None:
                    _close_socket(sock)
                    operation.complete_error(send_exc)
                    return
                _complete_connect_result(operation, sock, result_wrapper)

            try:
                if not chain_suboperation(
                    operation,
                    lambda: proactor.send(sock, payload),
                    on_send_complete,
                ):
                    # Parent is already _done; close fd only.
                    _close_socket(sock)
            except BaseException as exc:
                _close_socket(sock)
                operation.complete_error(exc)

        try:
            if not chain_suboperation(
                operation,
                lambda: proactor.connect(sock, connect_to),
                on_connect_complete,
            ):
                # Parent is already _done; close fd only.
                _close_socket(sock)
        except BaseException as exc:
            _close_socket(sock)
            operation.complete_error(exc)

    return delivery


def create_connect_operation_factory(
    proactor: Proactor,
    connect_to: Any,
    initial: SocketSendBuffer | None = None,
    result_wrapper: Callable[[socket.socket], T] | None = None,
) -> OperationFactory:
    """Factory for ``proactor.create_socket`` when ``connect_to`` is set."""

    return operation_factory(delivery=create_connect_delivery(proactor, connect_to, initial, result_wrapper))


def connect_initial_send_operation_factory(
    proactor: Proactor,
    initial: SocketSendBuffer,
) -> OperationFactory:
    """Factory for ``proactor.connect`` when ``initial`` send composition is needed."""

    return operation_factory(delivery=connect_initial_send_delivery(proactor, initial))

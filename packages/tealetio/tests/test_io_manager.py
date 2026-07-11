from __future__ import annotations

import gc
import os
import socket
from typing import Any
from unittest.mock import patch

import pytest

from tealetio import set_scheduler
from tealetio.io_manager import FileIO, PollIO, ProactorIOManager, ServerIO, SocketIO
from tealetio.io_waiter import (
    IOWaiter,
    IOWaitGroup,
    IOWaitGroupChild,
    IOWaitGroupChildProtocol,
)
from tealetio.operations import ContinuousOperation, Operation
from tealetio.proactor import SyncProactorScheduler
from uring_fakes import SCHEDULER_INTEGRATION_FACTORIES


class _StubScheduler:
    """Minimal scheduler stand-in for direct ``ProactorIOManager`` unit tests."""

    def __init__(self) -> None:
        self._exception_handler: Any = None

    def set_exception_handler(self, handler: Any) -> None:
        self._exception_handler = handler

    def call_exception_handler(self, context: dict[str, Any]) -> None:
        handler = self._exception_handler
        if handler is None:
            raise context["exception"]
        handler(context)

    def call_soon_threadsafe(self, callback, *args: object, **kwargs: object) -> None:
        del kwargs
        try:
            callback(*args)
        except BaseException as exc:
            self.call_exception_handler(
                {
                    "message": "Exception in callback",
                    "exception": exc,
                    "scheduler": self,
                }
            )


def _manager(proactor: _MockProactor) -> ProactorIOManager:
    return ProactorIOManager(_StubScheduler(), proactor)  # type: ignore[arg-type]


class _MockProactor:
    def __init__(self) -> None:
        self.recv_calls: list[tuple[socket.socket, int]] = []
        self.poll_calls: list[tuple[int, int]] = []
        self.send_calls: list[tuple[socket.socket, Any]] = []
        self.create_socket_calls: list[tuple[Any, ...]] = []
        self.last_create_socket: socket.socket | None = None
        self.openat_calls: list[tuple[str, int, int]] = []
        self.close_fd_calls: list[int] = []

    def recv(self, sock: socket.socket, n: int) -> Operation[bytes]:
        self.recv_calls.append((sock, n))
        operation = Operation[bytes](kind="recv", fileobj=sock.fileno())
        operation._finish(result=b"mock")
        return operation

    def accept(self, sock: socket.socket) -> Operation[socket.socket]:
        del sock
        conn, _peer = socket.socketpair()
        conn.setblocking(False)
        os.set_inheritable(conn.fileno(), False)
        operation = Operation[socket.socket](kind="accept", fileobj=None)
        operation._finish(result=conn)
        return operation

    def poll(self, fd: int, mask: int) -> Operation[int]:
        self.poll_calls.append((fd, mask))
        operation = Operation[int](kind="poll", fileobj=fd)
        operation._finish(result=mask)
        return operation

    def openat(self, path: str, flags: int, mode: int) -> Operation[int]:
        self.openat_calls.append((path, flags, mode))
        operation = Operation[int](kind="openat", fileobj=-1)
        operation._finish(result=901)
        return operation

    def create_socket(
        self,
        family: int,
        type: int,
        proto: int = 0,
        *,
        flags: int = 0,
        operation_factory: Any | None = None,
    ) -> Operation[Any]:
        self.create_socket_calls.append((family, type, proto, flags, operation_factory is not None))
        sock = socket.socket(family, type, proto)
        sock.setblocking(False)
        os.set_inheritable(sock.fileno(), False)
        self.last_create_socket = sock
        if operation_factory is None:
            operation = Operation[socket.socket](kind="create_socket", fileobj=(family, type, proto))
            operation._finish(result=sock)
            return operation
        operation = operation_factory("create_socket", (family, type, proto))
        delivery = operation._delivery
        if delivery is not None:
            delivery(self, operation, sock, None)
        return operation

    def connect(
        self,
        sock: socket.socket,
        address: Any,
        *,
        operation_factory: Any | None = None,
    ) -> Operation[None]:
        del address
        if operation_factory is None:
            operation = Operation[None](kind="connect", fileobj=sock)
            operation._finish(result=None)
            return operation
        operation = operation_factory("connect", sock)
        delivery = operation._delivery
        if delivery is not None:
            delivery(self, operation, None, None)
        elif not operation.done():
            operation._finish(result=None)
        return operation

    def send(
        self,
        sock: socket.socket,
        data: Any,
        progress: Any = None,
        *,
        operation_factory: Any | None = None,
    ) -> Operation[None]:
        del progress
        self.send_calls.append((sock, data))
        if operation_factory is None:
            operation = Operation[None](kind="send", fileobj=sock)
            operation._finish(result=None)
            return operation
        operation = operation_factory("send", sock)
        delivery = operation._delivery
        if delivery is not None:
            delivery(self, operation, None, None)
        elif not operation.done():
            operation._finish(result=None)
        return operation

    def poll_many(
        self,
        fd: int,
        mask: int,
        callback: Any,
    ) -> ContinuousOperation[int]:
        operation = ContinuousOperation[int](kind="poll_many", fileobj=fd)
        operation._finish(result=mask)
        return operation

    def shutdown(self, sock: socket.socket, how: int) -> Operation[None]:
        operation = Operation[None](kind="shutdown", fileobj=sock)
        try:
            sock.shutdown(how)
            operation._finish(result=None)
        except OSError as exc:
            operation._finish(exception=exc)
        return operation

    def close_socket(self, sock: socket.socket) -> Operation[None]:
        operation = Operation[None](kind="close_socket", fileobj=sock)
        try:
            sock.close()
            operation._finish(result=None)
        except OSError as exc:
            operation._finish(exception=exc)
        return operation

    def close_fd(self, fd: int) -> Operation[None]:
        self.close_fd_calls.append(fd)
        operation = Operation[None](kind="close_fd", fileobj=fd)
        operation._finish(result=None)
        return operation


class TestProactorIOManager:
    def test_basic_scheduler_io_raises(self):
        from tealetio.scheduler import BasicScheduler

        scheduler = BasicScheduler()
        with pytest.raises(RuntimeError, match="scheduler with IO support"):
            scheduler.io

    def test_selector_scheduler_io_raises_targeted_message(self):
        from tealetio.selector import SyncSelectorScheduler

        scheduler = SyncSelectorScheduler()
        try:
            with pytest.raises(RuntimeError, match="stream helpers require a proactor scheduler"):
                scheduler.io
        finally:
            scheduler.close()


class TestAbortiveClose:
    def test_abortive_close_closes_fd(self) -> None:
        from tealetio.socket_helpers import abortive_close

        conn, peer = socket.socketpair()
        peer.close()
        abortive_close(conn)
        assert conn.fileno() == -1
        abortive_close(conn)


class TestProactorIOManagerAcceptMany:
    @pytest.mark.parametrize("recv_size", [0, -1])
    def test_accept_many_rejects_invalid_recv_size(self, recv_size: int) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        server = socket.socket()
        try:
            server.setblocking(False)
            with pytest.raises(ValueError):
                io.accept_many(server, lambda _: None, recv_size=recv_size)
        finally:
            server.close()

    def test_accept_many_uses_callback_factory_only_when_recv_size_set(self) -> None:
        class _CaptureProactor(_MockProactor):
            def accept_many(self, sock: socket.socket, callback=None, *, callback_factory=None):
                self.last_callback = callback
                self.last_callback_factory = callback_factory
                return ContinuousOperation(kind="accept_many", fileobj=sock)

        proactor = _CaptureProactor()
        io = _manager(proactor)
        server = socket.socket()
        try:
            io.accept_many(server, lambda _: None)
            assert proactor.last_callback is not None
            assert proactor.last_callback_factory is None
            io.accept_many(server, lambda _: None, recv_size=64)
            assert proactor.last_callback_factory is not None
        finally:
            server.close()

    def test_accept_many_caps_oversized_recv_size(self) -> None:
        from tealetio.continuous_callbacks import normalize_accept_recv_size

        assert normalize_accept_recv_size(2**16 + 1) == 2**16

    def test_accept_many_on_recv_error_closes_after_callback(self, monkeypatch) -> None:
        captured_errors: list[tuple[socket.socket, BaseException]] = []

        class _EagerAcceptProactor(_MockProactor):
            def accept_many(self, sock: socket.socket, callback=None, *, callback_factory=None):
                if callback_factory is not None:
                    on_conn = callback_factory(
                        ContinuousOperation(kind="accept_many", fileobj=sock)
                    )
                    conn, peer = socket.socketpair()
                    peer.close()
                    on_conn(conn)
                return ContinuousOperation(kind="accept_many", fileobj=sock)

        def immediate_recv_error(proactor, parent, deliver, *, recv_size: int):
            del proactor, parent, recv_size

            def on_conn(conn: socket.socket) -> None:
                deliver((conn, None, OSError("recv failed")))

            return on_conn

        monkeypatch.setattr(
            "tealetio.io_manager.accept_read_delivery",
            immediate_recv_error,
        )

        proactor = _EagerAcceptProactor()
        io = _manager(proactor)
        server = socket.socket()
        try:
            io.accept_many(
                server,
                lambda _: (_ for _ in ()).throw(AssertionError("accept callback")),
                recv_size=64,
                on_recv_error=lambda conn, exc: captured_errors.append((conn, exc)),
            )
            assert len(captured_errors) == 1
            conn, exc = captured_errors[0]
            assert str(exc) == "recv failed"
            assert conn.fileno() == -1
        finally:
            server.close()

    def test_accept_many_recv_error_without_hook_closes_silently(self, monkeypatch) -> None:
        captured: list[tuple[socket.socket, bytes | None]] = []

        class _EagerAcceptProactor(_MockProactor):
            def accept_many(self, sock: socket.socket, callback=None, *, callback_factory=None):
                if callback_factory is not None:
                    on_conn = callback_factory(
                        ContinuousOperation(kind="accept_many", fileobj=sock)
                    )
                    conn, peer = socket.socketpair()
                    peer.close()
                    on_conn(conn)
                    captured.append((conn, None))
                return ContinuousOperation(kind="accept_many", fileobj=sock)

        def immediate_recv_error(proactor, parent, deliver, *, recv_size: int):
            del proactor, parent, recv_size

            def on_conn(conn: socket.socket) -> None:
                deliver((conn, None, OSError("recv failed")))

            return on_conn

        monkeypatch.setattr(
            "tealetio.io_manager.accept_read_delivery",
            immediate_recv_error,
        )

        proactor = _EagerAcceptProactor()
        io = _manager(proactor)
        server = socket.socket()
        try:
            io.accept_many(
                server,
                lambda delivery: (_ for _ in ()).throw(AssertionError("accept callback")),
                recv_size=64,
            )
            conn, _initial = captured[0]
            assert conn.fileno() == -1
        finally:
            server.close()

    def test_accept_many_reports_callback_exception(self) -> None:
        handler_errors: list[BaseException] = []

        class _EagerAcceptProactor(_MockProactor):
            def accept_many(self, sock: socket.socket, callback=None, *, callback_factory=None):
                if callback is not None:
                    conn, peer = socket.socketpair()
                    peer.close()
                    callback(conn)
                return ContinuousOperation(kind="accept_many", fileobj=sock)

        scheduler = _StubScheduler()
        scheduler.set_exception_handler(lambda context: handler_errors.append(context["exception"]))
        io = ProactorIOManager(scheduler, _EagerAcceptProactor())  # type: ignore[arg-type]
        server = socket.socket()
        try:
            io.accept_many(server, lambda _: (_ for _ in ()).throw(ValueError("accept failed")))
            assert len(handler_errors) == 1
            assert str(handler_errors[0]) == "accept failed"
        finally:
            server.close()

    def test_accept_many_streams_reports_callback_exception(self) -> None:
        handler_errors: list[BaseException] = []

        class _EagerAcceptProactor(_MockProactor):
            def accept_many(self, sock: socket.socket, callback=None, *, callback_factory=None):
                if callback is not None:
                    conn, peer = socket.socketpair()
                    peer.close()
                    callback(conn)
                return ContinuousOperation(kind="accept_many", fileobj=sock)

        scheduler = _StubScheduler()
        scheduler.set_exception_handler(lambda context: handler_errors.append(context["exception"]))
        io = ProactorIOManager(scheduler, _EagerAcceptProactor())  # type: ignore[arg-type]
        server = socket.socket()
        try:
            io.accept_many_streams(
                server,
                lambda _: (_ for _ in ()).throw(ValueError("streams failed")),
            )
            assert len(handler_errors) == 1
            assert str(handler_errors[0]) == "streams failed"
        finally:
            server.close()

    def test_accept_many_reports_on_recv_error_hook_exception(self, monkeypatch) -> None:
        handler_errors: list[BaseException] = []

        class _EagerAcceptProactor(_MockProactor):
            def accept_many(self, sock: socket.socket, callback=None, *, callback_factory=None):
                if callback_factory is not None:
                    on_conn = callback_factory(
                        ContinuousOperation(kind="accept_many", fileobj=sock)
                    )
                    conn, peer = socket.socketpair()
                    peer.close()
                    on_conn(conn)
                return ContinuousOperation(kind="accept_many", fileobj=sock)

        def immediate_recv_error(proactor, parent, deliver, *, recv_size: int):
            del proactor, parent, recv_size

            def on_conn(conn: socket.socket) -> None:
                deliver((conn, None, OSError("recv failed")))

            return on_conn

        monkeypatch.setattr(
            "tealetio.io_manager.accept_read_delivery",
            immediate_recv_error,
        )

        scheduler = _StubScheduler()
        scheduler.set_exception_handler(lambda context: handler_errors.append(context["exception"]))
        io = ProactorIOManager(scheduler, _EagerAcceptProactor())  # type: ignore[arg-type]
        server = socket.socket()
        try:
            io.accept_many(
                server,
                lambda _: None,
                recv_size=64,
                on_recv_error=lambda _conn, _exc: (_ for _ in ()).throw(RuntimeError("hook failed")),
            )
            assert len(handler_errors) == 1
            assert str(handler_errors[0]) == "hook failed"
        finally:
            server.close()

    def test_accept_many_streams_uses_bare_socket_callback(self) -> None:
        class _CaptureProactor(_MockProactor):
            def accept_many(self, sock: socket.socket, callback=None, *, callback_factory=None):
                self.last_callback = callback
                self.last_callback_factory = callback_factory
                return ContinuousOperation(kind="accept_many", fileobj=sock)

        proactor = _CaptureProactor()
        io = _manager(proactor)
        server = socket.socket()
        try:
            io.accept_many_streams(server, lambda _: None)
            assert proactor.last_callback is not None
            assert proactor.last_callback_factory is None
        finally:
            server.close()

    def test_accept_many_streams_uses_read_delivery_when_recv_size_set(self) -> None:
        class _CaptureProactor(_MockProactor):
            def accept_many(self, sock: socket.socket, callback=None, *, callback_factory=None):
                self.last_callback = callback
                self.last_callback_factory = callback_factory
                return ContinuousOperation(kind="accept_many", fileobj=sock)

        proactor = _CaptureProactor()
        io = _manager(proactor)
        server = socket.socket()
        try:
            io.accept_many_streams(server, lambda _: None, recv_size=64)
            assert proactor.last_callback_factory is not None
        finally:
            server.close()

    def test_accept_many_streams_on_recv_error_closes_after_callback(self, monkeypatch) -> None:
        captured_errors: list[tuple[socket.socket, BaseException]] = []

        class _EagerAcceptProactor(_MockProactor):
            def accept_many(self, sock: socket.socket, callback=None, *, callback_factory=None):
                if callback_factory is not None:
                    on_conn = callback_factory(
                        ContinuousOperation(kind="accept_many", fileobj=sock)
                    )
                    conn, peer = socket.socketpair()
                    peer.close()
                    on_conn(conn)
                return ContinuousOperation(kind="accept_many", fileobj=sock)

        def immediate_recv_error(proactor, parent, deliver, *, recv_size: int):
            del proactor, parent, recv_size

            def on_conn(conn: socket.socket) -> None:
                deliver((conn, None, OSError("recv failed")))

            return on_conn

        monkeypatch.setattr(
            "tealetio.io_manager.accept_read_delivery",
            immediate_recv_error,
        )

        proactor = _EagerAcceptProactor()
        io = _manager(proactor)
        server = socket.socket()
        try:
            io.accept_many_streams(
                server,
                lambda _: (_ for _ in ()).throw(AssertionError("streams callback")),
                recv_size=64,
                on_recv_error=lambda conn, exc: captured_errors.append((conn, exc)),
            )
            assert len(captured_errors) == 1
            conn, exc = captured_errors[0]
            assert str(exc) == "recv failed"
            assert conn.fileno() == -1
        finally:
            server.close()

    def test_accept_many_streams_recv_error_without_hook_closes_silently(self, monkeypatch) -> None:
        captured_errors: list[tuple[socket.socket, BaseException]] = []

        class _EagerAcceptProactor(_MockProactor):
            def accept_many(self, sock: socket.socket, callback=None, *, callback_factory=None):
                if callback_factory is not None:
                    on_conn = callback_factory(
                        ContinuousOperation(kind="accept_many", fileobj=sock)
                    )
                    conn, peer = socket.socketpair()
                    peer.close()
                    on_conn(conn)
                    captured_errors.append((conn, OSError("placeholder")))
                return ContinuousOperation(kind="accept_many", fileobj=sock)

        def immediate_recv_error(proactor, parent, deliver, *, recv_size: int):
            del proactor, parent, recv_size

            def on_conn(conn: socket.socket) -> None:
                deliver((conn, None, OSError("recv failed")))

            return on_conn

        monkeypatch.setattr(
            "tealetio.io_manager.accept_read_delivery",
            immediate_recv_error,
        )

        proactor = _EagerAcceptProactor()
        io = _manager(proactor)
        server = socket.socket()
        try:
            io.accept_many_streams(server, lambda _: None, recv_size=64)
            conn, _exc = captured_errors[0]
            assert conn.fileno() == -1
        finally:
            server.close()


class TestProactorIOManagerSockCreateStreams:
    def test_sock_create_streams_composes_create_connect_and_send(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        address = ("127.0.0.1", 9)
        waiter = io.sock_create_streams(
            socket.AF_INET,
            socket.SOCK_STREAM,
            connect_to=address,
            initial_data=b"hi",
        )
        assert isinstance(waiter, IOWaitGroup)
        reader, writer = waiter.wait()
        try:
            assert proactor.create_socket_calls == [(socket.AF_INET, socket.SOCK_STREAM, 0, 0, False)]
            assert len(proactor.send_calls) == 1
        finally:
            writer.close()

    def test_sock_create_streams_uses_io_wait_group(self) -> None:
        from tealetio.streams import StreamReader, StreamWriter

        proactor = _MockProactor()
        io = _manager(proactor)
        waiter = io.sock_create_streams(
            socket.AF_INET,
            socket.SOCK_STREAM,
            connect_to=("127.0.0.1", 9),
        )
        assert isinstance(waiter, IOWaitGroup)
        writer = None
        try:
            reader, writer = waiter.wait()
            assert isinstance(reader, StreamReader)
            assert isinstance(writer, StreamWriter)
        finally:
            if writer is not None:
                writer.close()

    def test_sock_create_streams_closes_socket_when_stream_factory_raises(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)

        def boom(_io: Any, _sock: socket.socket, **kwargs: Any) -> tuple[Any, Any]:
            raise ValueError("stream failed")

        waiter = io.sock_create_streams(
            socket.AF_INET,
            socket.SOCK_STREAM,
            connect_to=("127.0.0.1", 9),
            stream_factory=boom,
        )
        with pytest.raises(ValueError, match="stream failed"):
            waiter.wait()
        assert proactor.last_create_socket is not None
        assert proactor.last_create_socket.fileno() == -1


class TestProactorIOManagerDirect:
    def test_io_waiter_wait_returns_immediate_result(self):
        proactor = _MockProactor()
        io = _manager(proactor)
        sock = socket.socketpair()[0]
        try:
            operation = proactor.recv(sock, 4)
            assert IOWaiter(io, operation).wait() == b"mock"
        finally:
            sock.close()

    def test_io_waiter_forget_drops_interest_without_cancelling(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        operation = Operation[None](kind="test", fileobj=None)
        seen: list[int] = []
        operation.add_done_callback(lambda _op: seen.append(1))
        waiter = IOWaiter(io, operation)
        waiter.forget()
        assert not operation.cancelled()
        operation._finish(result=None)
        assert seen == [1]
        with pytest.raises(AssertionError):
            waiter.wait()
        waiter.forget()

    def test_io_waiter_forget_allows_backend_completion(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        conn, peer = socket.socketpair()
        peer.close()
        try:
            close_waiter = io.sock_close(conn)
            close_waiter.forget()
            assert conn.fileno() == -1
        finally:
            if conn.fileno() != -1:
                conn.close()

    def test_sock_shutdown_and_close_delegate_to_proactor(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        conn, peer = socket.socketpair()
        peer.close()
        try:
            io.sock_shutdown(conn, socket.SHUT_WR).wait()
            close_waiter = io.sock_close(conn)
            assert close_waiter._operation.kind == "close_socket"
            close_waiter.wait()
            assert conn.fileno() == -1
        finally:
            if conn.fileno() != -1:
                conn.close()

    def test_sock_recv_delegates_to_proactor(self):
        proactor = _MockProactor()
        io = _manager(proactor)
        sock = socket.socketpair()[0]
        try:
            assert io.sock_recv(sock, 4).wait() == b"mock"
            assert proactor.recv_calls == [(sock, 4)]
        finally:
            sock.close()

    def test_sock_sendall_delegates_to_proactor(self):
        proactor = _MockProactor()
        io = _manager(proactor)
        sock = socket.socketpair()[0]
        try:
            io.sock_sendall(sock, b"hello").wait()
            assert proactor.send_calls == [(sock, b"hello")]
        finally:
            sock.close()

    def test_poll_delegates_to_proactor(self):
        proactor = _MockProactor()
        io = _manager(proactor)
        assert io.poll(7, 3).wait() == 3
        assert proactor.poll_calls == [(7, 3)]

    def test_sock_send_iter_drains_chunks(self):
        proactor = _MockProactor()
        io = _manager(proactor)
        sock = socket.socketpair()[0]
        try:
            io.sock_send_iter(sock, [b"ab", b"", memoryview(b"cd")])
            assert len(proactor.send_calls) == 2
            assert bytes(proactor.send_calls[0][1]) == b"ab"
            assert bytes(proactor.send_calls[1][1]) == b"cd"
        finally:
            sock.close()

    def test_sock_create_applies_scheduler_socket_contract(self):
        proactor = _MockProactor()
        io = _manager(proactor)
        sock = io.sock_create(socket.AF_INET, socket.SOCK_STREAM).wait()
        try:
            import fcntl

            flags = fcntl.fcntl(sock.fileno(), fcntl.F_GETFL)
            assert flags & os.O_NONBLOCK
            assert not os.get_inheritable(sock.fileno())
        finally:
            sock.close()

    def test_sock_create_composes_connect_without_operation_factory(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        address = ("127.0.0.1", 9)
        waiter = io.sock_create(
            socket.AF_INET,
            socket.SOCK_STREAM,
            connect_to=address,
        )
        assert isinstance(waiter, IOWaitGroup)
        sock = waiter.wait()
        try:
            assert proactor.create_socket_calls == [(socket.AF_INET, socket.SOCK_STREAM, 0, 0, False)]
        finally:
            sock.close()

    def test_sock_create_composes_connect_and_send_without_operation_factory(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        address = ("127.0.0.1", 9)
        waiter = io.sock_create(
            socket.AF_INET,
            socket.SOCK_STREAM,
            connect_to=address,
            initial_data=b"hi",
        )
        assert isinstance(waiter, IOWaitGroup)
        sock = waiter.wait()
        try:
            assert proactor.create_socket_calls == [(socket.AF_INET, socket.SOCK_STREAM, 0, 0, False)]
            assert len(proactor.send_calls) == 1
            assert proactor.send_calls[0][0] is sock
        finally:
            sock.close()

    def test_sock_connect_without_initial_returns_io_waiter(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        sock = socket.socketpair()[0]
        try:
            waiter = io.sock_connect(sock, ("127.0.0.1", 9))
            assert isinstance(waiter, IOWaiter)
            waiter.wait()
            assert proactor.send_calls == []
        finally:
            sock.close()

    def test_sock_connect_composes_send_after_connect(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        sock = socket.socketpair()[0]
        try:
            waiter = io.sock_connect(sock, ("127.0.0.1", 9), initial=b"hi")
            assert isinstance(waiter, IOWaitGroup)
            waiter.wait()
            assert len(proactor.send_calls) == 1
            assert bytes(proactor.send_calls[0][1]) == b"hi"
        finally:
            sock.close()

    def test_sock_accept_without_recv_returns_io_waiter(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        listen = socket.socketpair()[0]
        try:
            waiter = io.sock_accept(listen)
            assert isinstance(waiter, IOWaiter)
            conn, initial = waiter.wait()
            try:
                assert initial is None
                assert proactor.recv_calls == []
            finally:
                conn.close()
        finally:
            listen.close()

    def test_sock_accept_composes_recv_after_accept(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        listen = socket.socketpair()[0]
        try:
            waiter = io.sock_accept(listen, 64)
            assert isinstance(waiter, IOWaitGroup)
            conn, data = waiter.wait()
            try:
                assert data == b"mock"
                assert proactor.recv_calls == [(conn, 64)]
            finally:
                conn.close()
        finally:
            listen.close()

    def test_sock_create_rejects_initial_data_without_connect_to(self):
        proactor = _MockProactor()
        io = _manager(proactor)
        with pytest.raises(ValueError, match="initial_data requires connect_to"):
            io.sock_create(
                socket.AF_INET,
                socket.SOCK_STREAM,
                initial_data=b"hi",
            )

    def test_sock_create_closes_socket_when_connect_fails(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)

        def failing_connect(
            sock: socket.socket,
            address: Any,
            *,
            operation_factory: Any | None = None,
        ) -> Operation[None]:
            del address, operation_factory
            operation = Operation[None](kind="connect", fileobj=sock)
            operation._finish(exception=OSError("connect failed"))
            return operation

        proactor.connect = failing_connect  # type: ignore[method-assign]
        waiter = io.sock_create(
            socket.AF_INET,
            socket.SOCK_STREAM,
            connect_to=("127.0.0.1", 9),
        )
        assert isinstance(waiter, IOWaitGroup)
        with pytest.raises(OSError, match="connect failed"):
            waiter.wait()
        assert proactor.last_create_socket is not None
        assert proactor.last_create_socket.fileno() == -1

    def test_open_returns_proactor_file(self):
        proactor = _MockProactor()
        io = _manager(proactor)
        handle = io.open("/tmp/example.txt", "rb").wait()
        try:
            from tealetio.files import ProactorFile

            assert isinstance(handle, ProactorFile)
            assert hasattr(handle, "read")
            assert hasattr(handle, "seek")
            assert handle.name == "/tmp/example.txt"
            assert proactor.openat_calls == [("/tmp/example.txt", os.O_RDONLY | os.O_CLOEXEC, 0o666)]
        finally:
            handle.close()
        assert proactor.close_fd_calls == [901]

    def test_poll_many_returns_continuous_operation(self):
        proactor = _MockProactor()
        io = _manager(proactor)
        seen: list[int] = []

        operation = io.poll_many(5, 1, seen.append)
        assert isinstance(operation, ContinuousOperation)


class TestIOWaitGroup:
    def test_group_wait_uses_single_threadsafe_event_for_multi_leg_compose(self) -> None:
        import tealetio.io_waiter as io_waiter_module

        proactor = _MockProactor()
        io = _manager(proactor)
        event_count = 0
        original_event = io_waiter_module.ThreadsafeEvent
        pending_connect: list[Operation[None]] = []

        class TrackingEvent(original_event):
            def __init__(self, scheduler: Any) -> None:
                nonlocal event_count
                event_count += 1
                super().__init__(scheduler)

            def swait(self) -> None:
                connect = pending_connect[0]
                if not connect.done():
                    connect._finish(result=None)
                super().swait()

        io_waiter_module.ThreadsafeEvent = TrackingEvent  # type: ignore[misc]
        try:
            create = Operation[socket.socket](kind="create", fileobj=None)
            connect = Operation[None](kind="connect", fileobj=None)
            pending_connect.append(connect)

            group = IOWaitGroup[socket.socket](io)

            def advance_create(child: IOWaitGroupChildProtocol[socket.socket]) -> None:
                sock = child.value()
                group.attach(
                    connect,
                    advance=lambda _connect_child: group.finish(sock),
                )

            group.attach(create, advance=advance_create)
            create._finish(result=proactor.last_create_socket)
            assert group._completion is None
            assert group.wait() is proactor.last_create_socket
            assert event_count == 1
        finally:
            io_waiter_module.ThreadsafeEvent = original_event

    def test_group_child_value_is_one_shot(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        operation = Operation[int](kind="test", fileobj=None)
        group = IOWaitGroup[int](io)
        child = group.attach(operation, advance=lambda _leg: None)
        operation._finish(result=7)
        assert child.value() == 7
        with pytest.raises(AssertionError):
            child.value()

    def test_group_child_on_cleanup_runs_when_value_not_consumed(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        operation = Operation[int](kind="test", fileobj=None)
        seen: list[tuple[bool, int | None]] = []
        group = IOWaitGroup[int](io)
        group.attach(
            operation,
            on_cleanup=lambda fail, value: seen.append((fail, value)),
            advance=lambda _child: group.finish(0),
        )
        operation._finish(result=9)
        del group
        gc.collect()
        assert seen == [(False, 9)]

    def test_group_child_on_cleanup_runs_on_operation_error(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        operation = Operation[None](kind="connect", fileobj=None)
        seen: list[tuple[bool, Any]] = []
        group = IOWaitGroup[None](io)
        group.attach(operation, on_cleanup=lambda fail, value: seen.append((fail, value)))
        operation._finish(exception=OSError("connect failed"))
        with pytest.raises(OSError, match="connect failed"):
            group.wait()
        assert seen == [(True, None)]

    def test_group_attach_operation_completes_group(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        first = Operation[None](kind="first", fileobj=None)
        second = Operation[None](kind="second", fileobj=None)
        group = IOWaitGroup[str](io)

        def advance_first(_child: IOWaitGroupChildProtocol[None]) -> None:
            group.attach_operation(second, on_complete=lambda: group.finish("done"))

        group.attach(first, advance=advance_first)
        first._finish(result=None)
        second._finish(result=None)
        assert group.wait() == "done"

    def test_group_wait_cancels_active_operations_on_exception(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        operation = Operation[None](kind="pending", fileobj=None)
        group = IOWaitGroup[None](io)
        group.attach(operation)

        import tealetio.io_waiter as io_waiter_module

        original_swait = io_waiter_module.ThreadsafeEvent.swait

        def swait_and_abort(self: Any) -> None:
            raise TimeoutError()

        io_waiter_module.ThreadsafeEvent.swait = swait_and_abort  # type: ignore[method-assign]
        try:
            with pytest.raises(TimeoutError):
                group.wait()
            assert operation.cancelled()
        finally:
            io_waiter_module.ThreadsafeEvent.swait = original_swait


@pytest.mark.parametrize("scheduler_factory", SCHEDULER_INTEGRATION_FACTORIES)
class TestProactorIOManagerIntegration:
    @pytest.fixture
    def scheduler(self, scheduler_factory) -> SyncProactorScheduler:
        sched = scheduler_factory()
        set_scheduler(sched)
        yield sched
        sched.close()

    def test_scheduler_exposes_io_facade(self, scheduler: SyncProactorScheduler) -> None:
        io = scheduler.io
        assert isinstance(io, ProactorIOManager)
        assert isinstance(io, SocketIO)
        assert isinstance(io, PollIO)
        assert isinstance(io, FileIO)
        _: ServerIO = io
        assert io.proactor is scheduler.proactor

    def test_scheduler_io_forwards_sock_recv(self, scheduler: SyncProactorScheduler) -> None:
        client, server = socket.socketpair()
        try:
            client.setblocking(False)
            server.setblocking(False)
            client.sendall(b"ping")

            def exercise() -> bytes:
                return scheduler.io.sock_recv(server, 4).wait()

            assert scheduler.run_until_complete(scheduler.spawn(exercise)) == b"ping"

            def read_zero() -> bytes:
                return scheduler.io.sock_recv(server, 0).wait()

            assert scheduler.run_until_complete(scheduler.spawn(read_zero)) == b""
        finally:
            client.close()
            server.close()

    def test_io_waiter_blocks_until_completion(self, scheduler: SyncProactorScheduler) -> None:
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            writer.sendall(b"z")

            def exercise() -> bytes:
                return scheduler.io.sock_recv(reader, 1).wait()

            assert scheduler.run_until_complete(scheduler.spawn(exercise)) == b"z"
        finally:
            reader.close()
            writer.close()

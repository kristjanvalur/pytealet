from __future__ import annotations

import gc
import os
import select
import socket
from typing import Any
from unittest.mock import patch

import pytest

from tealetio import set_scheduler
from tealetio.io_manager import (
    ProactorIOManager,
    ServerIO,
    _finish_or_close_socket,
)
from tealetio.io_waiter import (
    IOWaiter,
    IOWaiterSync,
    IOWaitGroup,
    IOWaitGroupChild,
    IOWaitGroupChildProtocol,
)
from tealetio.operations import (
    ContinuousOperation,
    InvalidStateError,
    MultishotDelivery,
    Operation,
    io_cancellation_error,
    is_io_cancellation,
)
from tealetio.proactor import SyncProactorScheduler, UringProactor
from io_fakes import StubScheduler
from uring_fakes import (
    SCHEDULER_INTEGRATION_FACTORIES,
    _DeferredCreateSocketUringRing,
    _ensure_deferred_connect_completed,
    _patch_uring_capabilities,
    _wait_for_uring,
)


def _manager(proactor: _MockProactor) -> ProactorIOManager:
    return ProactorIOManager(StubScheduler(), proactor)  # type: ignore[arg-type]


def _eager_accept_conn() -> socket.socket:
    conn, peer = socket.socketpair()
    peer.close()
    conn.setblocking(False)
    return conn


def _eager_accept_arm(
    sock: socket.socket,
    callback: Any,
    conn: socket.socket | None = None,
    *,
    more: bool = True,
) -> ContinuousOperation[Any]:
    operation = ContinuousOperation(kind="accept_many", fileobj=sock, result_callback=callback)
    operation._emit_result(conn if conn is not None else _eager_accept_conn(), more=more)
    return operation


class _MockProactor:
    def recycle_operation(self, operation: object) -> None:
        return

    def __init__(self, *, recv_result: bytes = b"mock") -> None:
        self._recv_result = recv_result
        self.recv_calls: list[tuple[socket.socket, int]] = []
        self.recv_many_calls: list[socket.socket] = []
        self.poll_calls: list[tuple[int, int]] = []
        self.send_calls: list[tuple[socket.socket, Any]] = []
        self.create_socket_calls: list[tuple[Any, ...]] = []
        self.last_create_socket: socket.socket | None = None
        self.connect_calls: list[tuple[socket.socket, Any]] = []
        self.last_connect_socket: socket.socket | None = None
        self.openat_calls: list[tuple[str, int, int]] = []
        self.close_fd_calls: list[int] = []

    def recv(self, sock: socket.socket, n: int) -> Operation[bytes]:
        self.recv_calls.append((sock, n))
        operation = Operation[bytes](kind="recv", fileobj=sock.fileno())
        operation._finish(result=self._recv_result)
        return operation

    def recv_many(self, sock, callback, *, buf_group, base_sequence=0):
        del callback, buf_group, base_sequence
        self.recv_many_calls.append(sock)
        return ContinuousOperation(kind="recv_many", fileobj=sock)

    def create_recv_buffer_pool(self, buffer_size: int, buffer_count: int):
        from tealetio.proactor import SyntheticRecvBufferPool

        return SyntheticRecvBufferPool(buffer_size, buffer_count)

    def shared_recv_buffer_pool(self):
        return self.create_recv_buffer_pool(8192, 4)

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
    ) -> Operation[Any]:
        self.create_socket_calls.append((family, type, proto, flags))
        sock = socket.socket(family, type, proto)
        sock.setblocking(False)
        os.set_inheritable(sock.fileno(), False)
        self.last_create_socket = sock
        operation = Operation[socket.socket](kind="create_socket", fileobj=(family, type, proto))
        operation._finish(result=sock)
        return operation

    def connect(
        self,
        sock: socket.socket,
        address: Any,
    ) -> Operation[None]:
        self.connect_calls.append((sock, address))
        self.last_connect_socket = sock
        operation = Operation[None](kind="connect", fileobj=sock)
        operation._finish(result=None)
        return operation

    def send(
        self,
        sock: socket.socket,
        data: Any,
        progress: Any = None,
    ) -> Operation[None]:
        del progress
        self.send_calls.append((sock, data))
        operation = Operation[None](kind="send", fileobj=sock)
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

    def cancel(self, operation: Operation[Any]) -> Operation[None]:
        if not operation.done():
            operation._finish(exception=io_cancellation_error())
        teardown = Operation[None](kind="cancel", fileobj=operation)
        teardown._finish(result=None)
        return teardown


class TestProactorIOManager:
    def test_basic_scheduler_io_raises(self):
        from tealetio.scheduler import BasicScheduler

        scheduler = BasicScheduler()
        with pytest.raises(RuntimeError, match="scheduler with IO support"):
            scheduler.io

class TestAbortiveClose:
    def test_abortive_close_closes_fd(self) -> None:
        from tealetio.socket_helpers import abortive_close

        conn, peer = socket.socketpair()
        peer.close()
        abortive_close(conn)
        assert conn.fileno() == -1
        abortive_close(conn)


class TestProactorIOManagerCancelOperation:
    def test_cancel_operation_returns_waiter_for_teardown(self) -> None:
        target = Operation[bytes](kind="recv")

        io = _manager(_MockProactor())
        waiter = io._cancel_operation(target)

        assert target.cancelled()
        assert waiter.operation is not None
        assert waiter.operation.kind == "cancel"
        assert waiter.poll() is True
        assert waiter.wait() is None

    def test_cancel_operation_returns_completed_waitable_when_target_already_done(self) -> None:
        target = Operation[bytes](kind="recv")
        target._finish(result=b"done")

        io = _manager(_MockProactor())
        waiter = io._cancel_operation(target)

        assert waiter.poll() is True
        assert waiter.wait() is None


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

    def test_accept_many_wires_plain_callback_with_recv_size(self) -> None:
        class _CaptureProactor(_MockProactor):
            def accept_many(self, sock: socket.socket, callback=None):
                self.last_callback = callback
                return ContinuousOperation(kind="accept_many", fileobj=sock)

        proactor = _CaptureProactor()
        io = _manager(proactor)
        server = socket.socket()
        try:
            io.accept_many(server, lambda _: None)
            assert proactor.last_callback is not None
            io.accept_many(server, lambda _: None, recv_size=64)
            assert proactor.last_callback is not None
        finally:
            server.close()

    def test_accept_many_recv_size_submits_recv_from_io_manager_callback(self) -> None:
        class _EagerAcceptProactor(_MockProactor):
            def accept_many(self, sock: socket.socket, callback=None):
                conn, peer = socket.socketpair()
                peer.close()
                return _eager_accept_arm(sock, callback, conn)

        delivered: list[tuple[socket.socket, bytes | None]] = []
        proactor = _EagerAcceptProactor(recv_result=b"peek")
        io = _manager(proactor)
        server = socket.socket()
        try:
            io.accept_many(
                server,
                lambda delivery: delivered.append(delivery),
                recv_size=8,
            )
            assert proactor.recv_calls == [(delivered[0][0], 8)]
            assert delivered == [(delivered[0][0], b"peek")]
        finally:
            for conn, _data in delivered:
                conn.close()
            server.close()

    def test_accept_many_caps_oversized_recv_size(self) -> None:
        from tealetio.continuous_callbacks import normalize_accept_recv_size

        assert normalize_accept_recv_size(2**16 + 1) == 2**16

    def test_wrap_accept_delivery_swallows_cancellation_terminal(self) -> None:
        from tealetio.continuous_callbacks import wrap_accept_delivery
        from tealetio.operations import MultishotDelivery
        seen: list[object] = []
        wrapped = wrap_accept_delivery(lambda item: seen.append(item))
        wrapped(MultishotDelivery(exception=io_cancellation_error()))
        assert seen == []

    @pytest.mark.parametrize("recv_timeout", [0, -1])
    def test_accept_many_rejects_invalid_recv_timeout(self, recv_timeout: float) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        server = socket.socket()
        try:
            with pytest.raises(ValueError):
                io.accept_many(
                    server,
                    lambda _: None,
                    recv_size=64,
                    recv_timeout=recv_timeout,
                )
        finally:
            server.close()

    def test_accept_many_recv_timeout_requires_recv_size(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        server = socket.socket()
        try:
            with pytest.raises(ValueError, match="recv_timeout requires recv_size"):
                io.accept_many(server, lambda _: None, recv_timeout=1.0)
        finally:
            server.close()

    def test_accept_many_recv_timeout_posts_cancelled_error_to_scheduler(self) -> None:
        class _PendingRecvProactor(_MockProactor):
            def __init__(self) -> None:
                super().__init__()
                self.pending_recvs: list[Operation[bytes]] = []

            def recv(self, sock: socket.socket, n: int) -> Operation[bytes]:
                self.recv_calls.append((sock, n))
                operation = Operation[bytes](kind="recv", fileobj=sock.fileno())
                self.pending_recvs.append(operation)
                return operation

        class _EagerAcceptProactor(_PendingRecvProactor):
            def accept_many(self, sock: socket.socket, callback=None):
                conn, peer = socket.socketpair()
                peer.close()
                return _eager_accept_arm(sock, callback, conn)

        recv_errors: list[tuple[socket.socket, BaseException]] = []
        proactor = _EagerAcceptProactor()
        scheduler = StubScheduler()
        io = ProactorIOManager(scheduler, proactor)  # type: ignore[arg-type]
        server = socket.socket()
        try:
            io.accept_many(
                server,
                lambda _: (_ for _ in ()).throw(AssertionError("accept callback")),
                recv_size=8,
                recv_timeout=0.5,
                on_recv_error=lambda conn, exc: recv_errors.append((conn, exc)),
            )
            recv_op = proactor.pending_recvs[0]
            scheduler.fire_timers()
            assert recv_op.cancelled()
            assert len(recv_errors) == 1
            assert is_io_cancellation(recv_errors[0][1])
            assert recv_errors[0][0].fileno() == -1
        finally:
            server.close()

    def test_accept_many_recv_timeout_cancels_pending_recv(self) -> None:
        class _PendingRecvProactor(_MockProactor):
            def __init__(self) -> None:
                super().__init__()
                self.pending_recvs: list[Operation[bytes]] = []

            def recv(self, sock: socket.socket, n: int) -> Operation[bytes]:
                self.recv_calls.append((sock, n))
                operation = Operation[bytes](kind="recv", fileobj=sock.fileno())
                self.pending_recvs.append(operation)
                return operation

        class _EagerAcceptProactor(_PendingRecvProactor):
            def accept_many(self, sock: socket.socket, callback=None):
                conn, peer = socket.socketpair()
                peer.close()
                return _eager_accept_arm(sock, callback, conn)

        delivered: list[tuple[socket.socket, bytes | None]] = []
        proactor = _EagerAcceptProactor()
        scheduler = StubScheduler()
        io = ProactorIOManager(scheduler, proactor)  # type: ignore[arg-type]
        server = socket.socket()
        try:
            io.accept_many(
                server,
                lambda delivery: delivered.append(delivery),
                recv_size=8,
                recv_timeout=0.5,
            )
            assert len(proactor.pending_recvs) == 1
            assert len(scheduler.timer_handles) == 1
            recv_op = proactor.pending_recvs[0]
            assert not recv_op.done()
            scheduler.fire_timers()
            assert recv_op.cancelled()
            assert delivered == []
            conn, _size = proactor.recv_calls[0]
            assert conn.fileno() == -1
        finally:
            server.close()

    def test_accept_many_recv_timeout_skips_arm_when_recv_already_done(self) -> None:
        class _DeferredArmScheduler(StubScheduler):
            def __init__(self) -> None:
                super().__init__()
                self.deferred: list[tuple[Any, tuple[object, ...]]] = []

            def call_soon_threadsafe(self, callback, *args: object, **kwargs: object) -> None:
                del kwargs
                self.deferred.append((callback, args))

        class _EagerAcceptProactor(_MockProactor):
            def accept_many(self, sock: socket.socket, callback=None):
                conn, peer = socket.socketpair()
                peer.close()
                return _eager_accept_arm(sock, callback, conn)

        delivered: list[tuple[socket.socket, bytes | None]] = []
        proactor = _EagerAcceptProactor(recv_result=b"peek")
        scheduler = _DeferredArmScheduler()
        io = ProactorIOManager(scheduler, proactor)  # type: ignore[arg-type]
        server = socket.socket()
        try:
            io.accept_many(
                server,
                lambda delivery: delivered.append(delivery),
                recv_size=8,
                recv_timeout=0.5,
            )
            arm_callbacks: list[Any] = []

            def drain_deferred() -> None:
                while scheduler.deferred:
                    callback, args = scheduler.deferred.pop(0)
                    if callback.__name__ == "arm":
                        arm_callbacks.append(callback)
                    callback(*args)

            drain_deferred()
            assert len(arm_callbacks) == 1
            assert not scheduler.timer_handles
            scheduler.fire_timers()
            assert len(delivered) == 1
            assert delivered[0][1] == b"peek"
        finally:
            for conn, _data in delivered:
                conn.close()
            server.close()

    def test_accept_many_recv_timeout_cancelled_when_recv_completes(self) -> None:
        class _EagerAcceptProactor(_MockProactor):
            def accept_many(self, sock: socket.socket, callback=None):
                conn, peer = socket.socketpair()
                peer.close()
                return _eager_accept_arm(sock, callback, conn)

        delivered: list[tuple[socket.socket, bytes | None]] = []
        proactor = _EagerAcceptProactor(recv_result=b"peek")
        scheduler = StubScheduler()
        io = ProactorIOManager(scheduler, proactor)  # type: ignore[arg-type]
        server = socket.socket()
        try:
            io.accept_many(
                server,
                lambda delivery: delivered.append(delivery),
                recv_size=8,
                recv_timeout=0.5,
            )
            assert not scheduler.timer_handles
            scheduler.fire_timers()
            assert len(delivered) == 1
            assert delivered[0][1] == b"peek"
        finally:
            for conn, _data in delivered:
                conn.close()
            server.close()

    def test_accept_many_on_recv_error_closes_after_callback(self) -> None:
        captured_errors: list[tuple[socket.socket, BaseException]] = []

        class _EagerAcceptProactor(_MockProactor):
            def accept_many(self, sock: socket.socket, callback=None):
                conn, peer = socket.socketpair()
                peer.close()
                return _eager_accept_arm(sock, callback, conn)

            def recv(self, sock: socket.socket, n: int) -> Operation[bytes]:
                operation = Operation[bytes](kind="recv", fileobj=sock.fileno())
                operation._finish(exception=OSError("recv failed"))
                return operation

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

    def test_accept_many_recv_error_without_hook_closes_silently(self) -> None:
        closed: list[socket.socket] = []

        class _EagerAcceptProactor(_MockProactor):
            def accept_many(self, sock: socket.socket, callback=None):
                conn, peer = socket.socketpair()
                peer.close()
                closed.append(conn)
                return _eager_accept_arm(sock, callback, conn)

            def recv(self, sock: socket.socket, n: int) -> Operation[bytes]:
                operation = Operation[bytes](kind="recv", fileobj=sock.fileno())
                operation._finish(exception=OSError("recv failed"))
                return operation

        proactor = _EagerAcceptProactor()
        io = _manager(proactor)
        server = socket.socket()
        try:
            io.accept_many(
                server,
                lambda delivery: (_ for _ in ()).throw(AssertionError("accept callback")),
                recv_size=64,
            )
            assert closed[0].fileno() == -1
        finally:
            server.close()

    def test_accept_many_reports_callback_exception(self) -> None:
        handler_errors: list[BaseException] = []

        class _EagerAcceptProactor(_MockProactor):
            def accept_many(self, sock: socket.socket, callback=None):
                return _eager_accept_arm(sock, callback)

        scheduler = StubScheduler()
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
            def accept_many(self, sock: socket.socket, callback=None):
                return _eager_accept_arm(sock, callback)

        scheduler = StubScheduler()
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

    def test_accept_many_reports_on_recv_error_hook_exception(self) -> None:
        handler_errors: list[BaseException] = []

        class _EagerAcceptProactor(_MockProactor):
            def accept_many(self, sock: socket.socket, callback=None):
                return _eager_accept_arm(sock, callback)

            def recv(self, sock: socket.socket, n: int) -> Operation[bytes]:
                operation = Operation[bytes](kind="recv", fileobj=sock.fileno())
                operation._finish(exception=OSError("recv failed"))
                return operation

        scheduler = StubScheduler()
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

    def test_accept_many_streams_opens_recv_many_before_marshalled_callback(self) -> None:
        class _QueueingScheduler(StubScheduler):
            def __init__(self) -> None:
                super().__init__()
                self.queued: list[tuple[Any, tuple[object, ...]]] = []

            def call_soon_threadsafe(self, callback, *args: object, **kwargs: object) -> None:
                del kwargs
                self.queued.append((callback, args))

        class _EagerAcceptProactor(_MockProactor):
            def accept_many(self, sock: socket.socket, callback=None):
                return _eager_accept_arm(sock, callback)

        proactor = _EagerAcceptProactor()
        scheduler = _QueueingScheduler()
        io = ProactorIOManager(scheduler, proactor)  # type: ignore[arg-type]
        server = socket.socket()
        handled: list[object] = []
        try:
            io.accept_many_streams(server, lambda streams: handled.append(streams))
            assert len(scheduler.queued) == 1
            scheduler.queued[0][0]()
            assert proactor.recv_many_calls
            assert handled
            _reader, writer = handled[0]
            writer.close()
        finally:
            server.close()

    def test_accept_many_streams_closes_socket_when_stream_factory_raises(self) -> None:
        accepted: list[socket.socket] = []

        class _EagerAcceptProactor(_MockProactor):
            def accept_many(self, sock: socket.socket, callback=None):
                conn = _eager_accept_conn()
                accepted.append(conn)
                return _eager_accept_arm(sock, callback, conn)

        def boom(_io: Any, _sock: socket.socket, **kwargs: Any) -> tuple[Any, Any]:
            raise ValueError("stream failed")

        io = _manager(_EagerAcceptProactor())
        server = socket.socket()
        try:
            with pytest.raises(ValueError, match="stream failed"):
                io.accept_many_streams(server, lambda _: None, stream_factory=boom)
            assert len(accepted) == 1
            assert accepted[0].fileno() == -1
        finally:
            server.close()

    def test_accept_many_streams_propagates_marshal_failure_without_closing_socket(self) -> None:
        accepted: list[socket.socket] = []

        class _ShutdownScheduler(StubScheduler):
            def call_soon_threadsafe(self, callback, *args: object, **kwargs: object) -> None:
                del callback, args, kwargs
                raise RuntimeError("scheduler shut down")

        class _EagerAcceptProactor(_MockProactor):
            def accept_many(self, sock: socket.socket, callback=None):
                conn = _eager_accept_conn()
                accepted.append(conn)
                return _eager_accept_arm(sock, callback, conn)

        io = ProactorIOManager(_ShutdownScheduler(), _EagerAcceptProactor())  # type: ignore[arg-type]
        server = socket.socket()
        try:
            with pytest.raises(RuntimeError, match="scheduler shut down"):
                io.accept_many_streams(server, lambda _: None)
            assert len(accepted) == 1
            assert accepted[0].fileno() != -1
            accepted[0].close()
        finally:
            server.close()

    def test_accept_many_streams_uses_bare_socket_callback(self) -> None:
        class _CaptureProactor(_MockProactor):
            def accept_many(self, sock: socket.socket, callback=None):
                self.last_callback = callback
                return ContinuousOperation(kind="accept_many", fileobj=sock)

        proactor = _CaptureProactor()
        io = _manager(proactor)
        server = socket.socket()
        try:
            io.accept_many_streams(server, lambda _: None)
            assert proactor.last_callback is not None
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
            assert proactor.create_socket_calls == []
            assert len(proactor.connect_calls) == 1
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
        assert proactor.last_connect_socket is not None
        assert proactor.last_connect_socket.fileno() == -1


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

    def test_sock_sendall_waiter_add_done_callback_registers_after_return(self):
        proactor = _MockProactor()
        io = _manager(proactor)
        sock = socket.socketpair()[0]
        completed: list[int] = []
        try:
            waiter = io.sock_sendall(sock, b"hello")
            waiter.add_done_callback(lambda: completed.append(1))
            waiter.forget()
            assert completed == [1]
            assert proactor.send_calls == [(sock, b"hello")]
        finally:
            sock.close()

    def test_sock_sendall_waiter_add_done_callback_runs_on_failure(self):
        proactor = _MockProactor()
        io = _manager(proactor)
        sock = socket.socketpair()[0]
        completed: list[int] = []
        try:

            def boom(sock: socket.socket, data: Any, progress: Any = None) -> Operation[None]:
                del data, progress
                operation = Operation[None](kind="send", fileobj=sock)
                operation._finish(exception=OSError("send failed"))
                return operation

            proactor.send = boom  # type: ignore[method-assign]
            waiter = io.sock_sendall(sock, b"hello")
            waiter.add_done_callback(lambda: completed.append(1))
            with pytest.raises(OSError, match="send failed"):
                waiter.wait()
            assert completed == [1]
        finally:
            sock.close()

    def test_sock_sendall_empty_payload_add_done_callback_runs_after_return(self):
        proactor = _MockProactor()
        io = _manager(proactor)
        sock = socket.socketpair()[0]
        phase: list[str] = []
        try:

            def send(target_sock: socket.socket, data: Any, progress: Any = None) -> Operation[None]:
                del data, progress
                phase.append("send")
                operation = Operation[None](kind="send", fileobj=target_sock)
                operation._finish(result=None)
                return operation

            proactor.send = send  # type: ignore[method-assign]
            waiter = io.sock_sendall(sock, b"")
            phase.append("returned")
            waiter.add_done_callback(lambda: phase.append("done"))
            assert phase == ["send", "returned", "done"]
            assert proactor.send_calls == []
        finally:
            sock.close()

    def test_poll_delegates_to_proactor(self):
        proactor = _MockProactor()
        io = _manager(proactor)
        assert io.poll(7, 3).wait() == 3
        assert proactor.poll_calls == [(7, 3)]

    def test_sock_create_applies_scheduler_socket_contract(self):
        proactor = _MockProactor()
        io = _manager(proactor)
        waiter = io.sock_create(socket.AF_INET, socket.SOCK_STREAM)
        assert isinstance(waiter, IOWaiterSync)
        assert waiter.poll() is True
        sock = waiter.wait()
        try:
            import fcntl

            flags = fcntl.fcntl(sock.fileno(), fcntl.F_GETFL)
            assert flags & os.O_NONBLOCK
            assert not os.get_inheritable(sock.fileno())
            assert proactor.create_socket_calls == []
        finally:
            sock.close()

    def test_io_waiter_sync_raises_stored_exception(self) -> None:
        waiter = IOWaiterSync.failed(OSError("create failed"))
        assert waiter.poll() is True
        with pytest.raises(OSError, match="create failed"):
            waiter.wait()
        seen: list[int] = []
        waiter.add_done_callback(lambda: seen.append(1))
        assert seen == [1]

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
            assert proactor.create_socket_calls == []
            assert proactor.connect_calls == [(sock, address)]
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
            assert proactor.create_socket_calls == []
            assert proactor.connect_calls == [(sock, address)]
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

    def test_sock_accept_closes_connection_when_recv_attach_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        listen = socket.socketpair()[0]
        accepted: list[socket.socket] = []

        def accept_capture(sock: socket.socket) -> Operation[socket.socket]:
            conn, _peer = socket.socketpair()
            accepted.append(conn)
            operation = Operation[socket.socket](kind="accept", fileobj=None)
            operation._finish(result=conn)
            return operation

        proactor.accept = accept_capture  # type: ignore[method-assign]

        real_attach = IOWaitGroup.attach

        def attach_fail_recv(self: IOWaitGroup[Any], operation: Operation[Any], **kwargs: Any) -> Any:
            if operation.kind == "recv":
                raise RuntimeError("attach failed")
            return real_attach(self, operation, **kwargs)

        monkeypatch.setattr(IOWaitGroup, "attach", attach_fail_recv)
        try:
            waiter = io.sock_accept(listen, 64)
            assert isinstance(waiter, IOWaitGroup)
            with pytest.raises(RuntimeError, match="attach failed"):
                waiter.wait()
            assert len(accepted) == 1
            assert accepted[0].fileno() == -1
        finally:
            listen.close()

    def test_sock_accept_delivers_empty_initial_read_as_eof(self) -> None:
        proactor = _MockProactor(recv_result=b"")
        io = _manager(proactor)
        listen = socket.socketpair()[0]
        try:
            waiter = io.sock_accept(listen, 64)
            conn, data = waiter.wait()
            try:
                assert data == b""
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
        seen: list[socket.socket] = []

        def failing_connect(
            sock: socket.socket,
            address: Any,
        ) -> Operation[None]:
            del address
            seen.append(sock)
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
        assert len(seen) == 1
        assert seen[0].fileno() == -1

    def test_sock_connect_leaves_socket_open_when_send_fails(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)

        def failing_send(
            sock: socket.socket,
            data: Any,
            progress: Any = None,
        ) -> Operation[None]:
            del data, progress
            operation = Operation[None](kind="send", fileobj=sock)
            operation._finish(exception=OSError("send failed"))
            return operation

        proactor.send = failing_send  # type: ignore[method-assign]
        sock = socket.socketpair()[0]
        try:
            waiter = io.sock_connect(sock, ("127.0.0.1", 9), initial=b"hi")
            assert isinstance(waiter, IOWaitGroup)
            with pytest.raises(OSError, match="send failed"):
                waiter.wait()
            assert sock.fileno() != -1
        finally:
            sock.close()

    def test_sock_create_closes_socket_when_send_fails(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)

        def failing_send(
            sock: socket.socket,
            data: Any,
            progress: Any = None,
        ) -> Operation[None]:
            del data, progress
            operation = Operation[None](kind="send", fileobj=sock)
            operation._finish(exception=OSError("send failed"))
            return operation

        proactor.send = failing_send  # type: ignore[method-assign]
        waiter = io.sock_create(
            socket.AF_INET,
            socket.SOCK_STREAM,
            connect_to=("127.0.0.1", 9),
            initial_data=b"hi",
        )
        assert isinstance(waiter, IOWaitGroup)
        with pytest.raises(OSError, match="send failed"):
            waiter.wait()
        assert proactor.last_connect_socket is not None
        assert proactor.last_connect_socket.fileno() == -1

    def test_io_waiter_wraps_continuous_operation(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        seen: list[int] = []
        operation = ContinuousOperation[int](kind="poll_many", fileobj=5, result_callback=seen.append)
        waiter = IOWaiter(io, operation)
        operation._emit_result(select.POLLIN)
        operation._finish(result=None)
        assert waiter.wait() is None
        assert [delivery.value for delivery in seen] == [select.POLLIN]

    def test_proactor_cancel_stops_continuous_operation_behind_waiter(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        pending = ContinuousOperation[None](kind="poll_many", fileobj=5)
        waiter = IOWaiter(io, pending)
        proactor.cancel(pending)
        assert pending.cancelled() is True
        assert waiter.operation is pending

    def test_io_waiter_exceptional_exit_routes_cancel_through_cancel_operation(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import tealetio.io_waiter as io_waiter_module

        proactor = _MockProactor()
        io = _manager(proactor)
        operation = Operation[bytes](kind="recv")
        waiter = IOWaiter(io, operation)
        cancelled: list[Operation[Any]] = []

        def track_cancel(op: Operation[Any]) -> IOWaiter[None]:
            cancelled.append(op)
            return IOWaiter(io, Operation[None](kind="cancel"))

        monkeypatch.setattr(io, "_cancel_operation", track_cancel)

        original_event = io_waiter_module.CrossThreadEvent

        class RaisingEvent(original_event):
            def swait(self) -> bool:
                raise KeyboardInterrupt()

        monkeypatch.setattr(io_waiter_module, "CrossThreadEvent", RaisingEvent)

        with pytest.raises(KeyboardInterrupt):
            waiter.wait()

        assert cancelled == [operation]

    def test_poll_many_returns_io_waitable(self):
        proactor = _MockProactor()
        io = _manager(proactor)
        seen: list[int] = []

        waiter = io.poll_many(5, 1, seen.append)
        assert isinstance(waiter, IOWaiter)
        assert waiter.operation is not None
        assert waiter.operation.kind == "poll_many"


class TestProactorIOManagerDeferredCompose:
    @pytest.fixture(autouse=True)
    def _patch_uring_probe_capabilities(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_uring_capabilities(monkeypatch)

    def test_sock_accept_cancel_during_pending_recv_closes_connection(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import tealetio.io_waiter as io_waiter_module

        proactor = _MockProactor()
        io = _manager(proactor)
        listen = socket.socketpair()[0]
        pending_recv: list[Operation[bytes]] = []
        accepted_conn: list[socket.socket] = []

        def pending_recv_operation(sock: socket.socket, n: int) -> Operation[bytes]:
            accepted_conn.append(sock)
            operation = Operation[bytes](kind="recv", fileobj=sock.fileno())
            pending_recv.append(operation)
            return operation

        proactor.recv = pending_recv_operation  # type: ignore[method-assign]
        original_swait = io_waiter_module.CrossThreadEvent.swait

        def swait_and_abort(self: Any) -> None:
            raise TimeoutError("abort wait")

        monkeypatch.setattr(io_waiter_module.CrossThreadEvent, "swait", swait_and_abort)
        try:
            waiter = io.sock_accept(listen, 64)
            assert isinstance(waiter, IOWaitGroup)
            assert len(accepted_conn) == 1
            conn = accepted_conn[0]
            assert len(pending_recv) == 1
            with pytest.raises(TimeoutError, match="abort wait"):
                waiter.wait()
            assert pending_recv[0].cancelled()
            assert conn.fileno() == -1
        finally:
            listen.close()
            io_waiter_module.CrossThreadEvent.swait = original_swait

    def test_sock_create_cancel_during_pending_connect_closes_socket(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import tealetio.io_waiter as io_waiter_module

        proactor = UringProactor(ring_factory=_DeferredCreateSocketUringRing)
        scheduler = SyncProactorScheduler(lambda: proactor)
        set_scheduler(scheduler)
        original_swait = io_waiter_module.CrossThreadEvent.swait

        def staged_swait(self: Any) -> None:
            _wait_for_uring(proactor, lambda: len(proactor.ring.pending_connect) == 1)
            raise TimeoutError("abort wait")

        monkeypatch.setattr(io_waiter_module.CrossThreadEvent, "swait", staged_swait)
        try:
            waiter = scheduler.io.sock_create(
                socket.AF_INET,
                socket.SOCK_STREAM,
                connect_to=("127.0.0.1", 9),
                initial_data=b"hi",
            )
            assert isinstance(waiter, IOWaitGroup)
            with pytest.raises(TimeoutError, match="abort wait"):
                waiter.wait()
            assert proactor.ring.submitted_socket == []
            assert proactor.ring.submitted_connect
            leaked_fd = proactor.ring.submitted_connect[0][0]
            with pytest.raises(OSError):
                os.fstat(leaked_fd)
        finally:
            scheduler.close()
            proactor.close()
            io_waiter_module.CrossThreadEvent.swait = original_swait

    def test_sock_create_cancel_during_pending_send_closes_socket(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import tealetio.io_waiter as io_waiter_module

        proactor = UringProactor(ring_factory=_DeferredCreateSocketUringRing)
        scheduler = SyncProactorScheduler(lambda: proactor)
        set_scheduler(scheduler)
        original_swait = io_waiter_module.CrossThreadEvent.swait

        def staged_swait(self: Any) -> None:
            _wait_for_uring(proactor, lambda: len(proactor.ring.pending_connect) == 1)
            _ensure_deferred_connect_completed(proactor.ring)
            _wait_for_uring(proactor, lambda: len(proactor.ring.pending_connect_send) == 1)
            raise TimeoutError("abort wait")

        monkeypatch.setattr(io_waiter_module.CrossThreadEvent, "swait", staged_swait)
        try:
            waiter = scheduler.io.sock_create(
                socket.AF_INET,
                socket.SOCK_STREAM,
                connect_to=("127.0.0.1", 9),
                initial_data=b"hi",
            )
            assert isinstance(waiter, IOWaitGroup)
            with pytest.raises(TimeoutError, match="abort wait"):
                waiter.wait()
            assert proactor.ring.submitted_socket == []
            assert proactor.ring.submitted_connect
            leaked_fd = proactor.ring.submitted_connect[0][0]
            with pytest.raises(OSError):
                os.fstat(leaked_fd)
        finally:
            scheduler.close()
            proactor.close()
            io_waiter_module.CrossThreadEvent.swait = original_swait


class TestIOWaitablePoll:
    def test_io_waiter_poll_tracks_operation_completion(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        listen = socket.socketpair()[0]
        pending: list[Operation[socket.socket]] = []

        def pending_accept(sock: socket.socket) -> Operation[socket.socket]:
            operation = Operation[socket.socket](kind="accept", fileobj=None)
            pending.append(operation)
            return operation

        proactor.accept = pending_accept  # type: ignore[method-assign]
        waiter = io.sock_accept(listen)
        try:
            assert waiter.poll() is False
            conn, _peer = socket.socketpair()
            pending[0]._finish(result=conn)
            assert waiter.poll() is True
            accepted, initial = waiter.wait()
            try:
                assert initial is None
                assert accepted is conn
            finally:
                accepted.close()
            assert waiter.poll() is False
        finally:
            listen.close()

    def test_io_waiter_poll_is_false_after_forget(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        operation = Operation[bytes](kind="recv", fileobj=None)
        proactor.recv = lambda _sock, _n: operation  # type: ignore[method-assign, assignment]
        waiter = io.sock_recv(socket.socket(), 4)
        waiter.forget()
        assert waiter.poll() is False

    def test_io_wait_group_poll_tracks_group_completion(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        operation = Operation[None](kind="pending", fileobj=None)
        group = IOWaitGroup[str](io)
        group.attach(operation)
        assert group.poll() is False
        group.finish("done")
        assert group.poll() is True
        assert group.wait() == "done"




class TestIOWaitGroup:
    def test_group_wait_uses_single_cross_thread_event_for_multi_leg_compose(self) -> None:
        import tealetio.io_waiter as io_waiter_module

        proactor = _MockProactor()
        io = _manager(proactor)
        event_count = 0
        original_event = io_waiter_module.CrossThreadEvent
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

        io_waiter_module.CrossThreadEvent = TrackingEvent  # type: ignore[misc]
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
            io_waiter_module.CrossThreadEvent = original_event

    def test_group_add_done_callback_runs_when_finish_completes(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        completed: list[int] = []
        group = IOWaitGroup[str](io)
        group.add_done_callback(lambda: completed.append(1))
        group.finish("done")
        assert completed == [1]
        assert group.wait() == "done"

    def test_group_add_done_callback_runs_immediately_when_already_done(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        completed: list[int] = []
        group = IOWaitGroup[str](io)
        group.finish("done")
        group.add_done_callback(lambda: completed.append(1))
        assert completed == [1]

    def test_group_attach_sync_completed_operation_clears_members(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        operation = Operation[socket.socket](kind="create", fileobj=None)
        operation._finish(result=sock)
        group = IOWaitGroup[socket.socket](io)
        group.attach(operation, advance=lambda child: group.finish(child.value()))
        assert group._members == set()
        assert group.wait() == sock
        sock.close()

    def test_group_child_value_is_one_shot(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        operation = Operation[int](kind="test", fileobj=None)
        group = IOWaitGroup[int](io)
        child = group.attach(operation, advance=lambda _leg: None)
        operation._finish(result=7)
        assert child.value() == 7
        with pytest.raises(InvalidStateError, match="already consumed"):
            child.value()

    def test_group_child_value_not_ready_raises_invalid_state(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        operation = Operation[int](kind="test", fileobj=None)
        group = IOWaitGroup[int](io)
        child = group.attach(operation, advance=lambda _leg: None)
        with pytest.raises(InvalidStateError, match="not ready"):
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

    def test_group_chained_attach_completes_group(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        first = Operation[None](kind="first", fileobj=None)
        second = Operation[None](kind="second", fileobj=None)
        group = IOWaitGroup[str](io)

        def advance_first(_child: IOWaitGroupChildProtocol[None]) -> None:
            group.attach(second, advance=lambda _second: group.finish("done"))

        group.attach(first, advance=advance_first)
        first._finish(result=None)
        second._finish(result=None)
        assert group.wait() == "done"

    def test_group_late_advance_after_finish_is_rejected(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        first = Operation[None](kind="first", fileobj=None)
        group = IOWaitGroup[str](io)

        def advance_first(_child: IOWaitGroupChildProtocol[None]) -> None:
            group.finish("done")
            with pytest.raises(RuntimeError, match="IOWaitGroup is closed"):
                group.attach(Operation[None](kind="late", fileobj=None))

        group.attach(first, advance=advance_first)
        first._finish(result=None)
        assert group.wait() == "done"

    def test_group_finish_returns_false_after_wait_interrupt(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        operation = Operation[None](kind="pending", fileobj=None)
        group = IOWaitGroup[str](io)
        group.attach(operation)

        import tealetio.io_waiter as io_waiter_module

        original_swait = io_waiter_module.CrossThreadEvent.swait

        def swait_and_abort(self: Any) -> None:
            raise TimeoutError("abort wait")

        io_waiter_module.CrossThreadEvent.swait = swait_and_abort  # type: ignore[method-assign]
        try:
            with pytest.raises(TimeoutError, match="abort wait"):
                group.wait()
            assert group.finish("late") is False
        finally:
            io_waiter_module.CrossThreadEvent.swait = original_swait

    def test_finish_or_close_socket_closes_on_rejected_delivery(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        group = IOWaitGroup[tuple[socket.socket, bytes]](io)
        conn, _peer = socket.socketpair()
        group._closed = True
        _finish_or_close_socket(group, conn, (conn, b"hi"))
        assert conn.fileno() == -1

    def test_group_wait_survives_finish_during_ready_registration(self) -> None:
        import tealetio.io_waiter as io_waiter_module

        proactor = _MockProactor()
        io = _manager(proactor)
        operation = Operation[None](kind="pending", fileobj=None)
        group = IOWaitGroup[str](io)
        group.attach(operation)

        original_event = io_waiter_module.CrossThreadEvent

        class RacingEvent(original_event):
            def __init__(self, scheduler: Any) -> None:
                super().__init__(scheduler)
                group.finish("raced")

        io_waiter_module.CrossThreadEvent = RacingEvent  # type: ignore[misc]
        try:
            assert group.wait() == "raced"
        finally:
            io_waiter_module.CrossThreadEvent = original_event

    def test_group_wait_returns_result_when_delivery_races_interrupt(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        operation = Operation[None](kind="pending", fileobj=None)
        group = IOWaitGroup[str](io)
        group.attach(operation)

        import tealetio.io_waiter as io_waiter_module

        original_swait = io_waiter_module.CrossThreadEvent.swait

        def swait_finish_then_abort(self: Any) -> None:
            group.finish("delivered")
            raise TimeoutError("abort wait")

        io_waiter_module.CrossThreadEvent.swait = swait_finish_then_abort  # type: ignore[method-assign]
        try:
            assert group.wait() == "delivered"
        finally:
            io_waiter_module.CrossThreadEvent.swait = original_swait

    def test_io_waiter_returns_result_when_delivery_races_interrupt(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        listen = socket.socketpair()[0]
        pending: list[Operation[socket.socket]] = []

        def pending_accept(sock: socket.socket) -> Operation[socket.socket]:
            operation = Operation[socket.socket](kind="accept", fileobj=None)
            pending.append(operation)
            return operation

        proactor.accept = pending_accept  # type: ignore[method-assign]

        import tealetio.io_waiter as io_waiter_module

        original_swait = io_waiter_module.CrossThreadEvent.swait
        waiter = io.sock_accept(listen)

        def swait_complete_then_abort(self: Any) -> None:
            conn, _peer = socket.socketpair()
            pending[0]._finish(result=conn)
            raise TimeoutError("abort wait")

        io_waiter_module.CrossThreadEvent.swait = swait_complete_then_abort  # type: ignore[method-assign]
        try:
            conn, initial = waiter.wait()
            try:
                assert initial is None
                assert conn.fileno() != -1
            finally:
                conn.close()
        finally:
            listen.close()
            io_waiter_module.CrossThreadEvent.swait = original_swait

    def test_group_wait_cancels_active_operations_on_exception(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        operation = Operation[None](kind="pending", fileobj=None)
        group = IOWaitGroup[None](io)
        group.attach(operation)

        import tealetio.io_waiter as io_waiter_module

        original_swait = io_waiter_module.CrossThreadEvent.swait

        def swait_and_abort(self: Any) -> None:
            raise TimeoutError()

        io_waiter_module.CrossThreadEvent.swait = swait_and_abort  # type: ignore[method-assign]
        try:
            with pytest.raises(TimeoutError):
                group.wait()
            assert operation.cancelled()
        finally:
            io_waiter_module.CrossThreadEvent.swait = original_swait


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



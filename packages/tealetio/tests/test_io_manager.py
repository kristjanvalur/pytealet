from __future__ import annotations

import os
import socket
from typing import Any
from unittest.mock import patch

import pytest

from tealetio import set_scheduler
from tealetio.io_manager import FileIO, PollIO, ProactorIOManager, ServerIO, SocketIO
from tealetio.operations import ContinuousOperation, Operation
from tealetio.proactor import SyncProactorScheduler
from uring_fakes import SCHEDULER_INTEGRATION_FACTORIES


class _StubScheduler:
    """Minimal scheduler stand-in for direct ``ProactorIOManager`` unit tests."""


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

    def recv(self, sock: socket.socket, n: int) -> Operation[bytes]:
        self.recv_calls.append((sock, n))
        operation = Operation[bytes](kind="recv", fileobj=sock.fileno())
        operation._set_result(b"mock")
        return operation

    def poll(self, fd: int, mask: int) -> Operation[int]:
        self.poll_calls.append((fd, mask))
        operation = Operation[int](kind="poll", fileobj=fd)
        operation._set_result(mask)
        return operation

    def send(self, sock: socket.socket, data: Any, progress: Any = None) -> Operation[None]:
        del progress
        self.send_calls.append((sock, data))
        operation = Operation[None](kind="send", fileobj=sock.fileno())
        operation._set_result(None)
        return operation

    def openat(self, path: str, flags: int, mode: int) -> Operation[int]:
        self.openat_calls.append((path, flags, mode))
        operation = Operation[int](kind="openat", fileobj=-1)
        operation._set_result(901)
        return operation

    def create_socket(
        self,
        family: int,
        type: int,
        proto: int = 0,
        *,
        flags: int = 0,
        connect_to: Any | None = None,
        initial_data: Any | None = None,
    ) -> Operation[tuple[socket.socket, bool, bool]]:
        self.create_socket_calls.append((family, type, proto, flags, connect_to, initial_data))
        operation = Operation[tuple[socket.socket, bool, bool]](kind="create_socket", fileobj=(family, type, proto))
        sock = socket.socket(family, type, proto)
        sock.setblocking(False)
        os.set_inheritable(sock.fileno(), False)
        self.last_create_socket = sock
        operation._set_result((sock, False, False))
        return operation

    def connect(
        self,
        sock: socket.socket,
        address: Any,
        *,
        initial: Any | None = None,
    ) -> Operation[None]:
        del sock, address, initial
        operation = Operation[None](kind="connect", fileobj=None)
        operation._set_result(None)
        return operation

    def poll_many(
        self,
        fd: int,
        mask: int,
        callback: Any,
    ) -> ContinuousOperation[int]:
        operation = ContinuousOperation[int](kind="poll_many", fileobj=fd)
        operation._set_result(mask)
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


class TestProactorIOManagerDirect:
    def test_wait_operation_returns_immediate_result(self):
        proactor = _MockProactor()
        io = _manager(proactor)
        sock = socket.socketpair()[0]
        try:
            operation = proactor.recv(sock, 4)
            assert io.wait_operation(operation) == b"mock"
        finally:
            sock.close()

    def test_sock_recv_delegates_to_proactor(self):
        proactor = _MockProactor()
        io = _manager(proactor)
        sock = socket.socketpair()[0]
        try:
            assert io.sock_recv(sock, 4) == b"mock"
            assert proactor.recv_calls == [(sock, 4)]
        finally:
            sock.close()

    def test_sock_sendall_delegates_to_proactor(self):
        proactor = _MockProactor()
        io = _manager(proactor)
        sock = socket.socketpair()[0]
        try:
            io.sock_sendall(sock, b"hello")
            assert proactor.send_calls == [(sock, b"hello")]
        finally:
            sock.close()

    def test_poll_delegates_to_proactor(self):
        proactor = _MockProactor()
        io = _manager(proactor)
        assert io.poll(7, 3) == 3
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
        sock, is_connected, initial_sent = io.sock_create(socket.AF_INET, socket.SOCK_STREAM)
        try:
            import fcntl

            flags = fcntl.fcntl(sock.fileno(), fcntl.F_GETFL)
            assert flags & os.O_NONBLOCK
            assert not os.get_inheritable(sock.fileno())
            assert is_connected is False
            assert initial_sent is False
        finally:
            sock.close()

    def test_sock_create_fallback_without_initial_data_reports_initial_sent_false(self):
        proactor = _MockProactor()
        io = _manager(proactor)
        connect_calls: list[tuple[socket.socket, Any, Any | None]] = []

        def fake_sock_connect(sock: socket.socket, address: Any, *, initial: Any | None = None) -> None:
            connect_calls.append((sock, address, initial))

        io.sock_connect = fake_sock_connect  # type: ignore[method-assign]
        address = ("127.0.0.1", 9)
        sock, is_connected, initial_sent = io.sock_create(
            socket.AF_INET,
            socket.SOCK_STREAM,
            connect_to=address,
        )
        try:
            assert len(connect_calls) == 1
            assert connect_calls[0][2] is None
            assert is_connected is True
            assert initial_sent is False
        finally:
            sock.close()

    def test_sock_create_falls_back_to_sock_connect_when_hints_ignored(self):
        proactor = _MockProactor()
        io = _manager(proactor)
        connect_calls: list[tuple[socket.socket, Any, Any | None]] = []

        def fake_sock_connect(sock: socket.socket, address: Any, *, initial: Any | None = None) -> None:
            connect_calls.append((sock, address, initial))

        io.sock_connect = fake_sock_connect  # type: ignore[method-assign]
        address = ("127.0.0.1", 9)
        sock, is_connected, initial_sent = io.sock_create(
            socket.AF_INET,
            socket.SOCK_STREAM,
            connect_to=address,
            initial_data=b"hi",
        )
        try:
            assert len(connect_calls) == 1
            assert connect_calls[0][0] is sock
            assert connect_calls[0][1] == address
            assert connect_calls[0][2] == b"hi"
            assert is_connected is True
            assert initial_sent is True
        finally:
            sock.close()

    def test_sock_create_rejects_initial_data_without_connect_to(self):
        proactor = _MockProactor()
        io = _manager(proactor)
        with pytest.raises(ValueError, match="initial_data requires connect_to"):
            io.sock_create(
                socket.AF_INET,
                socket.SOCK_STREAM,
                initial_data=b"hi",
            )

    def test_sock_create_closes_socket_when_connect_fallback_fails(self):
        proactor = _MockProactor()
        io = _manager(proactor)

        def failing_sock_connect(sock: socket.socket, address: Any, *, initial: Any | None = None) -> None:
            del sock, address, initial
            raise OSError("connect failed")

        io.sock_connect = failing_sock_connect  # type: ignore[method-assign]
        with pytest.raises(OSError, match="connect failed"):
            io.sock_create(
                socket.AF_INET,
                socket.SOCK_STREAM,
                connect_to=("127.0.0.1", 9),
            )
        assert proactor.last_create_socket is not None
        assert proactor.last_create_socket.fileno() == -1

    def test_open_returns_proactor_file(self):
        proactor = _MockProactor()
        io = _manager(proactor)
        with patch("tealetio.io_manager.os.close"), patch("tealetio.files.os.close"):
            handle = io.open("/tmp/example.txt", "rb")
            try:
                from tealetio.files import ProactorFile

                assert isinstance(handle, ProactorFile)
                assert hasattr(handle, "read")
                assert hasattr(handle, "seek")
                assert handle.name == "/tmp/example.txt"
                assert proactor.openat_calls == [("/tmp/example.txt", os.O_RDONLY | os.O_CLOEXEC, 0o666)]
            finally:
                handle.close()

    def test_poll_many_returns_continuous_operation(self):
        proactor = _MockProactor()
        io = _manager(proactor)
        seen: list[int] = []

        operation = io.poll_many(5, 1, seen.append)
        assert isinstance(operation, ContinuousOperation)


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
                return scheduler.io.sock_recv(server, 4)

            assert scheduler.run_until_complete(scheduler.spawn(exercise)) == b"ping"

            def read_zero() -> bytes:
                return scheduler.io.sock_recv(server, 0)

            assert scheduler.run_until_complete(scheduler.spawn(read_zero)) == b""
        finally:
            client.close()
            server.close()

    def test_wait_operation_blocks_until_completion(self, scheduler: SyncProactorScheduler) -> None:
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            writer.sendall(b"z")

            def exercise() -> bytes:
                return scheduler.io.sock_recv(reader, 1)

            assert scheduler.run_until_complete(scheduler.spawn(exercise)) == b"z"
        finally:
            reader.close()
            writer.close()

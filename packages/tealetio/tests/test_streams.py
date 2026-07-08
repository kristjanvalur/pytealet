from __future__ import annotations

import asyncio
import errno
import socket
import tempfile
from pathlib import Path
from typing import Any

import pytest

from tealetio import Event, set_scheduler
from tealetio.operations import Operation
from tealetio.proactor import SyncProactorScheduler, UringProactor
from tealetio.streams import (
    AsyncStreamReader,
    AsyncStreamWriter,
    SocketTransport,
    StreamReader,
    StreamWriter,
    open_connection,
    open_streams,
    run_coro,
    start_server,
)
from uring_fakes import (
    SCHEDULER_INTEGRATION_FACTORIES,
    _FakeUringRing,
    _patch_uring_capabilities,
    run_scheduler_task,
)

from tealetio.streams import _default_reuse_address

_HAS_AF_UNIX = hasattr(socket, "AF_UNIX")


def _scheduler_with_fake_ring() -> SyncProactorScheduler:
    return SyncProactorScheduler(lambda: UringProactor(ring_factory=_FakeUringRing))


@pytest.mark.parametrize("scheduler_factory", SCHEDULER_INTEGRATION_FACTORIES)
class TestStreamsPoC:
    @pytest.fixture
    def scheduler(self, scheduler_factory) -> SyncProactorScheduler:
        sched = scheduler_factory()
        set_scheduler(sched)
        yield sched
        sched.close()

    def test_async_stream_readexactly(self, scheduler: SyncProactorScheduler) -> None:
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(True)

            async def handler() -> bytes:
                stream_reader, _stream_writer = open_streams(reader, async_=True)
                return await stream_reader.readexactly(5)

            def deliver() -> None:
                writer.sendall(b"hello")

            def exercise() -> bytes:
                scheduler.spawn(deliver)
                return run_coro(handler())

            assert scheduler.run_until_complete(scheduler.spawn(exercise)) == b"hello"
        finally:
            reader.close()
            writer.close()

    def test_async_stream_write_drain_and_readline(self, scheduler: SyncProactorScheduler) -> None:
        client, server = socket.socketpair()
        try:
            client.setblocking(False)
            server.setblocking(False)

            async def echo_handler() -> bytes:
                stream_reader, stream_writer = open_streams(server, async_=True)
                line = await stream_reader.readline()
                stream_writer.write(line.upper())
                await stream_writer.drain()
                return line

            def client_side() -> None:
                client.sendall(b"ping\n")

            def exercise() -> bytes:
                scheduler.spawn(client_side)
                return run_coro(echo_handler())

            assert scheduler.run_until_complete(scheduler.spawn(exercise)) == b"ping\n"
            assert client.recv(16) == b"PING\n"
        finally:
            client.close()
            server.close()

    def test_stream_readline_raises_limit_overrun_without_newline(self, scheduler: SyncProactorScheduler) -> None:
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(True)

            def flood() -> None:
                writer.sendall(b"x" * 8)

            def exercise() -> None:
                stream_reader, _stream_writer = open_streams(reader, limit=8)
                scheduler.spawn(flood)
                stream_reader.readline()

            with pytest.raises(asyncio.LimitOverrunError) as excinfo:
                scheduler.run_until_complete(scheduler.spawn(exercise))
            assert excinfo.value.consumed == 8
        finally:
            reader.close()
            writer.close()

    def test_start_server_closes_listener_socket_when_bind_fails(self, scheduler: SyncProactorScheduler) -> None:
        def client_handler(reader: StreamReader, writer: StreamWriter) -> None:
            writer.close()

        def exercise() -> None:
            blocker, _, _ = scheduler.io.sock_create(socket.AF_INET, socket.SOCK_STREAM)
            blocker.bind(("127.0.0.1", 0))
            port = blocker.getsockname()[1]
            blocker.listen(1)
            try:
                with pytest.raises(OSError):
                    start_server(client_handler, addr=("127.0.0.1", port), scheduler=scheduler)
            finally:
                blocker.close()

        run_scheduler_task(scheduler, exercise)

    def test_native_stream_read_and_write(self, scheduler: SyncProactorScheduler) -> None:
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(True)

            def handler() -> bytes:
                stream_reader, stream_writer = open_streams(reader)
                assert isinstance(stream_reader, StreamReader)
                assert isinstance(stream_writer, StreamWriter)
                payload = stream_reader.readexactly(5)
                stream_writer.write(b"ack")
                stream_writer.drain()
                return payload

            def deliver() -> None:
                writer.sendall(b"hello")

            def exercise() -> bytes:
                scheduler.spawn(deliver)
                return handler()

            assert scheduler.run_until_complete(scheduler.spawn(exercise)) == b"hello"
            assert writer.recv(8) == b"ack"
        finally:
            reader.close()
            writer.close()

    def test_open_streams_uses_custom_stream_factory(self, scheduler: SyncProactorScheduler) -> None:
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)

            class TaggedStreamReader(StreamReader):
                tag = "native-custom"

            def custom_factory(io, sock, *, limit):
                transport = SocketTransport(io, sock)
                stream_reader = TaggedStreamReader(transport, limit=limit)
                stream_writer = StreamWriter(transport, stream_reader)
                return stream_reader, stream_writer

            stream_reader, _stream_writer = open_streams(
                reader,
                stream_factory=custom_factory,
                scheduler=scheduler,
            )
            assert isinstance(stream_reader, TaggedStreamReader)
            assert stream_reader.tag == "native-custom"
        finally:
            reader.close()
            writer.close()

    def test_open_async_streams_uses_custom_stream_factory(self, scheduler: SyncProactorScheduler) -> None:
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)

            class TaggedAsyncStreamReader(AsyncStreamReader):
                tag = "async-custom"

            def custom_factory(io, sock, *, limit):
                transport = SocketTransport(io, sock)
                stream_reader = TaggedAsyncStreamReader(transport, limit=limit)
                stream_writer = AsyncStreamWriter(transport, stream_reader)
                return stream_reader, stream_writer

            stream_reader, _stream_writer = open_streams(
                reader,
                stream_factory=custom_factory,
                async_=True,
                scheduler=scheduler,
            )
            assert isinstance(stream_reader, TaggedAsyncStreamReader)
            assert stream_reader.tag == "async-custom"
        finally:
            reader.close()
            writer.close()

    def test_open_connection_resolves_literal_ip_inside_scheduler(self, scheduler: SyncProactorScheduler) -> None:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_greeting = b"hello"
        try:
            server.setblocking(False)
            server.bind(("127.0.0.1", 0))
            server.listen()
            _host, port = server.getsockname()

            def accept_and_echo() -> None:
                conn = scheduler.io.sock_accept(server)
                try:
                    payload = scheduler.io.sock_recv(conn, len(client_greeting))
                    scheduler.io.sock_sendall(conn, payload.upper())
                finally:
                    conn.close()

            def connect_via_streams() -> bytes:
                stream_reader, stream_writer = open_connection(addr=("127.0.0.1", port))
                stream_writer.write(client_greeting)
                stream_writer.drain()
                return stream_reader.readexactly(len(client_greeting))

            connect_task = scheduler.spawn(connect_via_streams)
            scheduler.spawn(accept_and_echo)
            assert scheduler.run_until_complete(connect_task) == b"HELLO"
        finally:
            server.close()

    def test_open_connection_resolves_hostname_via_scheduler(self, scheduler: SyncProactorScheduler) -> None:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            server.setblocking(False)
            server.bind(("127.0.0.1", 0))
            server.listen()
            _host, port = server.getsockname()

            def accept_and_echo() -> None:
                conn = scheduler.io.sock_accept(server)
                try:
                    scheduler.io.sock_sendall(conn, b"PONG")
                finally:
                    conn.close()

            def connect_via_hostname() -> bytes:
                stream_reader, stream_writer = open_connection(addr=("localhost", port))
                stream_writer.write(b"ping")
                stream_writer.drain()
                return stream_reader.readexactly(4)

            connect_task = scheduler.spawn(connect_via_hostname)
            scheduler.spawn(accept_and_echo)
            assert scheduler.run_until_complete(connect_task) == b"PONG"
        finally:
            server.close()

    def test_open_connection_literal_ip_skips_executor(
        self, scheduler: SyncProactorScheduler, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls: list[object] = []
        real_run = scheduler.run_in_executor

        def tracking_run(executor, func, *args):
            calls.append(func)
            return real_run(executor, func, *args)

        monkeypatch.setattr(scheduler, "run_in_executor", tracking_run)
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            server.setblocking(False)
            server.bind(("127.0.0.1", 0))
            server.listen()
            _host, port = server.getsockname()

            def accept_side() -> None:
                conn = scheduler.io.sock_accept(server)
                conn.close()

            def connect_via_literal_ip() -> None:
                _reader, writer = open_connection(addr=("127.0.0.1", port))
                writer.close()

            connect_task = scheduler.spawn(connect_via_literal_ip)
            scheduler.spawn(accept_side)
            scheduler.run_until_complete(connect_task)
            assert calls == []
        finally:
            server.close()

    def test_open_connection_hostname_uses_executor(
        self, scheduler: SyncProactorScheduler, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls: list[object] = []
        real_run = scheduler.run_in_executor

        def tracking_run(executor, func, *args):
            calls.append(func)
            return real_run(executor, func, *args)

        monkeypatch.setattr(scheduler, "run_in_executor", tracking_run)
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            server.setblocking(False)
            server.bind(("127.0.0.1", 0))
            server.listen()
            _host, port = server.getsockname()

            def accept_side() -> None:
                conn = scheduler.io.sock_accept(server)
                conn.close()

            def connect_via_hostname() -> None:
                _reader, writer = open_connection(addr=("localhost", port))
                writer.close()

            connect_task = scheduler.spawn(connect_via_hostname)
            scheduler.spawn(accept_side)
            scheduler.run_until_complete(connect_task)
            assert calls == [socket.getaddrinfo]
        finally:
            server.close()

    def test_open_async_connection_resolves_literal_ip_inside_scheduler(self, scheduler: SyncProactorScheduler) -> None:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            server.setblocking(False)
            server.bind(("127.0.0.1", 0))
            server.listen()
            _host, port = server.getsockname()

            async def connect_and_ping() -> bytes:
                stream_reader, stream_writer = open_connection(addr=("127.0.0.1", port), async_=True)
                stream_writer.write(b"ping")
                await stream_writer.drain()
                return await stream_reader.readexactly(4)

            def accept_side() -> None:
                conn = scheduler.io.sock_accept(server)
                try:
                    scheduler.io.sock_sendall(conn, b"pong")
                finally:
                    conn.close()

            def exercise() -> bytes:
                scheduler.spawn(accept_side)
                return run_coro(connect_and_ping())

            assert scheduler.run_until_complete(scheduler.spawn(exercise)) == b"pong"
        finally:
            server.close()

    def test_stream_readinto_fills_caller_buffer(self, scheduler: SyncProactorScheduler) -> None:
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(True)

            def handler() -> tuple[int, bytes]:
                stream_reader, _stream_writer = open_streams(reader)
                buf = bytearray(8)
                nbytes = stream_reader.readinto(buf)
                return nbytes, bytes(buf)

            def deliver() -> None:
                writer.sendall(b"hello")

            def exercise() -> tuple[int, bytes]:
                scheduler.spawn(deliver)
                return handler()

            nbytes, payload = scheduler.run_until_complete(scheduler.spawn(exercise))
            assert nbytes == 5
            assert payload[:5] == b"hello"
        finally:
            reader.close()
            writer.close()

    def test_stream_reads_use_recv_into_not_sock_recv(self, scheduler: SyncProactorScheduler, monkeypatch) -> None:
        reader, writer = socket.socketpair()
        recv_calls: list[tuple[object, ...]] = []
        recv_into_calls: list[tuple[object, ...]] = []
        real_recv = scheduler.io.sock_recv
        real_recv_into = scheduler.io.sock_recv_into

        def tracking_recv(sock, n):
            recv_calls.append((sock, n))
            return real_recv(sock, n)

        def tracking_recv_into(sock, buf):
            recv_into_calls.append((sock, memoryview(buf).nbytes))
            return real_recv_into(sock, buf)

        monkeypatch.setattr(scheduler.io, "sock_recv", tracking_recv)
        monkeypatch.setattr(scheduler.io, "sock_recv_into", tracking_recv_into)
        try:
            reader.setblocking(False)
            writer.setblocking(True)

            def handler() -> bytes:
                stream_reader, _stream_writer = open_streams(reader)
                return stream_reader.readexactly(5)

            def deliver() -> None:
                writer.sendall(b"hello")

            def exercise() -> bytes:
                scheduler.spawn(deliver)
                return handler()

            assert scheduler.run_until_complete(scheduler.spawn(exercise)) == b"hello"
            assert recv_calls == []
            assert recv_into_calls
            assert recv_into_calls[0][1] >= 5
        finally:
            reader.close()
            writer.close()

    def test_open_connection_unix_path_round_trip(self, scheduler: SyncProactorScheduler) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = str(Path(temp_dir) / "stream.sock")
            server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                server.bind(path)
                server.listen()
                server.setblocking(False)

                def accept_and_echo() -> None:
                    conn = scheduler.io.sock_accept(server)
                    try:
                        payload = scheduler.io.sock_recv(conn, 5)
                        scheduler.io.sock_sendall(conn, payload.upper())
                    finally:
                        conn.close()

                def connect_via_unix_streams() -> bytes:
                    stream_reader, stream_writer = open_connection(path=path)
                    stream_writer.write(b"hello")
                    stream_writer.drain()
                    return stream_reader.readexactly(5)

                connect_task = scheduler.spawn(connect_via_unix_streams)
                scheduler.spawn(accept_and_echo)
                assert scheduler.run_until_complete(connect_task) == b"HELLO"
            finally:
                server.close()

    def test_stream_server_context_manager_closes_listener(self, scheduler: SyncProactorScheduler) -> None:
        def client_handler(reader: StreamReader, writer: StreamWriter) -> None:
            writer.close()

        def exercise() -> None:
            server = start_server(client_handler, addr=("127.0.0.1", 0), scheduler=scheduler)
            listen_sock = server.sockets[0]
            with server:
                assert listen_sock.fileno() != -1
                assert server.accept_operation.kind == "accept_many"
            assert listen_sock.fileno() == -1

        run_scheduler_task(scheduler, exercise)

    def test_stream_server_serve_forever_unblocks_on_close(self, scheduler: SyncProactorScheduler) -> None:
        finished = Event()

        def client_handler(reader: StreamReader, writer: StreamWriter) -> None:
            writer.close()

        def exercise() -> None:
            server = start_server(client_handler, addr=("127.0.0.1", 0), scheduler=scheduler)

            def run_server() -> None:
                server.serve_forever()
                finished.set()

            def shutdown() -> None:
                scheduler.yield_()
                server.close()

            scheduler.spawn(run_server)
            scheduler.spawn(shutdown)
            finished.swait()

        run_scheduler_task(scheduler, exercise)

    def test_stream_server_wait_closed_returns_after_close_with_no_handlers(
        self, scheduler: SyncProactorScheduler
    ) -> None:
        def client_handler(reader: StreamReader, writer: StreamWriter) -> None:
            writer.close()

        def exercise() -> None:
            server = start_server(client_handler, addr=("127.0.0.1", 0), scheduler=scheduler)
            server.close()
            server.wait_closed()

        run_scheduler_task(scheduler, exercise)

    def test_stream_server_wait_closed_waits_for_in_flight_handler(self, scheduler: SyncProactorScheduler) -> None:
        handler_started = Event()
        release_handler = Event()
        wait_finished = Event()

        def client_handler(reader: StreamReader, writer: StreamWriter) -> None:
            handler_started.set()
            release_handler.swait()
            writer.close()

        def exercise() -> None:
            server = start_server(client_handler, addr=("127.0.0.1", 0), scheduler=scheduler)
            _host, port = server.sockets[0].getsockname()

            def connect() -> None:
                _reader, writer = open_connection(addr=("127.0.0.1", port))
                writer.write(b"x")
                writer.drain()
                writer.close()

            def wait_for_closed() -> None:
                server.close()
                server.wait_closed()
                wait_finished.set()

            scheduler.spawn(connect)
            handler_started.swait()
            wait_task = scheduler.spawn(wait_for_closed)
            scheduler.yield_()
            assert not wait_finished.is_set()
            release_handler.set()
            wait_task.wait()

        run_scheduler_task(scheduler, exercise)
        assert wait_finished.is_set()

    def test_stream_server_spawn_failure_restores_handler_count(
        self, scheduler: SyncProactorScheduler, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        real_spawn = scheduler.spawn
        serve_spawn_attempts = 0
        callback_errors: list[BaseException] = []

        def failing_spawn(func, **kwargs):
            nonlocal serve_spawn_attempts
            if func.__name__ == "serve":
                serve_spawn_attempts += 1
                raise RuntimeError("spawn failed")
            return real_spawn(func, **kwargs)

        monkeypatch.setattr(scheduler, "spawn", failing_spawn)
        scheduler.set_exception_handler(lambda context: callback_errors.append(context["exception"]))

        def client_handler(reader: StreamReader, writer: StreamWriter) -> None:
            writer.close()

        _client, accepted = socket.socketpair()
        try:
            accepted.setblocking(False)

            def exercise() -> None:
                server = start_server(client_handler, addr=("127.0.0.1", 0), scheduler=scheduler)
                try:
                    server._dispatch_client(
                        accepted,
                        limit=2**16,
                        stream_factory=None,
                        client_handler=client_handler,
                        async_=False,
                    )
                    scheduler.yield_()
                    server.close()
                    server.wait_closed()
                finally:
                    server.close()

            run_scheduler_task(scheduler, exercise)
            assert serve_spawn_attempts == 1
            assert len(callback_errors) == 1
            assert str(callback_errors[0]) == "spawn failed"
        finally:
            _client.close()

    def test_stream_server_dispatch_client_on_closed_server_closes_connection(
        self, scheduler: SyncProactorScheduler
    ) -> None:

        def client_handler(reader: StreamReader, writer: StreamWriter) -> None:
            writer.close()

        _client, accepted = socket.socketpair()
        try:

            def exercise() -> None:
                server = start_server(client_handler, addr=("127.0.0.1", 0), scheduler=scheduler)
                server.close()
                server._dispatch_client(
                    accepted,
                    limit=2**16,
                    stream_factory=None,
                    client_handler=client_handler,
                    async_=False,
                )
                server.wait_closed()
                assert server._active_handlers == 0
                assert accepted.fileno() == -1

            run_scheduler_task(scheduler, exercise)
        finally:
            _client.close()

    def test_start_server_closes_accept_before_server_is_ready(
        self, scheduler: SyncProactorScheduler, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        real_accept_many = scheduler.proactor.accept_many

        def eager_accept_many(sock: socket.socket, callback, *, recv_size=None):
            operation = real_accept_many(sock, callback, recv_size=recv_size)
            _client, accepted = socket.socketpair()
            accepted.setblocking(False)
            try:
                callback((accepted, None, None))
                assert accepted.fileno() == -1
            finally:
                _client.close()
            return operation

        monkeypatch.setattr(scheduler.proactor, "accept_many", eager_accept_many)

        def client_handler(reader: StreamReader, writer: StreamWriter) -> None:
            writer.close()

        def exercise() -> None:
            server = start_server(client_handler, addr=("127.0.0.1", 0), scheduler=scheduler)
            try:
                assert server._active_handlers == 0
            finally:
                server.close()

        run_scheduler_task(scheduler, exercise)

    def test_stream_reader_feed_initial_avoids_socket_recv(self, scheduler: SyncProactorScheduler) -> None:
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            stream_reader, _stream_writer = open_streams(reader, scheduler=scheduler)
            stream_reader.feed_initial(b"cached")
            assert stream_reader.read(6) == b"cached"
        finally:
            reader.close()
            writer.close()

    def test_start_server_passes_recv_size_only_when_opted_in(
        self, scheduler: SyncProactorScheduler, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def client_handler(reader: StreamReader, writer: StreamWriter) -> None:
            writer.close()

        captured_results: list[int | None] = []

        def run_with_recv_size(recv_size: int | None) -> None:
            captured: list[int | None] = []
            real_accept_many = scheduler.proactor.accept_many

            def capture_accept_many(sock: socket.socket, callback, *, recv_size=None):
                captured.append(recv_size)
                return real_accept_many(sock, callback, recv_size=recv_size)

            monkeypatch.setattr(scheduler.proactor, "accept_many", capture_accept_many)

            def exercise() -> None:
                server = start_server(
                    client_handler,
                    addr=("127.0.0.1", 0),
                    recv_size=recv_size,
                    scheduler=scheduler,
                )
                try:
                    captured_results.append(captured[-1])
                finally:
                    server.close()

            run_scheduler_task(scheduler, exercise)

        run_with_recv_size(None)
        run_with_recv_size(1024)
        assert captured_results == [None, 1024]

    def test_start_server_uses_accept_many_and_dispatches_handler(self, scheduler: SyncProactorScheduler) -> None:
        handled = Event()
        received: list[bytes] = []

        def client_handler(reader: StreamReader, writer: StreamWriter) -> None:
            received.append(reader.readexactly(3))
            writer.close()
            handled.set()

        def exercise() -> None:
            server = start_server(client_handler, addr=("127.0.0.1", 0), scheduler=scheduler)
            try:
                assert server.accept_operation.kind == "accept_many"
                _host, port = server.sockets[0].getsockname()

                def connect_and_send() -> None:
                    _reader, writer = open_connection(addr=("127.0.0.1", port))
                    writer.write(b"abc")
                    writer.drain()
                    writer.close()

                scheduler.spawn(connect_and_send)
                handled.swait()
            finally:
                server.close()

        run_scheduler_task(scheduler, exercise)
        assert received == [b"abc"]

    def test_start_server_unix_path_dispatches_async_handler(self, scheduler: SyncProactorScheduler) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = str(Path(temp_dir) / "server.sock")
            server = None
            try:

                async def client_handler(reader: AsyncStreamReader, writer: AsyncStreamWriter) -> None:
                    payload = await reader.readexactly(4)
                    writer.write(payload.upper())
                    await writer.drain()
                    writer.close()

                responses: list[bytes] = []
                finished = Event()

                def connect_and_read() -> None:
                    stream_reader, stream_writer = open_connection(path=path)
                    stream_writer.write(b"ping")
                    stream_writer.drain()
                    responses.append(stream_reader.readexactly(4))
                    stream_writer.close()
                    finished.set()

                def exercise() -> None:
                    nonlocal server
                    server = start_server(client_handler, path=path, async_=True, scheduler=scheduler)
                    scheduler.spawn(connect_and_read)
                    finished.swait()

                run_scheduler_task(scheduler, exercise)
                assert responses == [b"PING"]
            finally:
                if server is not None:
                    server.close()

    def test_start_async_server_echoes_over_tcp(self, scheduler: SyncProactorScheduler) -> None:
        server = None
        try:

            async def client_handler(reader: AsyncStreamReader, writer: AsyncStreamWriter) -> None:
                payload = await reader.readexactly(3)
                writer.write(payload.upper())
                await writer.drain()
                writer.close()

            responses: list[bytes] = []
            finished = Event()

            def exercise() -> None:
                nonlocal server
                server = start_server(client_handler, addr=("127.0.0.1", 0), async_=True, scheduler=scheduler)
                _host, port = server.sockets[0].getsockname()

                async def connect_and_read() -> None:
                    stream_reader, stream_writer = open_connection(addr=("127.0.0.1", port), async_=True)
                    stream_writer.write(b"xyz")
                    await stream_writer.drain()
                    responses.append(await stream_reader.readexactly(3))
                    stream_writer.close()
                    finished.set()

                scheduler.spawn(lambda: run_coro(connect_and_read()))
                finished.swait()

            run_scheduler_task(scheduler, exercise)
            assert responses == [b"XYZ"]
        finally:
            if server is not None:
                server.close()

    def test_sock_connect_passes_connect_send_factory_when_initial_provided(
        self, scheduler: SyncProactorScheduler, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from tealetio.operation_delivery import connect_initial_send_factory

        io = scheduler.io
        client, _peer = socket.socketpair()
        captured: list[object | None] = []

        def fake_connect(sock: socket.socket, address, *, operation_factory=None):
            del sock, address
            captured.append(operation_factory)
            operation = Operation[None](kind="connect", fileobj=client)
            operation._finish(result=None)
            return operation

        monkeypatch.setattr(scheduler.proactor, "connect", fake_connect)

        try:
            client.setblocking(False)
            io.sock_connect(client, ("127.0.0.1", 0), initial=b"helloworld")
            assert len(captured) == 1
            factory = captured[0]
            assert factory is not None
            chained = factory("connect", client)
            expected = connect_initial_send_factory(b"helloworld")("connect", client)
            assert chained._delivery is not None
            assert chained._advance_hook is not None
            assert expected._delivery is not None
            assert expected._advance_hook is not None
        finally:
            client.close()

    def test_open_connection_passes_initial_send_to_sock_create(
        self, scheduler: SyncProactorScheduler, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured: list[bytes | None] = []
        real_sock_create = scheduler.io.sock_create

        def capture_sock_create(
            family,
            type,
            proto=0,
            *,
            flags=0,
            connect_to=None,
            initial_data: bytes | None = None,
        ):
            captured.append(initial_data)
            return real_sock_create(
                family,
                type,
                proto,
                flags=flags,
                connect_to=connect_to,
                initial_data=initial_data,
            )

        monkeypatch.setattr(scheduler.io, "sock_create", capture_sock_create)

        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            server.setblocking(False)
            server.bind(("127.0.0.1", 0))
            server.listen()
            _host, port = server.getsockname()

            def accept_side() -> None:
                conn = scheduler.io.sock_accept(server)
                conn.close()

            def connect_with_initial() -> None:
                _reader, writer = open_connection(
                    addr=("127.0.0.1", port),
                    initial_send=b"early",
                    scheduler=scheduler,
                )
                writer.close()

            connect_task = scheduler.spawn(connect_with_initial)
            scheduler.spawn(accept_side)
            scheduler.run_until_complete(connect_task)
            assert captured == [b"early"]
        finally:
            server.close()

    def test_open_connection_tries_next_address_after_connect_failure(
        self, scheduler: SyncProactorScheduler, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        connect_targets: list[Any] = []
        real_sock_create = scheduler.io.sock_create

        def track_sock_create(
            family,
            type,
            proto=0,
            *,
            flags=0,
            connect_to=None,
            initial_data: bytes | None = None,
        ):
            connect_targets.append(connect_to)
            return real_sock_create(
                family,
                type,
                proto,
                flags=flags,
                connect_to=connect_to,
                initial_data=initial_data,
            )

        monkeypatch.setattr(scheduler.io, "sock_create", track_sock_create)

        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            server.setblocking(False)
            server.bind(("127.0.0.1", 0))
            server.listen()
            _host, port = server.getsockname()

            monkeypatch.setattr(
                scheduler,
                "ensure_resolved",
                lambda *args, **kwargs: [
                    (socket.AF_INET, socket.SOCK_STREAM, 0, "", ("127.0.0.1", 1)),
                    (socket.AF_INET, socket.SOCK_STREAM, 0, "", ("127.0.0.1", port)),
                ],
            )

            def accept_side() -> None:
                conn = scheduler.io.sock_accept(server)
                conn.close()

            def connect_side() -> None:
                _reader, writer = open_connection(addr=("127.0.0.1", port), scheduler=scheduler)
                writer.close()

            connect_task = scheduler.spawn(connect_side)
            scheduler.spawn(accept_side)
            scheduler.run_until_complete(connect_task)
            assert connect_targets == [("127.0.0.1", 1), ("127.0.0.1", port)]
        finally:
            server.close()

    def test_open_connection_unix_passes_initial_send_to_sock_create(
        self, scheduler: SyncProactorScheduler, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured: list[bytes | None] = []
        real_sock_create = scheduler.io.sock_create

        def capture_sock_create(
            family,
            type,
            proto=0,
            *,
            flags=0,
            connect_to=None,
            initial_data: bytes | None = None,
        ):
            captured.append(initial_data)
            return real_sock_create(
                family,
                type,
                proto,
                flags=flags,
                connect_to=connect_to,
                initial_data=initial_data,
            )

        monkeypatch.setattr(scheduler.io, "sock_create", capture_sock_create)

        with tempfile.TemporaryDirectory() as temp_dir:
            path = str(Path(temp_dir) / "stream.sock")
            server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                server.bind(path)
                server.listen()
                server.setblocking(False)

                def accept_side() -> None:
                    conn = scheduler.io.sock_accept(server)
                    conn.close()

                def connect_with_initial() -> None:
                    _reader, writer = open_connection(path=path, initial_send=b"early", scheduler=scheduler)
                    writer.close()

                connect_task = scheduler.spawn(connect_with_initial)
                scheduler.spawn(accept_side)
                scheduler.run_until_complete(connect_task)
                assert captured == [b"early"]
            finally:
                server.close()


class TestStreamsFakeUring:
    def test_async_stream_readexactly_with_fake_uring_ring(self, monkeypatch: pytest.MonkeyPatch):
        _patch_uring_capabilities(monkeypatch, IORING_OP_SEND_ZC=False, IORING_OP_SENDMSG_ZC=False)
        scheduler = _scheduler_with_fake_ring()
        set_scheduler(scheduler)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)

            async def handler() -> bytes:
                stream_reader, _stream_writer = open_streams(reader, async_=True)
                return await stream_reader.readexactly(5)

            def exercise() -> bytes:
                return run_coro(handler())

            # _FakeUringRing completes recv_into synchronously with b"world".
            assert scheduler.run_until_complete(scheduler.spawn(exercise)) == b"world"
        finally:
            reader.close()
            writer.close()
            scheduler.close()

    def test_start_server_closes_connection_on_recv_error(self, monkeypatch: pytest.MonkeyPatch):
        _patch_uring_capabilities(monkeypatch, IORING_ACCEPT_MULTISHOT=True)
        scheduler = _scheduler_with_fake_ring()
        set_scheduler(scheduler)
        handled = Event()

        def client_handler(reader: StreamReader, writer: StreamWriter) -> None:
            handled.set()
            writer.close()

        server = start_server(client_handler, addr=("127.0.0.1", 0), recv_size=64, scheduler=scheduler)
        try:
            proactor = scheduler.proactor
            proactor.ring.complete_accept_multishot("peer-1")
            proactor.wait(proactor.get_time() + 0.05)
            proactor.ring.complete_accept_recv_error(-errno.EIO)
            proactor.wait(proactor.get_time() + 0.05)
            assert handled.is_set() is False
            assert server._active_handlers == 0
        finally:
            server.close()
            scheduler.close()

    def test_start_server_prefills_reader_from_accept_preread(self, monkeypatch: pytest.MonkeyPatch):
        _patch_uring_capabilities(monkeypatch, IORING_ACCEPT_MULTISHOT=True)
        scheduler = _scheduler_with_fake_ring()
        set_scheduler(scheduler)
        handled = Event()
        received: list[bytes] = []

        def client_handler(reader: StreamReader, writer: StreamWriter) -> None:
            received.append(reader.read(5))
            writer.close()
            handled.set()

        server = start_server(client_handler, addr=("127.0.0.1", 0), recv_size=64, scheduler=scheduler)
        try:
            proactor = scheduler.proactor

            def exercise() -> None:
                proactor.ring.complete_accept_multishot("peer-1")
                proactor.wait(proactor.get_time() + 0.05)
                proactor.ring.complete_accept_recv(b"early")
                handled.swait()

            scheduler.run_until_complete(scheduler.spawn(exercise))
            assert received == [b"early"]
        finally:
            server.close()
            scheduler.close()


def test_run_coro_rejects_real_yields() -> None:
    loop = asyncio.new_event_loop()
    try:

        async def waits_on_future() -> None:
            await loop.create_future()

        with pytest.raises(RuntimeError, match="yielded unexpectedly"):
            run_coro(waits_on_future())
    finally:
        loop.close()


def test_default_reuse_address_matches_asyncio() -> None:
    import os
    import sys

    expected = os.name == "posix" and sys.platform != "cygwin"
    assert _default_reuse_address() is expected


@pytest.mark.parametrize("scheduler_factory", SCHEDULER_INTEGRATION_FACTORIES)
class TestStartServerListenOptions:
    @pytest.fixture
    def scheduler(self, scheduler_factory) -> SyncProactorScheduler:
        sched = scheduler_factory()
        set_scheduler(sched)
        yield sched
        sched.close()

    def test_start_server_rejects_sock_with_addr(self, scheduler: SyncProactorScheduler) -> None:
        def client_handler(reader: StreamReader, writer: StreamWriter) -> None:
            writer.close()

        def exercise() -> None:
            listen_sock, _, _ = scheduler.io.sock_create(socket.AF_INET, socket.SOCK_STREAM)
            listen_sock.bind(("127.0.0.1", 0))
            try:
                with pytest.raises(ValueError, match="addr/path and sock cannot be specified"):
                    start_server(
                        client_handler,
                        addr=("127.0.0.1", 0),
                        sock=listen_sock,
                        scheduler=scheduler,
                    )
            finally:
                listen_sock.close()

        run_scheduler_task(scheduler, exercise)

    def test_start_server_accepts_prebound_sock(self, scheduler: SyncProactorScheduler) -> None:
        def client_handler(reader: StreamReader, writer: StreamWriter) -> None:
            writer.close()

        def exercise() -> None:
            listen_sock, _, _ = scheduler.io.sock_create(socket.AF_INET, socket.SOCK_STREAM)
            listen_sock.bind(("127.0.0.1", 0))
            port = listen_sock.getsockname()[1]
            server = start_server(client_handler, sock=listen_sock, scheduler=scheduler)
            try:
                assert server.sockets[0].fileno() == listen_sock.fileno()
                assert listen_sock.getsockname() == ("127.0.0.1", port)
                assert server.accept_operation.kind == "accept_many"
            finally:
                server.close()

        run_scheduler_task(scheduler, exercise)

    def test_start_server_honours_reuse_address_flag(
        self, scheduler: SyncProactorScheduler, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import tealetio.streams as streams_module

        captured: list[bool | None] = []
        real_bind = streams_module._bind_tcp_socket

        def capture_bind(io, addr, **kwargs):
            captured.append(kwargs.get("reuse_address"))
            return real_bind(io, addr, **kwargs)

        monkeypatch.setattr(streams_module, "_bind_tcp_socket", capture_bind)

        def client_handler(reader: StreamReader, writer: StreamWriter) -> None:
            writer.close()

        def exercise_without_reuse() -> None:
            server = start_server(
                client_handler,
                addr=("127.0.0.1", 0),
                reuse_address=False,
                scheduler=scheduler,
            )
            server.close()

        run_scheduler_task(scheduler, exercise_without_reuse)
        assert captured == [False]

        def exercise_with_default_reuse() -> None:
            server = start_server(client_handler, addr=("127.0.0.1", 0), scheduler=scheduler)
            server.close()

        run_scheduler_task(scheduler, exercise_with_default_reuse)
        assert captured[-1] is None


class TestStreamsRequiresIO:
    def test_open_streams_without_io_backend(self):
        from tealetio.scheduler import BasicScheduler

        scheduler = BasicScheduler()
        reader, _writer = socket.socketpair()
        try:
            with pytest.raises(RuntimeError, match="scheduler with IO support"):
                open_streams(reader, scheduler=scheduler)
        finally:
            reader.close()
            _writer.close()

    def test_open_connection_without_io_backend(self):
        from tealetio.scheduler import BasicScheduler

        scheduler = BasicScheduler()
        with pytest.raises(RuntimeError, match="scheduler with IO support"):
            open_connection(addr=("127.0.0.1", 8080), scheduler=scheduler)

    def test_start_server_without_io_backend(self):
        from tealetio.scheduler import BasicScheduler

        scheduler = BasicScheduler()

        def client_handler(_reader: StreamReader, _writer: StreamWriter) -> None:
            return None

        with pytest.raises(RuntimeError, match="scheduler with IO support"):
            start_server(client_handler, addr=("127.0.0.1", 0), scheduler=scheduler)

    def test_open_connection_selector_scheduler_raises_until_selector_io_manager(self):
        from tealetio.selector import SyncSelectorScheduler

        scheduler = SyncSelectorScheduler()
        try:
            with pytest.raises(RuntimeError, match="stream helpers require a proactor scheduler"):
                open_connection(addr=("127.0.0.1", 0), scheduler=scheduler)
        finally:
            scheduler.close()

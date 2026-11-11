from __future__ import annotations

import socket
import threading

import pytest

from tealetio import Event, set_scheduler
from tealetio.connections import (
    DEFAULT_CONNECTION_RECV_SIZE,
    Connection,
    RecvIntoBufferCache,
    start_connection_server,
    start_connection_stream_server,
)
from tealetio.proactor import SyncProactorScheduler
from tealetio.streams import open_connection
from uring_fakes import SCHEDULER_INTEGRATION_FACTORIES, run_scheduler_task


@pytest.mark.parametrize("scheduler_factory", SCHEDULER_INTEGRATION_FACTORIES)
class TestConnectionServer:
    @pytest.fixture
    def scheduler(self, scheduler_factory) -> SyncProactorScheduler:
        sched = scheduler_factory()
        set_scheduler(sched)
        yield sched
        sched.close()

    def test_recv_callback_gets_payload(self, scheduler: SyncProactorScheduler) -> None:
        got: list[bytes] = []
        done = Event()

        def on_conn(conn: Connection) -> None:
            def on_data(_conn: Connection, data: memoryview | None, exc: BaseException | None) -> None:
                copied = None if data is None else bytes(data)

                def deliver() -> None:
                    if exc is not None:
                        done.set()
                        raise exc
                    assert copied is not None
                    got.append(copied)
                    _conn.close()
                    done.set()

                scheduler.call_on_scheduler(deliver)

            conn.set_recv_callback(on_data)

        def exercise() -> None:
            server = start_connection_server(on_conn, addr=("127.0.0.1", 0), scheduler=scheduler)
            try:
                _host, port = server.sockets[0].getsockname()

                def client() -> None:
                    _reader, writer = open_connection(addr=("127.0.0.1", port))
                    writer.write(b"abc")
                    writer.drain()
                    done.swait()
                    writer.close()

                scheduler.spawn(client)
                done.swait()
            finally:
                server.close()
                server.wait_closed()

        run_scheduler_task(scheduler, exercise)
        assert got == [b"abc"]

    def test_open_streams_feeds_presend(self, scheduler: SyncProactorScheduler) -> None:
        payload = b"presend-payload"
        received: list[bytes] = []
        handled = Event()

        def on_conn(conn: Connection) -> None:
            def serve() -> None:
                reader, writer = conn.to_streams()
                try:
                    received.append(reader.read(len(payload)))
                finally:
                    writer.close()
                    handled.set()

            scheduler.call_on_scheduler(lambda: scheduler.spawn(serve))

        def exercise() -> None:
            server = start_connection_server(on_conn, addr=("127.0.0.1", 0), scheduler=scheduler)
            try:
                _host, port = server.sockets[0].getsockname()

                def client() -> None:
                    _reader, writer = open_connection(addr=("127.0.0.1", port))
                    writer.write(payload)
                    writer.drain()
                    handled.swait()
                    writer.close()

                scheduler.spawn(client)
                handled.swait()
            finally:
                server.close()
                server.wait_closed()

        run_scheduler_task(scheduler, exercise)
        assert received == [payload]

    def test_open_streams_while_recv_in_flight(self, scheduler: SyncProactorScheduler) -> None:
        parked = Event()
        received: list[bytes] = []
        handled = Event()

        def on_conn(conn: Connection) -> None:
            def serve() -> None:
                reader, writer = conn.to_streams()
                parked.set()
                try:
                    received.append(reader.readline())
                finally:
                    writer.close()
                    handled.set()

            scheduler.call_on_scheduler(lambda: scheduler.spawn(serve))

        def exercise() -> None:
            server = start_connection_server(on_conn, addr=("127.0.0.1", 0), scheduler=scheduler)
            try:
                _host, port = server.sockets[0].getsockname()

                def client() -> None:
                    _reader, writer = open_connection(addr=("127.0.0.1", port))
                    parked.swait()
                    writer.write(b"hello\n")
                    writer.drain()
                    handled.swait()
                    writer.close()

                scheduler.spawn(client)
                handled.swait()
            finally:
                server.close()
                server.wait_closed()

        run_scheduler_task(scheduler, exercise)
        assert received == [b"hello\n"]


    def test_send_nowait_from_accept_callback(self, scheduler: SyncProactorScheduler) -> None:
        sent = b"hello-from-server"
        received: list[bytes] = []
        conns: list[Connection] = []
        done = Event()

        def on_conn(conn: Connection) -> None:
            conns.append(conn)
            conn.send_nowait(sent)

        def exercise() -> None:
            server = start_connection_server(on_conn, addr=("127.0.0.1", 0), scheduler=scheduler)
            try:
                _host, port = server.sockets[0].getsockname()

                def client() -> None:
                    reader, writer = open_connection(addr=("127.0.0.1", port))
                    received.append(reader.read(len(sent)))
                    done.set()
                    writer.close()

                scheduler.spawn(client)
                done.swait()
            finally:
                for conn in conns:
                    conn.close()
                server.close()
                server.wait_closed()

        run_scheduler_task(scheduler, exercise)
        assert received == [sent]

    def test_send_close_nowait_from_accept_callback(self, scheduler: SyncProactorScheduler) -> None:
        sent = b"hello-from-server"
        received: list[bytes] = []
        done = Event()

        def on_conn(conn: Connection) -> None:
            conn.send_close_nowait(sent)

        def exercise() -> None:
            server = start_connection_server(on_conn, addr=("127.0.0.1", 0), scheduler=scheduler)
            try:
                _host, port = server.sockets[0].getsockname()

                def client() -> None:
                    reader, writer = open_connection(addr=("127.0.0.1", port))
                    received.append(reader.read(len(sent)))
                    done.set()
                    writer.close()

                scheduler.spawn(client)
                done.swait()
            finally:
                server.close()
                server.wait_closed()

        run_scheduler_task(scheduler, exercise)
        assert received == [sent]

    def test_recv_size_default_and_override(self, scheduler: SyncProactorScheduler) -> None:
        got: list[bytes] = []
        done = Event()

        def on_conn(conn: Connection) -> None:
            def on_data(_conn: Connection, data: memoryview | None, exc: BaseException | None) -> None:
                copied = None if data is None else bytes(data)

                def deliver() -> None:
                    if exc is not None:
                        done.set()
                        raise exc
                    assert copied is not None
                    got.append(copied)
                    _conn.close()
                    done.set()

                scheduler.call_on_scheduler(deliver)

            conn.set_recv_callback(on_data)

        def exercise() -> None:
            server = start_connection_server(
                on_conn,
                addr=("127.0.0.1", 0),
                recv_size=8,
                scheduler=scheduler,
            )
            try:
                assert server.recv_size == 8
                _host, port = server.sockets[0].getsockname()

                def client() -> None:
                    _reader, writer = open_connection(addr=("127.0.0.1", port))
                    writer.write(b"abcdefghij")
                    writer.drain()
                    done.swait()
                    writer.close()

                scheduler.spawn(client)
                done.swait()
            finally:
                server.close()
                server.wait_closed()

        run_scheduler_task(scheduler, exercise)
        assert got == [b"abcdefgh"]

    def test_recv_size_must_be_positive(self, scheduler: SyncProactorScheduler) -> None:
        def on_conn(conn: Connection) -> None:
            conn.close()

        def exercise() -> None:
            with pytest.raises(ValueError, match="recv_size must be positive"):
                start_connection_server(
                    on_conn,
                    addr=("127.0.0.1", 0),
                    recv_size=0,
                    scheduler=scheduler,
                )

        run_scheduler_task(scheduler, exercise)
        assert DEFAULT_CONNECTION_RECV_SIZE == 64 * 1024

    def test_accept_callback_failure_closes_connection(self, scheduler: SyncProactorScheduler) -> None:
        held: list[Connection] = []
        errors: list[BaseException] = []
        done = Event()

        def on_error(context: dict[str, object]) -> None:
            exc = context["exception"]
            assert isinstance(exc, BaseException)
            errors.append(exc)
            done.set()

        def on_conn(conn: Connection) -> None:
            held.append(conn)
            raise RuntimeError("accept callback failed")

        def exercise() -> None:
            scheduler.set_exception_handler(on_error)
            server = start_connection_server(on_conn, addr=("127.0.0.1", 0), scheduler=scheduler)
            try:
                _host, port = server.sockets[0].getsockname()

                def client() -> None:
                    _reader, writer = open_connection(addr=("127.0.0.1", port))
                    done.swait()
                    writer.close()

                scheduler.spawn(client)
                done.swait()
            finally:
                server.close()
                server.wait_closed()

        run_scheduler_task(scheduler, exercise)
        assert len(held) == 1
        assert held[0].closed
        assert len(errors) == 1
        assert isinstance(errors[0], RuntimeError)
        assert str(errors[0]) == "accept callback failed"

    def test_connection_stream_server_roundtrip(self, scheduler: SyncProactorScheduler) -> None:
        payload = b"GET / HTTP/1.1\r\nHost: x\r\n\r\n"
        reply = b"ok"
        received: list[bytes] = []
        done = Event()

        def client_handler(reader, writer) -> None:  # noqa: ANN001
            try:
                assert reader.readexactly(len(payload)) == payload
                writer.write(reply)
                writer.drain()
            finally:
                writer.close()

        def exercise() -> None:
            server = start_connection_stream_server(
                client_handler,
                addr=("127.0.0.1", 0),
                scheduler=scheduler,
            )
            try:
                _host, port = server.sockets[0].getsockname()

                def client() -> None:
                    reader, writer = open_connection(addr=("127.0.0.1", port))
                    writer.write(payload)
                    writer.drain()
                    received.append(reader.readexactly(len(reply)))
                    writer.close()
                    done.set()

                scheduler.spawn(client)
                done.swait()
            finally:
                server.close()
                server.wait_closed()

        run_scheduler_task(scheduler, exercise)
        assert received == [reply]

    def test_set_recv_callback_races_completion(self, scheduler: SyncProactorScheduler) -> None:
        def exercise() -> None:
            for _ in range(40):
                left, right = socket.socketpair()
                left.setblocking(False)
                pool = RecvIntoBufferCache(buffer_size=64)
                buf = pool.acquire()
                conn = Connection(scheduler.io, left, buf, pool=pool)
                conn._recv_handle = object()
                got: list[bytes] = []
                barrier = threading.Barrier(2)

                def complete() -> None:
                    barrier.wait()
                    conn._complete_recv(4, None)

                def bind() -> None:
                    barrier.wait()

                    def on_data(_conn: Connection, data: memoryview | None, exc: BaseException | None) -> None:
                        assert exc is None
                        assert data is not None
                        got.append(bytes(data))

                    conn.set_recv_callback(on_data)

                threads = [threading.Thread(target=complete), threading.Thread(target=bind)]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join()
                assert got == [b"\x00\x00\x00\x00"]
                conn.close()
                right.close()

        run_scheduler_task(scheduler, exercise)

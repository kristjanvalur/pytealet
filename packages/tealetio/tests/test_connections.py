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

                scheduler.call_soon_threadsafe(deliver, immediate=True)

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
                reader, writer = conn.open_streams()
                try:
                    received.append(reader.read(len(payload)))
                finally:
                    writer.close()
                    handled.set()

            scheduler.spawn(serve)

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
                reader, writer = conn.open_streams()
                parked.set()
                try:
                    received.append(reader.readline())
                finally:
                    writer.close()
                    handled.set()

            scheduler.spawn(serve)

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

    def test_open_streams_twice_raises(self, scheduler: SyncProactorScheduler) -> None:
        seen: list[str] = []
        done = Event()

        def on_conn(conn: Connection) -> None:
            conn.open_streams()
            try:
                conn.open_streams()
            except RuntimeError as exc:
                seen.append(str(exc))
            conn.close()
            done.set()

        def exercise() -> None:
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
        assert seen == ["streams already opened"]

    def test_set_recv_callback_then_open_streams_raises(self, scheduler: SyncProactorScheduler) -> None:
        seen: list[str] = []
        done = Event()

        def on_conn(conn: Connection) -> None:
            conn.set_recv_callback(lambda *_args: None)
            try:
                conn.open_streams()
            except RuntimeError as exc:
                seen.append(str(exc))
            conn.close()
            done.set()

        def exercise() -> None:
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
        assert seen == ["recv callback already bound"]

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

                scheduler.call_soon_threadsafe(deliver, immediate=True)

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

    def test_transform_runs_on_delivery_before_callback(self, scheduler: SyncProactorScheduler) -> None:
        delivered: list[str] = []
        order: list[str] = []
        callback_ran = False
        done = Event()

        class _Tag:
            def __init__(self, conn: Connection) -> None:
                self.conn = conn

            def close(self) -> None:
                self.conn.close()

        def transform(conn: Connection) -> _Tag:
            assert not callback_ran
            order.append("transform")
            order.append(f"transform-thread:{threading.get_ident()}")
            return _Tag(conn)

        def on_tag(tag: _Tag) -> None:
            nonlocal callback_ran
            callback_ran = True
            order.append("callback")
            order.append(f"callback-thread:{threading.get_ident()}")
            delivered.append(tag.conn.__class__.__name__)
            tag.close()
            done.set()

        def exercise() -> None:
            server = start_connection_server(
                on_tag,
                transform=transform,
                addr=("127.0.0.1", 0),
                scheduler=scheduler,
            )
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
        assert delivered == ["Connection"]
        assert order[0] == "transform"
        assert "callback" in order
        assert order.index("transform") < order.index("callback")

    def test_transform_failure_closes_connection(self, scheduler: SyncProactorScheduler) -> None:
        held: list[Connection] = []
        errors: list[BaseException] = []
        done = Event()

        def on_error(context: dict[str, object]) -> None:
            exc = context["exception"]
            assert isinstance(exc, BaseException)
            errors.append(exc)
            done.set()

        def transform(conn: Connection) -> Connection:
            held.append(conn)
            raise RuntimeError("transform failed")

        def on_conn(_conn: Connection) -> None:
            raise AssertionError("callback should not run")

        def exercise() -> None:
            scheduler.set_exception_handler(on_error)
            server = start_connection_server(
                on_conn,
                transform=transform,
                addr=("127.0.0.1", 0),
                scheduler=scheduler,
            )
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
        assert str(errors[0]) == "transform failed"

    def test_transform_open_streams(self, scheduler: SyncProactorScheduler) -> None:
        payload = b"from-transform\n"
        received: list[bytes] = []
        done = Event()

        class _Pair:
            def __init__(self, reader, writer) -> None:  # noqa: ANN001
                self.reader = reader
                self.writer = writer

            def close(self) -> None:
                self.writer.close()

        def transform(conn: Connection) -> _Pair:
            reader, writer = conn.open_streams()
            return _Pair(reader, writer)

        def on_pair(pair: _Pair) -> None:
            def serve() -> None:
                try:
                    received.append(pair.reader.readline())
                finally:
                    pair.close()
                    done.set()

            scheduler.spawn(serve)

        def exercise() -> None:
            server = start_connection_server(
                on_pair,
                transform=transform,
                addr=("127.0.0.1", 0),
                scheduler=scheduler,
            )
            try:
                _host, port = server.sockets[0].getsockname()

                def client() -> None:
                    _reader, writer = open_connection(addr=("127.0.0.1", port))
                    writer.write(payload)
                    writer.drain()
                    done.swait()
                    writer.close()

                scheduler.spawn(client)
                done.swait()
            finally:
                server.close()
                server.wait_closed()

        run_scheduler_task(scheduler, exercise)
        assert received == [payload]

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

from __future__ import annotations

import pytest

from tealetio import Event, set_scheduler
from tealetio.connections import Connection, start_connection_server
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
                if exc is not None:
                    done.set()
                    raise exc
                assert data is not None
                got.append(bytes(data))
                _conn.close()
                done.set()

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

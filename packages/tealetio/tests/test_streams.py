from __future__ import annotations

import asyncio
import socket

import pytest

from tealetio import set_scheduler
from tealetio.proactor import SyncProactorScheduler, UringProactor
from tealetio.streams import (
    AsyncStreamReader,
    AsyncStreamWriter,
    SocketTransport,
    StreamReader,
    StreamWriter,
    open_async_connection,
    open_async_streams,
    open_connection,
    open_streams,
    run_coro,
)
from test_proactor import _FakeUringRing


def _scheduler_with_fake_ring() -> SyncProactorScheduler:
    return SyncProactorScheduler(lambda: UringProactor(ring_factory=_FakeUringRing))


class TestStreamsPoC:
    def test_async_stream_readexactly_with_selector_proactor(self):
        scheduler = SyncProactorScheduler()
        set_scheduler(scheduler)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(True)

            async def handler() -> bytes:
                stream_reader, _stream_writer = open_async_streams(scheduler, reader)
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
            scheduler.close()

    def test_async_stream_write_drain_and_readline(self):
        scheduler = SyncProactorScheduler()
        set_scheduler(scheduler)
        client, server = socket.socketpair()
        try:
            client.setblocking(False)
            server.setblocking(False)

            async def echo_handler() -> bytes:
                stream_reader, stream_writer = open_async_streams(scheduler, server)
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
            scheduler.close()

    def test_native_stream_read_and_write(self):
        scheduler = SyncProactorScheduler()
        set_scheduler(scheduler)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(True)

            def handler() -> bytes:
                stream_reader, stream_writer = open_streams(scheduler, reader)
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
            scheduler.close()

    def test_async_stream_readexactly_with_fake_uring_ring(self):
        scheduler = _scheduler_with_fake_ring()
        set_scheduler(scheduler)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)

            async def handler() -> bytes:
                stream_reader, _stream_writer = open_async_streams(scheduler, reader)
                return await stream_reader.readexactly(5)

            def exercise() -> bytes:
                return run_coro(handler())

            # _FakeUringRing completes recv_into synchronously with b"world".
            assert scheduler.run_until_complete(scheduler.spawn(exercise)) == b"world"
        finally:
            reader.close()
            writer.close()
            scheduler.close()

    def test_open_streams_uses_custom_stream_factory(self):
        scheduler = SyncProactorScheduler()
        set_scheduler(scheduler)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)

            class TaggedStreamReader(StreamReader):
                tag = "native-custom"

            def custom_factory(scheduler, sock, *, limit):
                transport = SocketTransport(scheduler, sock)
                stream_reader = TaggedStreamReader(transport, limit=limit)
                stream_writer = StreamWriter(transport, stream_reader)
                return stream_reader, stream_writer

            stream_reader, _stream_writer = open_streams(
                scheduler,
                reader,
                stream_factory=custom_factory,
            )
            assert isinstance(stream_reader, TaggedStreamReader)
            assert stream_reader.tag == "native-custom"
        finally:
            reader.close()
            writer.close()
            scheduler.close()

    def test_open_async_streams_uses_custom_stream_factory(self):
        scheduler = SyncProactorScheduler()
        set_scheduler(scheduler)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)

            class TaggedAsyncStreamReader(AsyncStreamReader):
                tag = "async-custom"

            def custom_factory(scheduler, sock, *, limit):
                transport = SocketTransport(scheduler, sock)
                stream_reader = TaggedAsyncStreamReader(transport, limit=limit)
                stream_writer = AsyncStreamWriter(transport, stream_reader)
                return stream_reader, stream_writer

            stream_reader, _stream_writer = open_async_streams(
                scheduler,
                reader,
                stream_factory=custom_factory,
            )
            assert isinstance(stream_reader, TaggedAsyncStreamReader)
            assert stream_reader.tag == "async-custom"
        finally:
            reader.close()
            writer.close()
            scheduler.close()

    def test_open_connection_resolves_literal_ip_inside_scheduler(self):
        scheduler = SyncProactorScheduler()
        set_scheduler(scheduler)
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_greeting = b"hello"
        try:
            server.setblocking(False)
            server.bind(("127.0.0.1", 0))
            server.listen()
            _host, port = server.getsockname()

            def accept_and_echo() -> None:
                conn, _address = scheduler.sock_accept(server)
                try:
                    payload = scheduler.sock_recv(conn, len(client_greeting))
                    scheduler.sock_sendall(conn, payload.upper())
                finally:
                    conn.close()

            def connect_via_streams() -> bytes:
                stream_reader, stream_writer = open_connection(scheduler, "127.0.0.1", port)
                stream_writer.write(client_greeting)
                stream_writer.drain()
                return stream_reader.readexactly(len(client_greeting))

            connect_task = scheduler.spawn(connect_via_streams)
            scheduler.spawn(accept_and_echo)
            assert scheduler.run_until_complete(connect_task) == b"HELLO"
        finally:
            server.close()
            scheduler.close()

    def test_open_async_connection_resolves_literal_ip_inside_scheduler(self):
        scheduler = SyncProactorScheduler()
        set_scheduler(scheduler)
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            server.setblocking(False)
            server.bind(("127.0.0.1", 0))
            server.listen()
            _host, port = server.getsockname()

            async def connect_and_ping() -> bytes:
                stream_reader, stream_writer = open_async_connection(scheduler, "127.0.0.1", port)
                stream_writer.write(b"ping")
                await stream_writer.drain()
                return await stream_reader.readexactly(4)

            def accept_side() -> None:
                conn, _address = scheduler.sock_accept(server)
                try:
                    scheduler.sock_sendall(conn, b"pong")
                finally:
                    conn.close()

            def exercise() -> bytes:
                scheduler.spawn(accept_side)
                return run_coro(connect_and_ping())

            assert scheduler.run_until_complete(scheduler.spawn(exercise)) == b"pong"
        finally:
            server.close()
            scheduler.close()

    def test_stream_readinto_fills_caller_buffer(self):
        scheduler = SyncProactorScheduler()
        set_scheduler(scheduler)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(True)

            def handler() -> tuple[int, bytes]:
                stream_reader, _stream_writer = open_streams(scheduler, reader)
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
            scheduler.close()

    def test_stream_reads_use_recv_into_not_sock_recv(self, monkeypatch):
        scheduler = SyncProactorScheduler()
        set_scheduler(scheduler)
        reader, writer = socket.socketpair()
        recv_calls: list[tuple[object, ...]] = []
        recv_into_calls: list[tuple[object, ...]] = []
        real_recv = scheduler.sock_recv
        real_recv_into = scheduler.sock_recv_into

        def tracking_recv(sock, n):
            recv_calls.append((sock, n))
            return real_recv(sock, n)

        def tracking_recv_into(sock, buf):
            recv_into_calls.append((sock, memoryview(buf).nbytes))
            return real_recv_into(sock, buf)

        monkeypatch.setattr(scheduler, "sock_recv", tracking_recv)
        monkeypatch.setattr(scheduler, "sock_recv_into", tracking_recv_into)
        try:
            reader.setblocking(False)
            writer.setblocking(True)

            def handler() -> bytes:
                stream_reader, _stream_writer = open_streams(scheduler, reader)
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
            scheduler.close()

    def test_run_coro_rejects_real_yields(self):
        loop = asyncio.new_event_loop()
        try:

            async def waits_on_future() -> None:
                await loop.create_future()

            with pytest.raises(RuntimeError, match="yielded unexpectedly"):
                run_coro(waits_on_future())
        finally:
            loop.close()
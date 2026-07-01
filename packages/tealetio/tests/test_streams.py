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
    open_async_streams,
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

            def deliver() -> None:
                ring = scheduler.proactor.ring
                assert isinstance(ring, _FakeUringRing)
                ring.complete_recv_multishot(b"hello", more=False, sequence=0)

            def exercise() -> bytes:
                scheduler.spawn(deliver)
                return run_coro(handler())

            assert scheduler.run_until_complete(scheduler.spawn(exercise)) == b"hello"
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

    def test_run_coro_rejects_real_yields(self):
        loop = asyncio.new_event_loop()
        try:

            async def waits_on_future() -> None:
                await loop.create_future()

            with pytest.raises(RuntimeError, match="yielded unexpectedly"):
                run_coro(waits_on_future())
        finally:
            loop.close()
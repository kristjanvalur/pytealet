from __future__ import annotations

import asyncio
import socket
import threading
import time

import pytest

from uring_asyncio import UringProactorEventLoop, run

from conftest import require_uring


def _close_loop(loop: asyncio.AbstractEventLoop) -> None:
    if not loop.is_closed():
        loop.close()


def test_sock_sendall_and_recv_roundtrip() -> None:
    require_uring()
    loop = UringProactorEventLoop()
    a = b = None
    try:
        a, b = socket.socketpair()
        a.setblocking(False)
        b.setblocking(False)

        async def _exercise() -> None:
            await loop.sock_sendall(a, b"hello")
            data = await loop.sock_recv(b, 5)
            assert data == b"hello"

        loop.run_until_complete(_exercise())
    finally:
        if a is not None:
            a.close()
        if b is not None:
            b.close()
        _close_loop(loop)


def test_start_server_echo() -> None:
    require_uring()
    loop = UringProactorEventLoop()
    try:

        async def _handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            data = await reader.read(16)
            writer.write(data)
            await writer.drain()
            writer.close()
            await writer.wait_closed()

        async def _exercise() -> None:
            server = await asyncio.start_server(_handle, "127.0.0.1", 0)
            port = server.sockets[0].getsockname()[1]
            async with server:
                reader, writer = await asyncio.open_connection("127.0.0.1", port)
                writer.write(b"ping")
                await writer.drain()
                assert await reader.readexactly(4) == b"ping"
                writer.close()
                await writer.wait_closed()

        loop.run_until_complete(_exercise())
    finally:
        _close_loop(loop)


def test_cancel_pending_recv() -> None:
    require_uring()
    loop = UringProactorEventLoop()
    try:
        a, b = socket.socketpair()
        a.setblocking(False)
        b.setblocking(False)

        async def _exercise() -> None:
            task = asyncio.create_task(loop.sock_recv(b, 16))
            await asyncio.sleep(0)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

        loop.run_until_complete(_exercise())
    finally:
        a.close()
        b.close()
        _close_loop(loop)


def test_call_soon_threadsafe_wakes_loop() -> None:
    require_uring()
    loop = UringProactorEventLoop()
    try:
        seen = []

        async def _exercise() -> None:
            ready = asyncio.Event()

            def _from_thread() -> None:
                seen.append("wake")
                ready.set()

            threading.Thread(target=lambda: (time.sleep(0.05), loop.call_soon_threadsafe(_from_thread))).start()
            await asyncio.wait_for(ready.wait(), timeout=2.0)
            assert seen == ["wake"]

        loop.run_until_complete(_exercise())
    finally:
        _close_loop(loop)


def test_run_helper_sleeps() -> None:
    require_uring()

    async def _main() -> int:
        await asyncio.sleep(0)
        return 7

    assert run(_main()) == 7


def test_udp_sendto_recvfrom() -> None:
    require_uring()
    loop = UringProactorEventLoop()
    try:
        receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        receiver.bind(("127.0.0.1", 0))
        receiver.setblocking(False)
        sender.setblocking(False)
        addr = receiver.getsockname()

        async def _exercise() -> None:
            n = await loop.sock_sendto(sender, b"udp!", addr)
            assert n == 4
            data, peer = await loop.sock_recvfrom(receiver, 16)
            assert data == b"udp!"
            assert peer[0] == "127.0.0.1"

        loop.run_until_complete(_exercise())
    finally:
        receiver.close()
        sender.close()
        _close_loop(loop)


def test_sock_sendall_drains_buffer() -> None:
    require_uring()
    loop = UringProactorEventLoop()
    a = b = None
    try:
        a, b = socket.socketpair()
        a.setblocking(False)
        b.setblocking(False)
        payload = b"x" * 4096

        async def _exercise() -> None:
            sent = await loop.sock_sendall(a, payload)
            assert sent in (None, len(payload))
            received = bytearray()
            while len(received) < len(payload):
                received.extend(await loop.sock_recv(b, len(payload) - len(received)))
            assert bytes(received) == payload

        loop.run_until_complete(_exercise())
    finally:
        if a is not None:
            a.close()
        if b is not None:
            b.close()
        _close_loop(loop)

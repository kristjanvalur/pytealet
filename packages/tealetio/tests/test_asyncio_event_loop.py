from __future__ import annotations

import asyncio
import socket

from tealetio import run
from tealetio.asyncio import ForwardingProactor, TealetProactorEventLoop
from tealetio.operations import Operation
from tealetio.proactor import SyncProactorScheduler


class _SendTrackingProactor:
    def send(
        self,
        sock: socket.socket,
        data: bytes | bytearray | memoryview,
        progress: object = None,
    ) -> Operation[int]:
        del sock, progress
        self.last_send = bytes(data)
        operation = Operation[int](kind="send", fileobj=None)
        operation._finish(result=len(self.last_send))
        return operation


def test_forwarding_proactor_send_drains_buffer() -> None:
    backend = _SendTrackingProactor()
    forwarding = ForwardingProactor(backend)  # type: ignore[arg-type]
    loop = asyncio.new_event_loop()
    forwarding.set_loop(loop)
    sock = socket.socket()
    try:
        future = forwarding.send(sock, b"hello")
        assert loop.run_until_complete(asyncio.wrap_future(future)) == 5
        assert backend.last_send == b"hello"
    finally:
        sock.close()
        loop.close()


def test_forwarding_proactor_send_loops_on_short_proactor_send() -> None:
    """Asyncio sendall semantics: re-arm remainder when proactor.send completes short."""

    class _ShortThenFullProactor:
        def __init__(self) -> None:
            self.sends: list[bytes] = []
            self._cancel_targets: list[Operation[int]] = []

        def send(
            self,
            sock: socket.socket,
            data: bytes | bytearray | memoryview,
            progress: object = None,
        ) -> Operation[int]:
            del sock, progress
            cargo = bytes(data)
            self.sends.append(cargo)
            operation = Operation[int](kind="send", fileobj=None)
            if len(self.sends) == 1 and len(cargo) > 2:
                operation._finish(result=2)
            else:
                operation._finish(result=len(cargo))
            return operation

        def cancel(self, operation: Operation[int]) -> Operation[None]:
            self._cancel_targets.append(operation)
            cancel_op = Operation[None](kind="cancel", fileobj=None)
            cancel_op._finish(result=None)
            return cancel_op

    backend = _ShortThenFullProactor()
    forwarding = ForwardingProactor(backend)  # type: ignore[arg-type]
    loop = asyncio.new_event_loop()
    forwarding.set_loop(loop)
    sock = socket.socket()
    try:
        future = forwarding.send(sock, b"hello")
        assert loop.run_until_complete(asyncio.wrap_future(future)) == 5
        assert backend.sends == [b"hello", b"llo"]
    finally:
        sock.close()
        loop.close()


class _AcceptProactor:
    def accept(self, sock: socket.socket) -> Operation[socket.socket]:
        _listener, conn = socket.socketpair()
        operation = Operation[socket.socket](kind="accept", fileobj=sock)
        operation._finish(result=conn)
        return operation


def test_forwarding_proactor_accept_returns_conn_and_peername() -> None:
    backend = _AcceptProactor()
    forwarding = ForwardingProactor(backend)  # type: ignore[arg-type]
    loop = asyncio.new_event_loop()
    forwarding.set_loop(loop)
    listener = socket.socket()
    try:
        conn, peername = loop.run_until_complete(asyncio.wrap_future(forwarding.accept(listener)))
        assert isinstance(conn, socket.socket)
        assert peername is None or isinstance(peername, (tuple, str))
    finally:
        listener.close()
        loop.close()


def test_tealet_proactor_event_loop_start_server_accepts() -> None:
    def exercise() -> None:
        loop = TealetProactorEventLoop()
        try:
            asyncio.set_event_loop(loop)

            async def _handle_client(
                reader: asyncio.StreamReader,
                writer: asyncio.StreamWriter,
            ) -> None:
                writer.close()

            async def _serve_once() -> None:
                server = await asyncio.start_server(_handle_client, "127.0.0.1", 0)
                port = server.sockets[0].getsockname()[1]
                async with server:
                    _reader, writer = await asyncio.wait_for(
                        asyncio.open_connection("127.0.0.1", port),
                        timeout=2.0,
                    )
                    writer.close()
                    await writer.wait_closed()

            loop.run_until_complete(_serve_once())
        finally:
            asyncio.set_event_loop(None)
            loop.close()

    run(exercise, scheduler_factory=SyncProactorScheduler)

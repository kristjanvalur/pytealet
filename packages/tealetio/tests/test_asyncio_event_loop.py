from __future__ import annotations

import asyncio
import socket

from tealetio import set_scheduler
from tealetio.asyncio import ForwardingProactor, TealetProactorEventLoop
from tealetio.operations import Operation
from tealetio.proactor import SyncProactorScheduler


def test_tealet_proactor_event_loop_runs_without_self_reading_hook(monkeypatch):
    scheduler = SyncProactorScheduler()
    set_scheduler(scheduler)

    def run_asyncio() -> str:
        monkeypatch.setattr(TealetProactorEventLoop, "_loop_self_reading", None)
        loop = TealetProactorEventLoop()
        try:
            asyncio.set_event_loop(loop)

            async def main() -> str:
                return "done"

            return loop.run_until_complete(main())
        finally:
            asyncio.set_event_loop(None)
            loop.close()

    try:
        assert scheduler.run_until_complete(run_asyncio) == "done"
    finally:
        scheduler.close()


class _ConnectIgnoringInitialProactor:
    def __init__(self) -> None:
        self.sendall_calls: list[tuple[socket.socket, bytes]] = []

    def connect(
        self,
        sock: socket.socket,
        address: object,
        *,
        initial: bytes | None = None,
    ) -> Operation[bool]:
        del address, initial
        operation = Operation[bool](kind="connect", fileobj=sock)
        operation._set_result(False)
        return operation

    def sendall(
        self,
        sock: socket.socket,
        data: bytes | bytearray | memoryview,
        progress: object = None,
    ) -> Operation[None]:
        del progress
        self.sendall_calls.append((sock, bytes(data)))
        operation = Operation[None](kind="sendall", fileobj=sock)
        operation._set_result(None)
        return operation


def test_forwarding_proactor_connect_flushes_initial_when_backend_ignores_hint() -> None:
    backend = _ConnectIgnoringInitialProactor()
    forwarding = ForwardingProactor(backend)  # type: ignore[arg-type]
    loop = asyncio.new_event_loop()
    forwarding.set_loop(loop)
    sock = socket.socket()
    try:
        future = forwarding.connect(sock, ("127.0.0.1", 9), initial=b"hello")
        assert loop.run_until_complete(asyncio.wrap_future(future)) is True
        assert backend.sendall_calls == [(sock, b"hello")]
    finally:
        sock.close()
        loop.close()

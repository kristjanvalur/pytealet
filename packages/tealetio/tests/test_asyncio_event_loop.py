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


class _SendTrackingProactor:
    def send(
        self,
        sock: socket.socket,
        data: bytes | bytearray | memoryview,
        progress: object = None,
    ) -> Operation[None]:
        del sock, progress
        self.last_send = bytes(data)
        operation = Operation[None](kind="send", fileobj=None)
        operation._set_result(None)
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

from __future__ import annotations

import asyncio
import socket

from tealetio.asyncio import ForwardingProactor
from tealetio.operations import Operation


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
        operation._finish(result=None)
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
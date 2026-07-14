"""IO capability protocols for stream construction.

Defined here so ``streams.open`` and stream endpoints do not import
``io_manager``. ``ProactorIOManager`` satisfies these protocols structurally.
"""

from __future__ import annotations

import socket
from typing import Any, Protocol

from ..io_buffers import RecvIterBuffer, SendBuffer
from ..io_waiter import IOWaiter


class StreamOpenIO(Protocol):
    """IO manager slice needed to open stream buffers on a connected socket."""

    def _open_sock_recv_iter(
        self,
        sock: socket.socket,
        buffer_pool: Any | None,
    ) -> RecvIterBuffer: ...

    def _open_send_buffer(self, sock: socket.socket) -> SendBuffer: ...

    def create_recv_buffer_pool(self, buffer_size: int, buffer_count: int) -> Any: ...


class StreamWriterIO(Protocol):
    """IO manager slice needed to shut down and close a stream writer socket."""

    def sock_shutdown(self, sock: socket.socket, how: int) -> IOWaiter[None]: ...

    def sock_close(self, sock: socket.socket) -> IOWaiter[None]: ...


__all__ = ["StreamOpenIO", "StreamWriterIO"]

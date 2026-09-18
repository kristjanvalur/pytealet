"""Stream writer cores and public writer types."""

from __future__ import annotations

import socket
import ssl
from collections.abc import Iterable
from typing import Any, Protocol

from ..io_buffers import SendBuffer
from .reader import AsyncStreamReader, ReadStream, StreamReader
from .util import run_coro, writer_extra_info


class StreamWriterIO(Protocol):
    """IO manager slice needed to shut down and close a stream writer socket.

    A subset of ``SocketIO``; ``ProactorIOManager`` satisfies this structurally.
    """

    def sock_shutdown(self, sock: socket.socket, how: int) -> None: ...

    def sock_close(self, sock: socket.socket) -> None: ...

    def sock_send_close(self, sock: socket.socket, data: bytes | bytearray | memoryview) -> None: ...


class WriteStream(Protocol):
    """Writable half of a native tealet stream pair."""

    @property
    def reader(self) -> ReadStream: ...

    def get_extra_info(self, name: str, default: Any = None) -> Any: ...

    def handshake(self, timeout: float | None = None) -> None: ...

    def start_tls(
        self,
        sslcontext: ssl.SSLContext,
        *,
        server_side: bool = False,
        server_hostname: str | None = None,
        ssl_handshake_timeout: float | None = None,
    ) -> tuple[ReadStream, WriteStream]: ...

    def write(self, data: bytes | bytearray | memoryview) -> None: ...

    def writelines(self, lines: Iterable[bytes | bytearray | memoryview]) -> None: ...

    def close(self) -> None: ...

    def is_closing(self) -> bool: ...

    def drain(self) -> None: ...

    def flush(self) -> None: ...

    def set_write_buffer_limits(self, high: int | None = None, low: int | None = None) -> None: ...

    def can_write_eof(self) -> bool: ...

    def write_eof(self) -> None: ...

    def wait_closed(self) -> None: ...


class WriterCore:
    def __init__(
        self,
        *,
        send_buffer: SendBuffer,
        sock: socket.socket,
        io: StreamWriterIO,
    ) -> None:
        self._send_buffer = send_buffer
        self._sock = sock
        self._io = io
        self._closing = False
        self._closed = False

    def write(self, data: bytes | bytearray | memoryview) -> None:
        if self._closing or self._closed:
            raise RuntimeError("StreamWriter is closed")
        self._send_buffer.write(data)

    def writelines(self, lines: Iterable[bytes | bytearray | memoryview]) -> None:
        for line in lines:
            self.write(line)

    def drain(self) -> None:
        self._send_buffer.drain()

    def flush(self) -> None:
        self._send_buffer.flush()

    def set_write_buffer_limits(self, high: int | None = None, low: int | None = None) -> None:
        self._send_buffer.set_write_buffer_limits(high, low)

    def can_write_eof(self) -> bool:
        return (
            not self._closing and not self._closed and not self._send_buffer.eof_pending and self._sock.fileno() != -1
        )

    def write_eof(self) -> None:
        """Request half-close of the write side after queued data is sent."""

        if self._closing or self._closed:
            raise RuntimeError("write_eof() called on closed StreamWriter")
        if self._sock.fileno() == -1:
            raise RuntimeError("write_eof() called on closed StreamWriter")
        self._send_buffer.write_eof()

    def close(self) -> None:
        """Begin writer shutdown without waiting for queued data or socket close."""

        if self._closing or self._closed:
            return
        self._closing = True
        self._send_buffer.close()

    def wait_closed(self) -> None:
        """Submit remaining sends then close the socket without parking.

        If bytes are queued and no send is in flight, uses ``sock_send_close``
        (sendall then nowait close). If a send is already in flight (for
        example after ``drain()``), close runs when that drain finishes.
        Idle writers just ``sock_close``. Does not wait for the fd to go
        away; later send errors go to the delivery exception handler.

        ``SHUT_WR`` is still only ``write_eof()``.
        """

        if self._closed:
            return
        if not self._closing:
            self.close()
        pending = self._send_buffer.steal_pending()
        if pending:
            self._io.sock_send_close(self._sock, pending)
            self._closed = True
            return
        if self._send_buffer.arm_close_when_idle():  # noqa: SIM102 — arm has side effects
            if self._sock.fileno() != -1:
                self._io.sock_close(self._sock)
        self._closed = True

    def is_closing(self) -> bool:
        return self._closing or self._closed


class StreamWriter:
    """Native tealet stream writer with synchronous methods.

    Implements ``WriteStream``.
    """

    def __init__(
        self,
        *,
        send_buffer: SendBuffer,
        sock: socket.socket,
        io: StreamWriterIO,
        reader: StreamReader | None = None,
    ) -> None:
        self._send_buffer = send_buffer
        self._sock = sock
        self._io = io
        self._core = WriterCore(send_buffer=send_buffer, sock=sock, io=io)
        self._reader = reader

    def get_extra_info(self, name: str, default: Any = None) -> Any:
        return writer_extra_info(self._sock, name, default)

    @property
    def reader(self) -> ReadStream:
        if self._reader is None:
            raise RuntimeError("StreamWriter has no paired reader")
        return self._reader

    def handshake(self, timeout: float | None = None) -> None:
        """No-op for plaintext; TLS factories implement a real handshake."""

        return

    def start_tls(
        self,
        sslcontext: ssl.SSLContext,
        *,
        server_side: bool = False,
        server_hostname: str | None = None,
        ssl_handshake_timeout: float | None = None,
    ) -> tuple[ReadStream, WriteStream]:
        """Drain plaintext, wrap this pair as TLS, and handshake.

        Returns a new ``(stream, stream)`` pair. The old reader and writer
        become the ciphertext legs and must not be used.
        """

        from .ssl import start_tls as ssl_start_tls

        return ssl_start_tls(
            self,
            sslcontext,
            server_side=server_side,
            server_hostname=server_hostname,
            ssl_handshake_timeout=ssl_handshake_timeout,
        )

    def write(self, data: bytes | bytearray | memoryview) -> None:
        self._core.write(data)

    def writelines(self, lines: Iterable[bytes | bytearray | memoryview]) -> None:
        self._core.writelines(lines)

    def close(self) -> None:
        if self._reader is not None:
            self._reader.close()
        self._core.close()

    def is_closing(self) -> bool:
        return self._core.is_closing()

    def drain(self) -> None:
        self._core.drain()

    def flush(self) -> None:
        self._core.flush()

    def set_write_buffer_limits(self, high: int | None = None, low: int | None = None) -> None:
        self._core.set_write_buffer_limits(high, low)

    def can_write_eof(self) -> bool:
        return self._core.can_write_eof()

    def write_eof(self) -> None:
        self._core.write_eof()

    def wait_closed(self) -> None:
        self._core.wait_closed()


class AsyncStreamWriter:
    """Asyncio-shaped stream writer backed by tealet-blocking socket I/O."""

    def __init__(
        self,
        *,
        send_buffer: SendBuffer,
        sock: socket.socket,
        io: StreamWriterIO,
        reader: AsyncStreamReader | None = None,
    ) -> None:
        self._send_buffer = send_buffer
        self._sock = sock
        self._io = io
        self._core = WriterCore(send_buffer=send_buffer, sock=sock, io=io)
        self._reader = reader

    def get_extra_info(self, name: str, default: Any = None) -> Any:
        return writer_extra_info(self._sock, name, default)

    def handshake(self, timeout: float | None = None) -> None:
        """No-op for plaintext; TLS factories implement a real handshake."""

        return

    def write(self, data: bytes | bytearray | memoryview) -> None:
        self._core.write(data)

    def writelines(self, lines: Iterable[bytes | bytearray | memoryview]) -> None:
        self._core.writelines(lines)

    def close(self) -> None:
        if self._reader is not None:
            self._reader.close()
        self._core.close()

    def is_closing(self) -> bool:
        return self._core.is_closing()

    async def drain(self) -> None:
        self._core.drain()

    async def flush(self) -> None:
        self._core.flush()

    def set_write_buffer_limits(self, high: int | None = None, low: int | None = None) -> None:
        self._core.set_write_buffer_limits(high, low)

    def can_write_eof(self) -> bool:
        return self._core.can_write_eof()

    def write_eof(self) -> None:
        self._core.write_eof()

    async def wait_closed(self) -> None:
        self._core.wait_closed()


def shutdown_stream_writer(
    writer: WriteStream | AsyncStreamWriter,
    *,
    best_effort: bool = False,
) -> None:
    """Close a stream writer and finish teardown (``wait_closed``).

    ``wait_closed()`` submits remaining sends and close without parking.

    When ``best_effort`` is false (normal handler cleanup), flush and transport
    errors propagate after best-effort socket close. When true (discarded
    accepts or failed handler spawn), all shutdown errors are suppressed.
    """

    try:
        writer.close()
        if isinstance(writer, AsyncStreamWriter):
            run_coro(writer.wait_closed())
        else:
            writer.wait_closed()
    except OSError:
        pass
    except BaseException:
        if not best_effort:
            raise

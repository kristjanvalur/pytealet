"""Stream writer cores and public writer types."""

from __future__ import annotations

import socket
from collections.abc import Iterable
from typing import Any, Protocol

from ..io_buffers import SendBuffer
from ..io_waiter import IOWaiter
from .util import run_coro, writer_extra_info
from .reader import AsyncStreamReader, StreamReader


class StreamWriterIO(Protocol):
    """IO manager slice needed to shut down and close a stream writer socket.

    A subset of ``SocketIO``; ``ProactorIOManager`` satisfies this structurally.
    """

    def sock_shutdown(self, sock: socket.socket, how: int) -> IOWaiter[None]: ...

    def sock_close(self, sock: socket.socket) -> IOWaiter[None]: ...


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
        """Block until queued sends finish and the socket is closed via the proactor."""

        if self._closed:
            return
        if not self._closing:
            self.close()
        flush_error: BaseException | None = None
        try:
            self._send_buffer.flush()
        except BaseException as exc:
            flush_error = exc
        if self._sock.fileno() != -1:
            if not self._send_buffer.write_eof_done:
                self._io.sock_shutdown(self._sock, socket.SHUT_WR).forget()
            try:
                self._io.sock_close(self._sock).wait()
            except OSError:
                pass
        self._closed = True
        if flush_error is not None:
            raise flush_error

    def is_closing(self) -> bool:
        return self._closing or self._closed


class StreamWriter:
    """Native tealet stream writer with synchronous methods."""

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
    writer: StreamWriter | AsyncStreamWriter,
    *,
    best_effort: bool = False,
) -> None:
    """Close a stream writer and wait for queued sends and socket teardown.

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

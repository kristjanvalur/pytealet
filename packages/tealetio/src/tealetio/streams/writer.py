"""Native stream writer, and the asyncio-shaped subclass."""

from __future__ import annotations

import ssl
from collections.abc import Iterable
from typing import Any, Protocol

from ..io_buffers import SendBuffer
from ..socket_helpers import socket
from .reader import AsyncStreamReader, ReadStream, StreamReader
from .util import writer_extra_info


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
        limit: int | None = None,
    ) -> tuple[ReadStream, WriteStream]: ...

    def write(self, data: bytes | bytearray | memoryview) -> None: ...

    def writelines(self, lines: Iterable[bytes | bytearray | memoryview]) -> None: ...

    def close(self) -> None: ...

    def close_nowait(self) -> None: ...

    def is_closing(self) -> bool: ...

    def drain(self) -> None: ...

    def drain_nowait(self) -> None: ...

    def set_write_buffer_limits(self, high: int | None = None, low: int | None = None) -> None: ...

    def can_write_eof(self) -> bool: ...

    def write_eof(self) -> None: ...

    def wait_closed(self) -> None: ...

    def abort(self) -> None: ...


class StreamWriter:
    """Native tealet stream writer with synchronous methods.

    Implements ``WriteStream``.
    """

    def __init__(
        self,
        *,
        send_buffer: SendBuffer,
        sock: socket.socket,
        reader: StreamReader | None = None,
    ) -> None:
        self._send_buffer = send_buffer
        self._sock = sock
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
        limit: int | None = None,
    ) -> tuple[ReadStream, WriteStream]:
        """Submit queued plaintext, wrap this pair as TLS, and handshake.

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
            limit=limit,
        )

    def write(self, data: bytes | bytearray | memoryview) -> None:
        if self._send_buffer.closed:
            raise RuntimeError("StreamWriter is closed")
        self._send_buffer.write(data)

    def writelines(self, lines: Iterable[bytes | bytearray | memoryview]) -> None:
        for line in lines:
            self.write(line)

    def drain(self) -> None:
        """Submit queued bytes and park above high water."""

        self._send_buffer.drain()

    def drain_nowait(self) -> None:
        """Submit queued bytes. Does not park for the high-water mark."""

        self._send_buffer.drain_nowait()

    def set_write_buffer_limits(self, high: int | None = None, low: int | None = None) -> None:
        self._send_buffer.set_write_buffer_limits(high, low)

    def can_write_eof(self) -> bool:
        """Socket writers support ``SHUT_WR``."""

        return True

    def write_eof(self) -> None:
        """Request half-close of the write side after queued data is sent."""

        if self._send_buffer.closed:
            raise RuntimeError("write_eof() called on closed StreamWriter")
        if self._sock.fileno() == -1:
            raise RuntimeError("write_eof() called on closed StreamWriter")
        self._send_buffer.write_eof()

    def close(self) -> None:
        """Queue the final send and a close completion. Does not park.

        Also closes the paired reader. ``wait_closed()`` does not.
        """

        if self._reader is not None:
            self._reader.close()
        self._send_buffer.close()

    def close_nowait(self) -> None:
        """Queue the final send and a nowait close. Does not park.

        Also closes the paired reader. No close completion is delivered.
        ``wait_closed()`` still waits for a counted ``drain`` send.
        """

        if self._reader is not None:
            self._reader.close()
        self._send_buffer.close_nowait()

    def wait_closed(self) -> None:
        """Park until the buffer's close has completed.

        Counted sends, a ``write_eof()`` tail and its ``SHUT_WR``, and the
        close tail are ahead of that close on the socket, so they have
        finished too. The scheduler can then exit without dropping them.
        ``SHUT_WR`` is still only ``write_eof()``. Does not close the
        paired reader; ``close()`` does that. A later call asks the buffer
        again; the buffer returns once ``connection_lost`` is set.
        """

        self._send_buffer.wait_closed()

    def abort(self) -> None:
        """Abort the send pipeline. Does not park.

        Submits the close when ``close()`` has not been accepted, then
        aborts that queue, and closes the paired reader. After
        ``close_nowait()`` there is no queue handle, so the abort does
        not call ``abort_queue``. The library does
        not call this. A server that timed out in its own ``wait_closed()``
        may. The close still runs. A later ``wait_closed()`` raises the
        cancelled send.
        """

        self._send_buffer.abort()
        if self._reader is not None:
            self._reader.close()

    def is_closing(self) -> bool:
        """True once the buffer has accepted ``close()`` or ``close_nowait()``."""

        return self._send_buffer.closed


class AsyncStreamWriter(StreamWriter):
    """Asyncio-shaped stream writer.

    Same send and close path as ``StreamWriter``. ``drain`` and
    ``wait_closed`` are coroutines around those calls. ``drain_nowait``,
    ``close_nowait``, and ``abort`` stay synchronous, so the backlog can
    be handed to the proactor without entering a coroutine. Not a
    ``WriteStream``: the coroutine signatures are the asyncio shape.
    """

    def __init__(
        self,
        *,
        send_buffer: SendBuffer,
        sock: socket.socket,
        reader: AsyncStreamReader | None = None,
    ) -> None:
        super().__init__(send_buffer=send_buffer, sock=sock)
        self._reader = reader

    async def drain(self) -> None:  # ty: ignore[incompatible-method-override]
        StreamWriter.drain(self)

    async def wait_closed(self) -> None:  # ty: ignore[incompatible-method-override]
        StreamWriter.wait_closed(self)


def shutdown_stream_writer(
    writer: WriteStream | AsyncStreamWriter,
    *,
    best_effort: bool = False,
) -> None:
    """Close a stream writer. Does not park.

    ``close_nowait()`` submits the tail and the close with no callback.
    A handler that needs the close completion calls ``close()`` and
    ``wait_closed()`` itself.

    When ``best_effort`` is false (normal handler cleanup), transport errors
    propagate. When true (discarded accepts or failed handler spawn), an
    ``Exception`` from ``close_nowait()`` is suppressed. ``KeyboardInterrupt``
    and ``SystemExit`` propagate either way.
    """

    try:
        writer.close_nowait()
    except OSError:
        pass
    except Exception:
        if not best_effort:
            raise

"""Tealet-native stream helpers with optional asyncio-compatible facades."""

from __future__ import annotations

import asyncio
import socket
from collections.abc import Coroutine
from typing import Any, Protocol, TypeVar

from asynkit import coro_drive

from .proactor import ProactorScheduler

T = TypeVar("T")

_DEFAULT_LIMIT = 2**16

__all__ = [
    "SocketTransport",
    "StreamReader",
    "StreamWriter",
    "AsyncStreamReader",
    "AsyncStreamWriter",
    "StreamFactory",
    "AsyncStreamFactory",
    "default_stream_factory",
    "default_async_stream_factory",
    "open_connection",
    "open_streams",
    "open_async_connection",
    "open_async_streams",
    "run_coro",
]


class StreamFactory(Protocol):
    """Build a native ``(StreamReader, StreamWriter)`` pair for a connected socket."""

    def __call__(
        self,
        scheduler: ProactorScheduler,
        sock: socket.socket,
        *,
        limit: int = _DEFAULT_LIMIT,
    ) -> tuple[StreamReader, StreamWriter]: ...


class AsyncStreamFactory(Protocol):
    """Build an asyncio-shaped ``(AsyncStreamReader, AsyncStreamWriter)`` pair."""

    def __call__(
        self,
        scheduler: ProactorScheduler,
        sock: socket.socket,
        *,
        limit: int = _DEFAULT_LIMIT,
    ) -> tuple[AsyncStreamReader, AsyncStreamWriter]: ...


def run_coro(coro: Coroutine[Any, Any, T]) -> T:
    """Drive an async-stream coroutine without an asyncio event loop.

    ``AsyncStream*`` methods are ``async def`` for handler compatibility, but
    they block through ``scheduler.wait_operation`` rather than yielding asyncio
    futures. Unexpected yields surface as ``RuntimeError``.
    """

    def on_yield(value: object) -> object:
        raise RuntimeError(f"tealetio stream coroutine yielded unexpectedly: {value!r}")

    return coro_drive(coro, on_yield)


class SocketTransport:
    """Blocking socket I/O through a proactor scheduler."""

    def __init__(self, scheduler: ProactorScheduler, sock: socket.socket) -> None:
        self._scheduler = scheduler
        self._sock = sock
        self._closed = False

    @property
    def sock(self) -> socket.socket:
        return self._sock

    def recv(self, n: int) -> bytes:
        return self._scheduler.sock_recv(self._sock, n)

    def sendall(self, data: bytes | bytearray | memoryview) -> None:
        self._scheduler.sock_sendall(self._sock, data)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._sock.close()

    def get_extra_info(self, name: str, default: Any = None) -> Any:
        if name == "socket":
            return self._sock
        if name == "peername":
            try:
                return self._sock.getpeername()
            except OSError:
                return default
        if name == "sockname":
            try:
                return self._sock.getsockname()
            except OSError:
                return default
        return default


class _ReaderCore:
    def __init__(self, transport: SocketTransport, *, limit: int = _DEFAULT_LIMIT) -> None:
        self._transport = transport
        self._limit = limit
        self._buffer = bytearray()
        self._eof = False

    @property
    def at_eof(self) -> bool:
        return self._eof and not self._buffer

    def _readinto_buffer(self, n: int) -> bytes:
        if self._buffer:
            if len(self._buffer) >= n:
                chunk = bytes(self._buffer[:n])
                del self._buffer[:n]
                return chunk
            chunk = bytes(self._buffer)
            self._buffer.clear()
            if self._eof:
                return chunk
            extra = self._transport.recv(n - len(chunk))
            if not extra:
                self._eof = True
                return chunk
            return chunk + extra

        if self._eof:
            return b""

        chunk = self._transport.recv(n)
        if not chunk:
            self._eof = True
        return chunk

    def read(self, n: int = -1) -> bytes:
        if n == 0:
            return b""
        if n < 0:
            parts: list[bytes] = []
            if self._buffer:
                parts.append(bytes(self._buffer))
                self._buffer.clear()
            while not self._eof:
                chunk = self._transport.recv(self._limit)
                if not chunk:
                    self._eof = True
                    break
                parts.append(chunk)
            return b"".join(parts)
        return self._readinto_buffer(n)

    def readexactly(self, n: int) -> bytes:
        if n < 0:
            raise ValueError("readexactly size must not be negative")
        if n == 0:
            return b""

        collected = bytearray()
        while len(collected) < n:
            if self._eof:
                raise asyncio.IncompleteReadError(bytes(collected), n)
            chunk = self._readinto_buffer(n - len(collected))
            if not chunk:
                raise asyncio.IncompleteReadError(bytes(collected), n)
            collected.extend(chunk)
        return bytes(collected)

    def readline(self) -> bytes:
        while True:
            newline = self._buffer.find(b"\n")
            if newline >= 0:
                line = bytes(self._buffer[: newline + 1])
                del self._buffer[: newline + 1]
                return line
            if self._eof:
                if self._buffer:
                    line = bytes(self._buffer)
                    self._buffer.clear()
                    return line
                return b""
            chunk = self._transport.recv(self._limit)
            if not chunk:
                self._eof = True
                continue
            self._buffer.extend(chunk)


class _WriterCore:
    def __init__(self, transport: SocketTransport) -> None:
        self._transport = transport
        self._closed = False

    def write(self, data: bytes | bytearray | memoryview) -> None:
        if self._closed:
            raise RuntimeError("StreamWriter is closed")
        if not data:
            return
        self._transport.sendall(data)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._transport.close()

    def is_closing(self) -> bool:
        return self._closed


class StreamReader:
    """Native tealet stream reader with synchronous methods."""

    def __init__(self, transport: SocketTransport, *, limit: int = _DEFAULT_LIMIT) -> None:
        self._core = _ReaderCore(transport, limit=limit)

    @property
    def at_eof(self) -> bool:
        return self._core.at_eof

    def read(self, n: int = -1) -> bytes:
        return self._core.read(n)

    def readexactly(self, n: int) -> bytes:
        return self._core.readexactly(n)

    def readline(self) -> bytes:
        return self._core.readline()


class AsyncStreamReader:
    """Asyncio-shaped stream reader backed by tealet-blocking socket I/O."""

    def __init__(self, transport: SocketTransport, *, limit: int = _DEFAULT_LIMIT) -> None:
        self._core = _ReaderCore(transport, limit=limit)

    @property
    def at_eof(self) -> bool:
        return self._core.at_eof

    async def read(self, n: int = -1) -> bytes:
        return self._core.read(n)

    async def readexactly(self, n: int) -> bytes:
        return self._core.readexactly(n)

    async def readline(self) -> bytes:
        return self._core.readline()


class StreamWriter:
    """Native tealet stream writer with synchronous methods."""

    def __init__(self, transport: SocketTransport, reader: StreamReader | None = None) -> None:
        self._core = _WriterCore(transport)
        self._reader = reader

    @property
    def transport(self) -> SocketTransport:
        return self._core._transport

    def get_extra_info(self, name: str, default: Any = None) -> Any:
        return self._core._transport.get_extra_info(name, default)

    def write(self, data: bytes | bytearray | memoryview) -> None:
        self._core.write(data)

    def close(self) -> None:
        self._core.close()

    def is_closing(self) -> bool:
        return self._core.is_closing()

    def drain(self) -> None:
        return None

    def wait_closed(self) -> None:
        return None


class AsyncStreamWriter:
    """Asyncio-shaped stream writer backed by tealet-blocking socket I/O."""

    def __init__(self, transport: SocketTransport, reader: AsyncStreamReader | None = None) -> None:
        self._core = _WriterCore(transport)
        self._reader = reader

    @property
    def transport(self) -> SocketTransport:
        return self._core._transport

    def get_extra_info(self, name: str, default: Any = None) -> Any:
        return self._core._transport.get_extra_info(name, default)

    def write(self, data: bytes | bytearray | memoryview) -> None:
        self._core.write(data)

    def close(self) -> None:
        self._core.close()

    def is_closing(self) -> bool:
        return self._core.is_closing()

    async def drain(self) -> None:
        return None

    async def wait_closed(self) -> None:
        return None


def default_stream_factory(
    scheduler: ProactorScheduler,
    sock: socket.socket,
    *,
    limit: int = _DEFAULT_LIMIT,
) -> tuple[StreamReader, StreamWriter]:
    """Construct the default native stream pair for a connected socket."""

    transport = SocketTransport(scheduler, sock)
    reader = StreamReader(transport, limit=limit)
    writer = StreamWriter(transport, reader)
    return reader, writer


def default_async_stream_factory(
    scheduler: ProactorScheduler,
    sock: socket.socket,
    *,
    limit: int = _DEFAULT_LIMIT,
) -> tuple[AsyncStreamReader, AsyncStreamWriter]:
    """Construct the default asyncio-shaped stream pair for a connected socket."""

    transport = SocketTransport(scheduler, sock)
    reader = AsyncStreamReader(transport, limit=limit)
    writer = AsyncStreamWriter(transport, reader)
    return reader, writer


def open_streams(
    scheduler: ProactorScheduler,
    sock: socket.socket,
    *,
    limit: int = _DEFAULT_LIMIT,
    stream_factory: StreamFactory | None = None,
) -> tuple[StreamReader, StreamWriter]:
    """Wrap a connected non-blocking socket as native stream endpoints."""

    factory = default_stream_factory if stream_factory is None else stream_factory
    return factory(scheduler, sock, limit=limit)


def open_async_streams(
    scheduler: ProactorScheduler,
    sock: socket.socket,
    *,
    limit: int = _DEFAULT_LIMIT,
    stream_factory: AsyncStreamFactory | None = None,
) -> tuple[AsyncStreamReader, AsyncStreamWriter]:
    """Wrap a connected non-blocking socket as asyncio-shaped stream endpoints."""

    factory = default_async_stream_factory if stream_factory is None else stream_factory
    return factory(scheduler, sock, limit=limit)


def open_connection(
    scheduler: ProactorScheduler,
    host: str,
    port: int,
    *,
    family: int = socket.AF_UNSPEC,
    proto: int = 0,
    limit: int = _DEFAULT_LIMIT,
    stream_factory: StreamFactory | None = None,
) -> tuple[StreamReader, StreamWriter]:
    """Connect to ``host:port`` and return native stream endpoints."""

    infos = socket.getaddrinfo(host, port, family=family, type=socket.SOCK_STREAM, proto=proto)
    last_error: OSError | None = None
    for family, socktype, proto, _canonname, sockaddr in infos:
        sock: socket.socket | None = None
        try:
            sock = socket.socket(family, socktype, proto)
            sock.setblocking(False)
            scheduler.sock_connect(sock, sockaddr)
            return open_streams(scheduler, sock, limit=limit, stream_factory=stream_factory)
        except OSError as exc:
            last_error = exc
            if sock is not None:
                sock.close()
    if last_error is not None:
        raise last_error
    raise OSError("open_connection failed without address resolution results")


def open_async_connection(
    scheduler: ProactorScheduler,
    host: str,
    port: int,
    *,
    family: int = socket.AF_UNSPEC,
    proto: int = 0,
    limit: int = _DEFAULT_LIMIT,
    stream_factory: AsyncStreamFactory | None = None,
) -> tuple[AsyncStreamReader, AsyncStreamWriter]:
    """Connect to ``host:port`` and return asyncio-shaped stream endpoints."""

    infos = socket.getaddrinfo(host, port, family=family, type=socket.SOCK_STREAM, proto=proto)
    last_error: OSError | None = None
    for family, socktype, proto, _canonname, sockaddr in infos:
        sock: socket.socket | None = None
        try:
            sock = socket.socket(family, socktype, proto)
            sock.setblocking(False)
            scheduler.sock_connect(sock, sockaddr)
            return open_async_streams(scheduler, sock, limit=limit, stream_factory=stream_factory)
        except OSError as exc:
            last_error = exc
            if sock is not None:
                sock.close()
    if last_error is not None:
        raise last_error
    raise OSError("open_connection failed without address resolution results")
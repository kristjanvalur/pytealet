"""Tealet-native stream helpers with optional asyncio-compatible facades."""

from __future__ import annotations

import asyncio
import os
import socket
from collections.abc import Callable, Coroutine
from typing import Any, Protocol, TypeVar

from asynkit import coro_drive

from .operations import ContinuousOperation
from .proactor import ProactorScheduler

T = TypeVar("T")
TStreamPair = TypeVar("TStreamPair")

_DEFAULT_LIMIT = 2**16

__all__ = [
    "SocketTransport",
    "StreamReader",
    "StreamWriter",
    "AsyncStreamReader",
    "AsyncStreamWriter",
    "StreamFactory",
    "AsyncStreamFactory",
    "StreamServer",
    "default_stream_factory",
    "default_async_stream_factory",
    "open_connection",
    "open_unix_connection",
    "open_streams",
    "open_async_connection",
    "open_async_unix_connection",
    "open_async_streams",
    "start_server",
    "start_unix_server",
    "start_async_server",
    "start_async_unix_server",
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

    def recv_into(self, buf: Any) -> int:
        return self._scheduler.sock_recv_into(self._sock, buf)

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

    def _recv_into_socket(self, view: memoryview) -> int:
        if not view:
            return 0
        nbytes = self._transport.recv_into(view)
        if nbytes == 0:
            self._eof = True
        return nbytes

    def _recv_chunk(self, chunk_size: int) -> bytes:
        chunk = bytearray(chunk_size)
        nbytes = self._recv_into_socket(memoryview(chunk))
        return bytes(chunk[:nbytes])

    def _fill_buffer(self, min_bytes: int) -> None:
        while len(self._buffer) < min_bytes and not self._eof:
            chunk_size = min(self._limit, max(min_bytes - len(self._buffer), 1))
            chunk = self._recv_chunk(chunk_size)
            if not chunk:
                return
            self._buffer.extend(chunk)

    def _take_bytes(self, n: int) -> bytes:
        count = min(n, len(self._buffer))
        if count == 0:
            return b""
        chunk = bytes(self._buffer[:count])
        del self._buffer[:count]
        return chunk

    def _read_some(self, n: int) -> bytes:
        if self._eof and not self._buffer:
            return b""
        if len(self._buffer) < n and not self._eof:
            self._fill_buffer(n)
        return self._take_bytes(min(n, len(self._buffer)))

    def read(self, n: int = -1) -> bytes:
        if n == 0:
            return b""
        if n < 0:
            parts: list[bytes] = []
            if self._buffer:
                parts.append(self._take_bytes(len(self._buffer)))
            while not self._eof:
                chunk = self._recv_chunk(self._limit)
                if not chunk:
                    break
                parts.append(chunk)
            return b"".join(parts)
        return self._read_some(n)

    def readinto(self, b: Any) -> int | None:
        view = memoryview(b).cast("B")
        if not view.nbytes:
            return 0

        total = 0
        if self._buffer:
            prefix = min(view.nbytes, len(self._buffer))
            view[:prefix] = self._buffer[:prefix]
            del self._buffer[:prefix]
            total += prefix
            if total == view.nbytes:
                return total

        if self._eof:
            return total or 0

        total += self._recv_into_socket(view[total:])
        return total

    def readexactly(self, n: int) -> bytes:
        if n < 0:
            raise ValueError("readexactly size must not be negative")
        if n == 0:
            return b""

        if len(self._buffer) < n and not self._eof:
            self._fill_buffer(n)
        if len(self._buffer) < n:
            partial = bytes(self._buffer)
            self._buffer.clear()
            raise asyncio.IncompleteReadError(partial, n)
        return self._take_bytes(n)

    def readline(self) -> bytes:
        while True:
            newline = self._buffer.find(b"\n")
            if newline >= 0:
                return self._take_bytes(newline + 1)
            if self._eof:
                return self._take_bytes(len(self._buffer))
            self._fill_buffer(len(self._buffer) + 1)


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

    def readinto(self, b: Any) -> int | None:
        return self._core.readinto(b)

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

    async def readinto(self, b: Any) -> int | None:
        return self._core.readinto(b)

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


def _connect_tcp_streams(
    scheduler: ProactorScheduler,
    host: str,
    port: int,
    *,
    family: int = socket.AF_UNSPEC,
    proto: int = 0,
    limit: int = _DEFAULT_LIMIT,
    stream_factory: StreamFactory | AsyncStreamFactory | None,
    open_streams_fn: Callable[..., TStreamPair],
) -> TStreamPair:
    infos = scheduler.ensure_resolved(
        (host, port),
        family=family,
        type=socket.SOCK_STREAM,
        proto=proto,
    )
    if not infos:
        raise OSError("getaddrinfo() returned empty list")

    last_error: OSError | None = None
    for addr_family, socktype, addr_proto, _canonname, sockaddr in infos:
        sock: socket.socket | None = None
        try:
            sock = socket.socket(addr_family, socktype, addr_proto)
            sock.setblocking(False)
            scheduler.sock_connect(sock, sockaddr)
            return open_streams_fn(
                scheduler,
                sock,
                limit=limit,
                stream_factory=stream_factory,
            )
        except OSError as exc:
            last_error = exc
            if sock is not None:
                sock.close()
    if last_error is not None:
        raise last_error
    raise OSError("open_connection failed without address resolution results")


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

    return _connect_tcp_streams(
        scheduler,
        host,
        port,
        family=family,
        proto=proto,
        limit=limit,
        stream_factory=stream_factory,
        open_streams_fn=open_streams,
    )


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

    return _connect_tcp_streams(
        scheduler,
        host,
        port,
        family=family,
        proto=proto,
        limit=limit,
        stream_factory=stream_factory,
        open_streams_fn=open_async_streams,
    )


def _connect_unix_streams(
    scheduler: ProactorScheduler,
    path: str,
    *,
    limit: int = _DEFAULT_LIMIT,
    stream_factory: StreamFactory | AsyncStreamFactory | None,
    open_streams_fn: Callable[..., TStreamPair],
) -> TStreamPair:
    if not hasattr(socket, "AF_UNIX"):
        raise RuntimeError("AF_UNIX is not supported on this platform")

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.setblocking(False)
    try:
        scheduler.sock_connect(sock, path)
    except OSError:
        sock.close()
        raise
    return open_streams_fn(
        scheduler,
        sock,
        limit=limit,
        stream_factory=stream_factory,
    )


def open_unix_connection(
    scheduler: ProactorScheduler,
    path: str,
    *,
    limit: int = _DEFAULT_LIMIT,
    stream_factory: StreamFactory | None = None,
) -> tuple[StreamReader, StreamWriter]:
    """Connect to a Unix domain socket path and return native stream endpoints."""

    return _connect_unix_streams(
        scheduler,
        path,
        limit=limit,
        stream_factory=stream_factory,
        open_streams_fn=open_streams,
    )


def open_async_unix_connection(
    scheduler: ProactorScheduler,
    path: str,
    *,
    limit: int = _DEFAULT_LIMIT,
    stream_factory: AsyncStreamFactory | None = None,
) -> tuple[AsyncStreamReader, AsyncStreamWriter]:
    """Connect to a Unix domain socket path and return asyncio-shaped stream endpoints."""

    return _connect_unix_streams(
        scheduler,
        path,
        limit=limit,
        stream_factory=stream_factory,
        open_streams_fn=open_async_streams,
    )


def _bind_tcp_socket(
    host: str | None,
    port: int,
    *,
    family: int = socket.AF_INET,
    backlog: int,
) -> socket.socket:
    if port is None:
        raise ValueError("port is required for TCP servers")

    sock = socket.socket(family, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.setblocking(False)
    bind_host = "" if host is None else host
    sock.bind((bind_host, port))
    sock.listen(backlog)
    return sock


def _bind_unix_socket(path: str, *, backlog: int) -> socket.socket:
    if not hasattr(socket, "AF_UNIX"):
        raise RuntimeError("AF_UNIX is not supported on this platform")

    try:
        os.unlink(path)
    except FileNotFoundError:
        pass

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.setblocking(False)
    sock.bind(path)
    sock.listen(backlog)
    return sock


class StreamServer:
    """Listening stream server backed by a continuous ``accept_many`` operation."""

    def __init__(
        self,
        scheduler: ProactorScheduler,
        sockets: list[socket.socket],
        accept_operation: ContinuousOperation[tuple[socket.socket, Any]],
    ) -> None:
        self._scheduler = scheduler
        self._sockets = tuple(sockets)
        self._accept_operation = accept_operation
        self._closed = False

    @property
    def sockets(self) -> tuple[socket.socket, ...]:
        return self._sockets

    @property
    def accept_operation(self) -> ContinuousOperation[tuple[socket.socket, Any]]:
        return self._accept_operation

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._accept_operation.cancel()
        for sock in self._sockets:
            sock.close()


def _spawn_stream_client(
    scheduler: ProactorScheduler,
    conn: socket.socket,
    *,
    limit: int,
    stream_factory: StreamFactory | AsyncStreamFactory | None,
    open_streams_fn: Callable[..., TStreamPair],
    client_handler: Callable[..., Any],
    async_handler: bool,
) -> None:
    def serve() -> None:
        writer: StreamWriter | AsyncStreamWriter | None = None
        try:
            reader, writer = open_streams_fn(
                scheduler,
                conn,
                limit=limit,
                stream_factory=stream_factory,
            )
            if async_handler:
                run_coro(client_handler(reader, writer))
            else:
                client_handler(reader, writer)
        finally:
            if writer is not None:
                writer.close()
            else:
                conn.close()

    scheduler.call_soon_threadsafe(lambda: scheduler.spawn(serve))


def _start_stream_server(
    scheduler: ProactorScheduler,
    sock: socket.socket,
    client_handler: Callable[..., Any],
    *,
    limit: int = _DEFAULT_LIMIT,
    stream_factory: StreamFactory | AsyncStreamFactory | None = None,
    open_streams_fn: Callable[..., TStreamPair],
    async_handler: bool,
) -> StreamServer:
    def on_accept(accepted: tuple[socket.socket, Any]) -> None:
        conn, _address = accepted
        _spawn_stream_client(
            scheduler,
            conn,
            limit=limit,
            stream_factory=stream_factory,
            open_streams_fn=open_streams_fn,
            client_handler=client_handler,
            async_handler=async_handler,
        )

    accept_operation = scheduler.proactor.accept_many(sock, on_accept)
    return StreamServer(scheduler, [sock], accept_operation)


def start_server(
    scheduler: ProactorScheduler,
    client_handler: Callable[[StreamReader, StreamWriter], Any],
    host: str | None = None,
    port: int | None = None,
    *,
    family: int = socket.AF_INET,
    backlog: int = 100,
    limit: int = _DEFAULT_LIMIT,
    stream_factory: StreamFactory | None = None,
) -> StreamServer:
    """Start a TCP stream server that dispatches each accept to ``client_handler``.

    Accepts use ``proactor.accept_many()``, so ``UringProactor`` can service
    connections through multishot accept when the runtime probe allows it.
    Handler tealets are spawned through ``call_soon_threadsafe`` because accept
    callbacks may run on completion worker threads.
    """

    sock = _bind_tcp_socket(host, port if port is not None else 0, family=family, backlog=backlog)
    return _start_stream_server(
        scheduler,
        sock,
        client_handler,
        limit=limit,
        stream_factory=stream_factory,
        open_streams_fn=open_streams,
        async_handler=False,
    )


def start_unix_server(
    scheduler: ProactorScheduler,
    client_handler: Callable[[StreamReader, StreamWriter], Any],
    path: str,
    *,
    backlog: int = 100,
    limit: int = _DEFAULT_LIMIT,
    stream_factory: StreamFactory | None = None,
) -> StreamServer:
    """Start a Unix-domain stream server backed by ``accept_many``."""

    sock = _bind_unix_socket(path, backlog=backlog)
    return _start_stream_server(
        scheduler,
        sock,
        client_handler,
        limit=limit,
        stream_factory=stream_factory,
        open_streams_fn=open_streams,
        async_handler=False,
    )


def start_async_server(
    scheduler: ProactorScheduler,
    client_handler: Callable[[AsyncStreamReader, AsyncStreamWriter], Coroutine[Any, Any, Any]],
    host: str | None = None,
    port: int | None = None,
    *,
    family: int = socket.AF_INET,
    backlog: int = 100,
    limit: int = _DEFAULT_LIMIT,
    stream_factory: AsyncStreamFactory | None = None,
) -> StreamServer:
    """Start a TCP server that runs an asyncio-shaped handler per connection."""

    sock = _bind_tcp_socket(host, port if port is not None else 0, family=family, backlog=backlog)
    return _start_stream_server(
        scheduler,
        sock,
        client_handler,
        limit=limit,
        stream_factory=stream_factory,
        open_streams_fn=open_async_streams,
        async_handler=True,
    )


def start_async_unix_server(
    scheduler: ProactorScheduler,
    client_handler: Callable[[AsyncStreamReader, AsyncStreamWriter], Coroutine[Any, Any, Any]],
    path: str,
    *,
    backlog: int = 100,
    limit: int = _DEFAULT_LIMIT,
    stream_factory: AsyncStreamFactory | None = None,
) -> StreamServer:
    """Start a Unix-domain server that runs an asyncio-shaped handler per connection."""

    sock = _bind_unix_socket(path, backlog=backlog)
    return _start_stream_server(
        scheduler,
        sock,
        client_handler,
        limit=limit,
        stream_factory=stream_factory,
        open_streams_fn=open_async_streams,
        async_handler=True,
    )
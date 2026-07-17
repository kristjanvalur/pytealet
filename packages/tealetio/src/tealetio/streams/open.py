"""Open connected sockets as stream endpoint pairs.

Isolated from ``connect`` / ``server`` so ``io_manager`` can import this module
without pulling the public streams API surface or importing ``io_manager``.
"""

from __future__ import annotations

import socket
from collections.abc import Callable, Coroutine
from typing import Any, Literal, Protocol, TypeAlias, cast, overload

from ..io_buffers import RecvIterBuffer, SendBuffer
from .util import DEFAULT_LIMIT
from .reader import AsyncStreamReader, StreamReader
from .writer import AsyncStreamWriter, StreamWriter, StreamWriterIO

__all__ = [
    "StreamOpenIO",
    "AsyncClientHandler",
    "AsyncStreamFactory",
    "AsyncStreamPair",
    "ClientHandler",
    "NativeClientHandler",
    "NativeStreamPair",
    "StreamFactory",
    "StreamFactoryArg",
    "default_async_stream_factory",
    "default_server_stream_factory",
    "default_stream_factory",
    "open_recv_buffer",
    "open_send_buffer",
    "open_streams",
    "pooled_default_stream_factory",
]

NativeStreamPair: TypeAlias = tuple[StreamReader, StreamWriter]
AsyncStreamPair: TypeAlias = tuple[AsyncStreamReader, AsyncStreamWriter]
StreamFactoryArg: TypeAlias = "StreamFactory | AsyncStreamFactory | None"
NativeClientHandler: TypeAlias = Callable[[StreamReader, StreamWriter], Any]
AsyncClientHandler: TypeAlias = Callable[[AsyncStreamReader, AsyncStreamWriter], Coroutine[Any, Any, Any]]
ClientHandler: TypeAlias = NativeClientHandler | AsyncClientHandler


class StreamOpenIO(Protocol):
    """IO manager slice needed to open stream buffers on a connected socket.

    ``ProactorIOManager`` satisfies this structurally; see also ``SocketIO`` in
    ``io_manager`` for the wider socket helper surface.
    """

    def _open_sock_recv_iter(
        self,
        sock: socket.socket,
        buffer_pool: Any | None,
    ) -> RecvIterBuffer: ...

    def _open_send_buffer(self, sock: socket.socket) -> SendBuffer: ...

    def create_recv_buffer_pool(self, buffer_size: int, buffer_count: int) -> Any: ...

    def acquire_recv_buffer_pool(self, buffer_size: int, buffer_count: int) -> Any: ...


class StreamFactory(Protocol):
    """Build a native ``(StreamReader, StreamWriter)`` pair for a connected socket."""

    def __call__(
        self,
        io: StreamOpenIO,
        sock: socket.socket,
        *,
        limit: int = DEFAULT_LIMIT,
    ) -> tuple[StreamReader, StreamWriter]: ...


class AsyncStreamFactory(Protocol):
    """Build an asyncio-shaped ``(AsyncStreamReader, AsyncStreamWriter)`` pair."""

    def __call__(
        self,
        io: StreamOpenIO,
        sock: socket.socket,
        *,
        limit: int = DEFAULT_LIMIT,
    ) -> tuple[AsyncStreamReader, AsyncStreamWriter]: ...


def open_recv_buffer(
    io: StreamOpenIO,
    sock: socket.socket,
    recv_buffer_pool: Any | None,
) -> RecvIterBuffer:
    return io._open_sock_recv_iter(sock, recv_buffer_pool)


def open_send_buffer(io: StreamOpenIO, sock: socket.socket) -> SendBuffer:
    return io._open_send_buffer(sock)


def default_stream_factory(
    io: StreamOpenIO,
    sock: socket.socket,
    *,
    limit: int = DEFAULT_LIMIT,
    recv_buffer_pool: Any | None = None,
) -> tuple[StreamReader, StreamWriter]:
    """Construct the default native stream pair for a connected socket."""

    writer_io = cast(StreamWriterIO, io)
    recv_buffer = open_recv_buffer(io, sock, recv_buffer_pool)
    send_buffer = open_send_buffer(io, sock)
    reader = StreamReader(limit=limit, recv_buffer=recv_buffer)
    writer = StreamWriter(send_buffer=send_buffer, sock=sock, io=writer_io, reader=reader)
    return reader, writer


def default_async_stream_factory(
    io: StreamOpenIO,
    sock: socket.socket,
    *,
    limit: int = DEFAULT_LIMIT,
    recv_buffer_pool: Any | None = None,
) -> tuple[AsyncStreamReader, AsyncStreamWriter]:
    """Construct the default asyncio-shaped stream pair for a connected socket."""

    writer_io = cast(StreamWriterIO, io)
    recv_buffer = open_recv_buffer(io, sock, recv_buffer_pool)
    send_buffer = open_send_buffer(io, sock)
    reader = AsyncStreamReader(limit=limit, recv_buffer=recv_buffer)
    writer = AsyncStreamWriter(send_buffer=send_buffer, sock=sock, io=writer_io, reader=reader)
    return reader, writer


@overload
def pooled_default_stream_factory(
    *,
    async_: Literal[False] = False,
    buffer_size: int = 16 * 1024,
    buffer_count: int = 4,
    pool: Any | None = None,
) -> StreamFactory: ...


@overload
def pooled_default_stream_factory(
    *,
    async_: Literal[True],
    buffer_size: int = 16 * 1024,
    buffer_count: int = 4,
    pool: Any | None = None,
) -> AsyncStreamFactory: ...


def pooled_default_stream_factory(
    *,
    async_: bool = False,
    buffer_size: int = 16 * 1024,
    buffer_count: int = 4,
    pool: Any | None = None,
) -> StreamFactory | AsyncStreamFactory:
    """Return a default stream factory with an explicit provided-buffer pool.

    When ``pool`` is omitted, each connection checks out a pool of
    ``(buffer_size, buffer_count)`` from the IO manager size cache
    (``acquire_recv_buffer_pool``). Closing the receive buffer calls
    ``pool.close()``, which returns it to the cache via ``release_callback``.
    When ``pool`` is set, every connection shares that pool (leave
    ``release_callback`` unset so ``close()`` is a real dispose). Pair
    ``async_`` with the stream types returned by ``start_server`` /
    ``open_streams`` on the call site.
    """

    delegate = default_async_stream_factory if async_ else default_stream_factory

    def factory(
        io: StreamOpenIO,
        sock: socket.socket,
        *,
        limit: int = DEFAULT_LIMIT,
    ) -> tuple[StreamReader, StreamWriter] | tuple[AsyncStreamReader, AsyncStreamWriter]:
        chosen = pool if pool is not None else io.acquire_recv_buffer_pool(buffer_size, buffer_count)
        return delegate(io, sock, limit=limit, recv_buffer_pool=chosen)

    if async_:
        return cast(AsyncStreamFactory, factory)
    return cast(StreamFactory, factory)


def default_server_stream_factory(*, async_: bool) -> StreamFactory | AsyncStreamFactory:
    """Size-cached provided-buffer pools for multi-client listeners."""

    return pooled_default_stream_factory(async_=async_)


def open_streams(
    io: StreamOpenIO,
    sock: socket.socket,
    *,
    limit: int = DEFAULT_LIMIT,
    stream_factory: StreamFactoryArg = None,
    async_: bool = False,
) -> NativeStreamPair | AsyncStreamPair:
    # ``async_`` only selects the default stream factory when ``stream_factory`` is
    # omitted. An explicit factory must already match the intended stream types.
    if stream_factory is None:
        factory = default_async_stream_factory if async_ else default_stream_factory
    else:
        factory = stream_factory
    reader, writer = factory(io, sock, limit=limit)
    if async_:
        return cast(AsyncStreamPair, (reader, writer))
    return cast(NativeStreamPair, (reader, writer))

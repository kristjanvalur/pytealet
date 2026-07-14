"""Stream factory protocols and stream-pair construction."""

from __future__ import annotations

import socket
from collections.abc import Callable, Coroutine
from typing import Any, Literal, Protocol, TypeAlias, cast, overload

from ..io_buffers import RecvIterBuffer, SendBuffer
from ..io_manager import ProactorIOManager, SocketIO
from .common import DEFAULT_LIMIT
from .reader import AsyncStreamReader, StreamReader
from .writer import AsyncStreamWriter, StreamWriter

NativeStreamPair: TypeAlias = tuple[StreamReader, StreamWriter]
AsyncStreamPair: TypeAlias = tuple[AsyncStreamReader, AsyncStreamWriter]
StreamFactoryArg: TypeAlias = "StreamFactory | AsyncStreamFactory | None"
NativeClientHandler: TypeAlias = Callable[[StreamReader, StreamWriter], Any]
AsyncClientHandler: TypeAlias = Callable[[AsyncStreamReader, AsyncStreamWriter], Coroutine[Any, Any, Any]]
ClientHandler: TypeAlias = NativeClientHandler | AsyncClientHandler


class StreamFactory(Protocol):
    """Build a native ``(StreamReader, StreamWriter)`` pair for a connected socket."""

    def __call__(
        self,
        io: SocketIO,
        sock: socket.socket,
        *,
        limit: int = DEFAULT_LIMIT,
    ) -> tuple[StreamReader, StreamWriter]: ...


class AsyncStreamFactory(Protocol):
    """Build an asyncio-shaped ``(AsyncStreamReader, AsyncStreamWriter)`` pair."""

    def __call__(
        self,
        io: SocketIO,
        sock: socket.socket,
        *,
        limit: int = DEFAULT_LIMIT,
    ) -> tuple[AsyncStreamReader, AsyncStreamWriter]: ...


def open_recv_buffer(
    io: SocketIO,
    sock: socket.socket,
    recv_buffer_pool: Any | None,
) -> RecvIterBuffer:
    if not isinstance(io, ProactorIOManager):
        raise RuntimeError("stream readers require a proactor IO manager")
    return io._open_sock_recv_iter(sock, recv_buffer_pool)


def open_send_buffer(io: SocketIO, sock: socket.socket) -> SendBuffer:
    if not isinstance(io, ProactorIOManager):
        raise RuntimeError("stream writers require a proactor IO manager")
    return io._open_send_buffer(sock)


def default_stream_factory(
    io: SocketIO,
    sock: socket.socket,
    *,
    limit: int = DEFAULT_LIMIT,
    recv_buffer_pool: Any | None = None,
) -> tuple[StreamReader, StreamWriter]:
    """Construct the default native stream pair for a connected socket."""

    proactor_io = cast(ProactorIOManager, io)
    recv_buffer = open_recv_buffer(io, sock, recv_buffer_pool)
    send_buffer = open_send_buffer(io, sock)
    reader = StreamReader(limit=limit, recv_buffer=recv_buffer)
    writer = StreamWriter(send_buffer=send_buffer, sock=sock, io=proactor_io, reader=reader)
    return reader, writer


def default_async_stream_factory(
    io: SocketIO,
    sock: socket.socket,
    *,
    limit: int = DEFAULT_LIMIT,
    recv_buffer_pool: Any | None = None,
) -> tuple[AsyncStreamReader, AsyncStreamWriter]:
    """Construct the default asyncio-shaped stream pair for a connected socket."""

    proactor_io = cast(ProactorIOManager, io)
    recv_buffer = open_recv_buffer(io, sock, recv_buffer_pool)
    send_buffer = open_send_buffer(io, sock)
    reader = AsyncStreamReader(limit=limit, recv_buffer=recv_buffer)
    writer = AsyncStreamWriter(send_buffer=send_buffer, sock=sock, io=proactor_io, reader=reader)
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

    When ``pool`` is omitted, each connection gets a fresh pool from
    ``io.create_recv_buffer_pool(buffer_size, buffer_count)``. When ``pool`` is
    set, every connection shares that pool. Pair ``async_`` with the stream
    types returned by ``start_server`` / ``open_streams`` on the call site.
    """

    delegate = default_async_stream_factory if async_ else default_stream_factory

    def factory(
        io: SocketIO,
        sock: socket.socket,
        *,
        limit: int = DEFAULT_LIMIT,
    ) -> tuple[StreamReader, StreamWriter] | tuple[AsyncStreamReader, AsyncStreamWriter]:
        chosen = pool if pool is not None else io.create_recv_buffer_pool(buffer_size, buffer_count)
        return delegate(io, sock, limit=limit, recv_buffer_pool=chosen)

    if async_:
        return cast(AsyncStreamFactory, factory)
    return cast(StreamFactory, factory)


def default_server_stream_factory(*, async_: bool) -> StreamFactory | AsyncStreamFactory:
    """Per-connection provided-buffer pools for multi-client listeners."""

    return pooled_default_stream_factory(async_=async_)


def open_streams(
    io: SocketIO,
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

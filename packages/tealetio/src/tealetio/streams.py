"""Tealet-native stream helpers with optional asyncio-compatible facades."""

from __future__ import annotations

import asyncio
import os
import socket
import sys

from collections.abc import Callable, Coroutine, Iterable
from typing import Any, Literal, Protocol, TypeAlias, TypeVar, cast, overload

from asynkit import coro_drive

from .io_manager import (
    IO_UNSUPPORTED_ERROR,
    SELECTOR_IO_UNSUPPORTED_ERROR,
    ProactorIOManager,
    ServerIO,
    SocketIO,
    SocketSendBuffer,
    SupportsProactorIO,
)
from .io_buffers import RECV_MANY_BUFFER_PRESSURE, RecvIterBuffer, SendBuffer
from .continuous_callbacks import AcceptStreamsDelivery as _AcceptedStreams

from .scheduler import BaseScheduler
from .tasks import CancelledError, Task, get_current

T = TypeVar("T")

_DEFAULT_LIMIT = 2**16

__all__ = [
    "StreamReader",
    "StreamWriter",
    "AsyncStreamReader",
    "AsyncStreamWriter",
    "StreamFactory",
    "AsyncStreamFactory",
    "StreamServer",
    "default_stream_factory",
    "default_async_stream_factory",
    "pooled_default_stream_factory",
    "open_connection",
    "open_streams",
    "start_server",
    "run_coro",
]


class StreamFactory(Protocol):
    """Build a native ``(StreamReader, StreamWriter)`` pair for a connected socket."""

    def __call__(
        self,
        io: SocketIO,
        sock: socket.socket,
        *,
        limit: int = _DEFAULT_LIMIT,
    ) -> tuple[StreamReader, StreamWriter]: ...


class AsyncStreamFactory(Protocol):
    """Build an asyncio-shaped ``(AsyncStreamReader, AsyncStreamWriter)`` pair."""

    def __call__(
        self,
        io: SocketIO,
        sock: socket.socket,
        *,
        limit: int = _DEFAULT_LIMIT,
    ) -> tuple[AsyncStreamReader, AsyncStreamWriter]: ...


def run_coro(coro: Coroutine[Any, Any, T]) -> T:
    """Drive an async-stream coroutine without an asyncio event loop.

    ``AsyncStream*`` methods are ``async def`` for handler compatibility, but
    they ultimately block through the scheduler-owned IO manager's
    ``IOWaiter.wait()``
    path rather than yielding asyncio futures. Public stream and factory APIs
    depend on ``SocketIO`` only. Unexpected yields surface as ``RuntimeError``.
    """

    def on_yield(value: object) -> object:
        raise RuntimeError(f"tealetio stream coroutine yielded unexpectedly: {value!r}")

    return coro_drive(coro, on_yield)


def _writer_extra_info(sock: socket.socket, name: str, default: Any = None) -> Any:
    if name == "socket":
        return sock
    if name == "peername":
        try:
            return sock.getpeername()
        except OSError:
            return default
    if name == "sockname":
        try:
            return sock.getsockname()
        except OSError:
            return default
    return default


class _ReaderCore:
    _COMPACT_PREFIX = 4096

    def __init__(self, *, recv_buffer: RecvIterBuffer, limit: int = _DEFAULT_LIMIT) -> None:
        self._recv_buffer = recv_buffer
        self._limit = limit
        self._buffer = bytearray()
        self._buffer_pos = 0
        self._eof = False

    @property
    def at_eof(self) -> bool:
        return self._eof and not self._buffer_available()

    def close(self) -> None:
        self._recv_buffer.close()

    def _buffer_available(self) -> int:
        return len(self._buffer) - self._buffer_pos

    def _compact_buffer(self) -> None:
        if self._buffer_pos:
            del self._buffer[: self._buffer_pos]
            self._buffer_pos = 0

    def _maybe_compact_buffer(self) -> None:
        if self._buffer_pos >= self._COMPACT_PREFIX and self._buffer_pos >= len(self._buffer) // 2:
            self._compact_buffer()

    def _append_next_chunk(self) -> bool:
        while True:
            item = self._recv_buffer.take_next()
            if item is None:
                self._eof = True
                return False
            index, chunk = item
            if index == RECV_MANY_BUFFER_PRESSURE:
                continue
            if chunk.nbytes:
                self._buffer.extend(chunk)
                chunk.release()
                return True
            self._eof = True
            return False

    def _fill_buffer(self, min_bytes: int) -> None:
        while self._buffer_available() < min_bytes and not self._eof:
            if not self._append_next_chunk():
                return

    def _take_bytes(self, n: int) -> bytes:
        available = self._buffer_available()
        count = min(n, available)
        if count == 0:
            return b""
        start = self._buffer_pos
        chunk = bytes(self._buffer[start : start + count])
        self._buffer_pos += count
        self._maybe_compact_buffer()
        return chunk

    def _take_into(self, view: memoryview, offset: int, n: int) -> int:
        available = self._buffer_available()
        count = min(n, available)
        if count == 0:
            return 0
        start = self._buffer_pos
        view[offset : offset + count] = self._buffer[start : start + count]
        self._buffer_pos += count
        self._maybe_compact_buffer()
        return count

    def read(self, n: int = -1) -> bytes:
        if n == 0:
            return b""
        if n < 0:
            while not self._eof:
                if not self._append_next_chunk():
                    break
            payload = bytes(self._buffer[self._buffer_pos :])
            self._buffer.clear()
            self._buffer_pos = 0
            return payload
        if self._buffer_available():
            return self._take_bytes(min(n, self._buffer_available()))
        if self._eof:
            return b""
        self._append_next_chunk()
        return self._take_bytes(min(n, self._buffer_available()))

    def readinto(self, b: Any) -> int:
        view = memoryview(b).cast("B")
        if not view.nbytes:
            return 0
        if self._eof and not self._buffer_available():
            return 0

        nbytes = view.nbytes
        if self._buffer_available() < nbytes and not self._eof:
            self._fill_buffer(nbytes)

        total = 0
        while total < nbytes and self._buffer_available():
            total += self._take_into(view, total, nbytes - total)
        return total

    def readexactly(self, n: int) -> bytes:
        if n < 0:
            raise ValueError("readexactly size must not be negative")
        if n == 0:
            return b""

        if self._buffer_available() < n and not self._eof:
            self._fill_buffer(n)
        if self._buffer_available() < n:
            partial = bytes(self._buffer[self._buffer_pos :])
            self._buffer.clear()
            self._buffer_pos = 0
            raise asyncio.IncompleteReadError(partial, n)
        return self._take_bytes(n)

    def readline(self) -> bytes:
        while True:
            newline = self._buffer.find(b"\n", self._buffer_pos)
            if newline >= 0:
                return self._take_bytes(newline - self._buffer_pos + 1)
            if self._eof:
                return self._take_bytes(self._buffer_available())
            if self._buffer_available() >= self._limit:
                raise asyncio.LimitOverrunError(
                    "Separator is not found, and chunk exceed the limit",
                    self._buffer_available(),
                )
            if not self._append_next_chunk():
                return self._take_bytes(self._buffer_available())


class _WriterCore:
    def __init__(
        self,
        *,
        send_buffer: SendBuffer,
        sock: socket.socket,
        io: ProactorIOManager,
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


class StreamReader:
    """Native tealet stream reader with synchronous methods."""

    def __init__(
        self,
        *,
        limit: int = _DEFAULT_LIMIT,
        recv_buffer: RecvIterBuffer,
    ) -> None:
        self._core = _ReaderCore(recv_buffer=recv_buffer, limit=limit)

    @property
    def at_eof(self) -> bool:
        return self._core.at_eof

    def close(self) -> None:
        self._core.close()

    def read(self, n: int = -1) -> bytes:
        return self._core.read(n)

    def readinto(self, b: Any) -> int:
        return self._core.readinto(b)

    def readexactly(self, n: int) -> bytes:
        return self._core.readexactly(n)

    def readline(self) -> bytes:
        return self._core.readline()


class AsyncStreamReader:
    """Asyncio-shaped stream reader backed by tealet-blocking socket I/O."""

    def __init__(
        self,
        *,
        limit: int = _DEFAULT_LIMIT,
        recv_buffer: RecvIterBuffer,
    ) -> None:
        self._core = _ReaderCore(recv_buffer=recv_buffer, limit=limit)

    @property
    def at_eof(self) -> bool:
        return self._core.at_eof

    def close(self) -> None:
        self._core.close()

    async def read(self, n: int = -1) -> bytes:
        return self._core.read(n)

    async def readinto(self, b: Any) -> int:
        return self._core.readinto(b)

    async def readexactly(self, n: int) -> bytes:
        return self._core.readexactly(n)

    async def readline(self) -> bytes:
        return self._core.readline()


class StreamWriter:
    """Native tealet stream writer with synchronous methods."""

    def __init__(
        self,
        *,
        send_buffer: SendBuffer,
        sock: socket.socket,
        io: ProactorIOManager,
        reader: StreamReader | None = None,
    ) -> None:
        self._send_buffer = send_buffer
        self._sock = sock
        self._io = io
        self._core = _WriterCore(send_buffer=send_buffer, sock=sock, io=io)
        self._reader = reader

    def get_extra_info(self, name: str, default: Any = None) -> Any:
        return _writer_extra_info(self._sock, name, default)

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
        io: ProactorIOManager,
        reader: AsyncStreamReader | None = None,
    ) -> None:
        self._send_buffer = send_buffer
        self._sock = sock
        self._io = io
        self._core = _WriterCore(send_buffer=send_buffer, sock=sock, io=io)
        self._reader = reader

    def get_extra_info(self, name: str, default: Any = None) -> Any:
        return _writer_extra_info(self._sock, name, default)

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


def _shutdown_stream_writer(
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


_NativeStreamPair: TypeAlias = tuple[StreamReader, StreamWriter]
_AsyncStreamPair: TypeAlias = tuple[AsyncStreamReader, AsyncStreamWriter]
_StreamFactoryArg: TypeAlias = StreamFactory | AsyncStreamFactory | None
_NativeClientHandler: TypeAlias = Callable[[StreamReader, StreamWriter], Any]
_AsyncClientHandler: TypeAlias = Callable[[AsyncStreamReader, AsyncStreamWriter], Coroutine[Any, Any, Any]]
_ClientHandler: TypeAlias = _NativeClientHandler | _AsyncClientHandler


def _open_recv_buffer(
    io: SocketIO,
    sock: socket.socket,
    recv_buffer_pool: Any | None,
) -> RecvIterBuffer:
    if not isinstance(io, ProactorIOManager):
        raise RuntimeError("stream readers require a proactor IO manager")
    return io._open_sock_recv_iter(sock, recv_buffer_pool)


def _open_send_buffer(io: SocketIO, sock: socket.socket) -> SendBuffer:
    if not isinstance(io, ProactorIOManager):
        raise RuntimeError("stream writers require a proactor IO manager")
    return io._open_send_buffer(sock)


def default_stream_factory(
    io: SocketIO,
    sock: socket.socket,
    *,
    limit: int = _DEFAULT_LIMIT,
    recv_buffer_pool: Any | None = None,
) -> tuple[StreamReader, StreamWriter]:
    """Construct the default native stream pair for a connected socket."""

    proactor_io = cast(ProactorIOManager, io)
    recv_buffer = _open_recv_buffer(io, sock, recv_buffer_pool)
    send_buffer = _open_send_buffer(io, sock)
    reader = StreamReader(limit=limit, recv_buffer=recv_buffer)
    writer = StreamWriter(send_buffer=send_buffer, sock=sock, io=proactor_io, reader=reader)
    return reader, writer


def default_async_stream_factory(
    io: SocketIO,
    sock: socket.socket,
    *,
    limit: int = _DEFAULT_LIMIT,
    recv_buffer_pool: Any | None = None,
) -> tuple[AsyncStreamReader, AsyncStreamWriter]:
    """Construct the default asyncio-shaped stream pair for a connected socket."""

    proactor_io = cast(ProactorIOManager, io)
    recv_buffer = _open_recv_buffer(io, sock, recv_buffer_pool)
    send_buffer = _open_send_buffer(io, sock)
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
        limit: int = _DEFAULT_LIMIT,
    ) -> tuple[StreamReader, StreamWriter] | tuple[AsyncStreamReader, AsyncStreamWriter]:
        chosen = pool if pool is not None else io.create_recv_buffer_pool(buffer_size, buffer_count)
        return delegate(io, sock, limit=limit, recv_buffer_pool=chosen)

    if async_:
        return cast(AsyncStreamFactory, factory)
    return cast(StreamFactory, factory)


def _default_server_stream_factory(*, async_: bool) -> StreamFactory | AsyncStreamFactory:
    """Per-connection provided-buffer pools for multi-client listeners."""

    return pooled_default_stream_factory(async_=async_)


def _resolve_scheduler(scheduler: BaseScheduler | None) -> BaseScheduler:
    if scheduler is not None:
        return scheduler
    from .scheduler import get_running_scheduler

    return get_running_scheduler()


def _require_proactor_io(scheduler: BaseScheduler) -> ProactorIOManager:
    """Return ``scheduler.io`` for proactor schedulers or raise with a targeted message."""

    from .proactor import ProactorScheduler
    from .selector import SelectorScheduler

    if isinstance(scheduler, ProactorScheduler):
        proactor_scheduler: SupportsProactorIO = scheduler
        return proactor_scheduler.io
    if isinstance(scheduler, SelectorScheduler):
        raise RuntimeError(SELECTOR_IO_UNSUPPORTED_ERROR)
    raise RuntimeError(IO_UNSUPPORTED_ERROR)


def _open_streams(
    io: SocketIO,
    sock: socket.socket,
    *,
    limit: int = _DEFAULT_LIMIT,
    stream_factory: _StreamFactoryArg = None,
    async_: bool = False,
) -> _NativeStreamPair | _AsyncStreamPair:
    # ``async_`` only selects the default stream factory when ``stream_factory`` is
    # omitted. An explicit factory must already match the intended stream types.
    if stream_factory is None:
        factory = default_async_stream_factory if async_ else default_stream_factory
    else:
        factory = stream_factory
    reader, writer = factory(io, sock, limit=limit)
    if async_:
        return cast(_AsyncStreamPair, (reader, writer))
    return cast(_NativeStreamPair, (reader, writer))


@overload
def open_streams(
    sock: socket.socket,
    *,
    limit: int = _DEFAULT_LIMIT,
    stream_factory: StreamFactory | None = None,
    async_: Literal[False] = False,
) -> tuple[StreamReader, StreamWriter]: ...


@overload
def open_streams(
    sock: socket.socket,
    *,
    limit: int = _DEFAULT_LIMIT,
    stream_factory: AsyncStreamFactory | None = None,
    async_: Literal[True],
) -> tuple[AsyncStreamReader, AsyncStreamWriter]: ...


def open_streams(
    sock: socket.socket,
    *,
    limit: int = _DEFAULT_LIMIT,
    stream_factory: _StreamFactoryArg = None,
    async_: bool = False,
    scheduler: BaseScheduler | None = None,
) -> _NativeStreamPair | _AsyncStreamPair:
    """Wrap a connected non-blocking socket as stream endpoints.

    ``async_=False`` returns native ``StreamReader`` / ``StreamWriter`` pairs;
    ``async_=True`` returns asyncio-shaped ``AsyncStream*`` endpoints. The flag
    only selects the default factory when ``stream_factory`` is omitted.

    Default factories on proactor schedulers receive through ``recv_many`` via
    ``sock_recv_iter`` and the scheduler shared provided-buffer pool. Use
    ``pooled_default_stream_factory`` or a custom ``stream_factory`` for
    dedicated pool sizing.
    """

    return _open_streams(
        _require_proactor_io(_resolve_scheduler(scheduler)),
        sock,
        limit=limit,
        stream_factory=stream_factory,
        async_=async_,
    )


def _connect_tcp_streams(
    scheduler: BaseScheduler,
    addr: tuple[str, int],
    *,
    family: int = socket.AF_UNSPEC,
    proto: int = 0,
    limit: int = _DEFAULT_LIMIT,
    stream_factory: _StreamFactoryArg = None,
    async_: bool = False,
    initial_send: SocketSendBuffer | None = None,
) -> _NativeStreamPair | _AsyncStreamPair:
    io = _require_proactor_io(scheduler)
    # ``ensure_resolved`` fast-paths literal IPv4/IPv6 via ``ipaddr_info`` and
    # falls back to ``scheduler.getaddrinfo()`` for hostnames (executor-backed).
    infos = scheduler.ensure_resolved(
        addr,
        family=family,
        type=socket.SOCK_STREAM,
        proto=proto,
    )
    if not infos:
        raise OSError("getaddrinfo() returned empty list")

    last_error: OSError | None = None
    server_io = cast(ServerIO, io)
    for addr_family, socktype, addr_proto, _canonname, sockaddr in infos:
        try:
            return server_io.sock_create_streams(
                addr_family,
                socktype,
                addr_proto,
                connect_to=sockaddr,
                initial_data=initial_send,
                limit=limit,
                stream_factory=stream_factory,
                async_=async_,
            ).wait()
        except OSError as exc:
            last_error = exc
    if last_error is not None:
        raise last_error
    raise OSError("open_connection failed without address resolution results")


@overload
def open_connection(
    *,
    addr: tuple[str, int],
    family: int = socket.AF_UNSPEC,
    proto: int = 0,
    limit: int = _DEFAULT_LIMIT,
    stream_factory: StreamFactory | None = None,
    initial_send: SocketSendBuffer | None = None,
    async_: Literal[False] = False,
) -> tuple[StreamReader, StreamWriter]: ...


@overload
def open_connection(
    *,
    addr: tuple[str, int],
    family: int = socket.AF_UNSPEC,
    proto: int = 0,
    limit: int = _DEFAULT_LIMIT,
    stream_factory: AsyncStreamFactory | None = None,
    initial_send: SocketSendBuffer | None = None,
    async_: Literal[True],
) -> tuple[AsyncStreamReader, AsyncStreamWriter]: ...


@overload
def open_connection(
    *,
    path: str,
    limit: int = _DEFAULT_LIMIT,
    stream_factory: StreamFactory | None = None,
    initial_send: SocketSendBuffer | None = None,
    async_: Literal[False] = False,
) -> tuple[StreamReader, StreamWriter]: ...


@overload
def open_connection(
    *,
    path: str,
    limit: int = _DEFAULT_LIMIT,
    stream_factory: AsyncStreamFactory | None = None,
    initial_send: SocketSendBuffer | None = None,
    async_: Literal[True],
) -> tuple[AsyncStreamReader, AsyncStreamWriter]: ...


def open_connection(
    *,
    addr: tuple[str, int] | None = None,
    path: str | None = None,
    family: int = socket.AF_UNSPEC,
    proto: int = 0,
    limit: int = _DEFAULT_LIMIT,
    stream_factory: _StreamFactoryArg = None,
    initial_send: SocketSendBuffer | None = None,
    async_: bool = False,
    scheduler: BaseScheduler | None = None,
) -> _NativeStreamPair | _AsyncStreamPair:
    """Connect and return stream endpoints.

    Pass ``addr=(host, port)`` for TCP, or ``path`` for a Unix-domain socket.
    The host may be a hostname or literal IP; resolution goes through
    ``scheduler.ensure_resolved()``, which skips the executor for literal
    addresses and uses ``getaddrinfo`` otherwise. Results are tried in order
    (no happy eyeballs). ``async_=False`` returns native streams;
    ``async_=True`` returns asyncio-shaped streams. The flag only selects the
    default factory when ``stream_factory`` is omitted.

    ``initial_send`` is flushed during the connect chain before streams are
    returned.
    """

    sched = _resolve_scheduler(scheduler)
    if path is not None:
        if addr is not None:
            raise TypeError("open_connection() accepts addr= or path=, not both")
        return _connect_unix_streams(
            sched,
            path,
            limit=limit,
            stream_factory=stream_factory,
            async_=async_,
            initial_send=initial_send,
        )
    if addr is None:
        raise TypeError("open_connection() requires addr= or path=")
    return _connect_tcp_streams(
        sched,
        addr,
        family=family,
        proto=proto,
        limit=limit,
        stream_factory=stream_factory,
        async_=async_,
        initial_send=initial_send,
    )


def _connect_unix_streams(
    scheduler: BaseScheduler,
    path: str,
    *,
    limit: int = _DEFAULT_LIMIT,
    stream_factory: _StreamFactoryArg = None,
    async_: bool = False,
    initial_send: SocketSendBuffer | None = None,
) -> _NativeStreamPair | _AsyncStreamPair:
    if not hasattr(socket, "AF_UNIX"):
        raise RuntimeError("AF_UNIX is not supported on this platform")

    io = cast(ServerIO, _require_proactor_io(scheduler))
    return io.sock_create_streams(
        socket.AF_UNIX,
        socket.SOCK_STREAM,
        connect_to=path,
        initial_data=initial_send,
        limit=limit,
        stream_factory=stream_factory,
        async_=async_,
    ).wait()


def _default_reuse_address() -> bool:
    return os.name == "posix" and sys.platform != "cygwin"


def _set_reuseport(sock: socket.socket) -> None:
    if not hasattr(socket, "SO_REUSEPORT"):
        raise ValueError("reuse_port not supported by socket module")
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    except OSError as exc:
        raise ValueError(
            "reuse_port not supported by socket module, SO_REUSEPORT defined but not implemented."
        ) from exc


def _apply_listen_socket_contract(sock: socket.socket) -> None:
    sock.setblocking(False)
    os.set_inheritable(sock.fileno(), False)


def _prepare_listen_socket(sock: socket.socket, *, backlog: int) -> socket.socket:
    if sock.type != socket.SOCK_STREAM:
        raise ValueError(f"A stream socket was expected, got {sock!r}")
    _apply_listen_socket_contract(sock)
    sock.listen(backlog)
    return sock


def _bind_tcp_socket(
    io: SocketIO,
    addr: tuple[str | None, int],
    *,
    family: int = socket.AF_INET,
    backlog: int,
    reuse_address: bool | None = None,
    reuse_port: bool | None = None,
) -> socket.socket:
    if reuse_address is None:
        reuse_address = _default_reuse_address()
    host, port = addr
    sock = io.sock_create(family, socket.SOCK_STREAM).wait()
    try:
        if reuse_address:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if reuse_port:
            _set_reuseport(sock)
        bind_host = "" if host is None else host
        sock.bind((bind_host, port))
        sock.listen(backlog)
    except OSError:
        sock.close()
        raise
    return sock


def _bind_unix_socket(io: SocketIO, path: str, *, backlog: int) -> socket.socket:
    if not hasattr(socket, "AF_UNIX"):
        raise RuntimeError("AF_UNIX is not supported on this platform")

    try:
        os.unlink(path)
    except FileNotFoundError:
        pass

    sock = io.sock_create(socket.AF_UNIX, socket.SOCK_STREAM).wait()
    try:
        sock.bind(path)
        sock.listen(backlog)
    except OSError:
        sock.close()
        raise
    return sock


class StreamServer:
    """Listening stream server with a scheduler accept-loop tealet.

    ``start_server()`` spawns a tealet that repeatedly ``wait()``s on
    ``accept_many_streams`` (one emulated accept per iteration, or one multishot
    leg until cancel/error). ``close()`` cancels that accept-loop tealet
    synchronously; it does not close listening socket(s) itself. The accept-loop
    tealet wraps its main loop in ``try``/``finally`` so ``CancelledError`` from
    ``cancel()`` runs cleanup that sets ``_closed`` and closes listeners. Handler
    tealets already spawned for accepted connections keep running until they
    finish on their own. Late accepts delivered while shutting down see
    ``_closed`` and are discarded. ``wait_closed()`` blocks until the accept-loop
    tealet has exited and every dispatched handler tealet has finished. Accept
    callbacks are marshalled onto the
    scheduler thread before ``_dispatch_streams()`` runs.

    Use as a context manager to call ``close()`` and ``wait_closed()`` on
    scope exit. ``serve_forever()`` blocks the current tealet until
    ``close()`` is called; pair with ``wait_closed()`` or the context manager
    to drain in-flight handlers.
    """

    _io: SocketIO

    def __init__(
        self,
        scheduler: BaseScheduler,
        sockets: list[socket.socket],
    ) -> None:
        self._scheduler = scheduler
        self._io = _require_proactor_io(scheduler)
        self._sockets = tuple(sockets)
        self._accept_task: Task | None = None
        self._handler_tasks: set[Task] = set()
        self._closed = False

    def __enter__(self) -> StreamServer:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()
        self.wait_closed()

    @property
    def sockets(self) -> tuple[socket.socket, ...]:
        return self._sockets

    @property
    def accept_task(self) -> Task | None:
        """Scheduler tealet running the accept loop, when started."""

        return self._accept_task

    def close(self) -> None:
        """Request shutdown by cancelling the accept-loop tealet.

        Listening socket(s) are closed when that tealet exits. Does not interrupt
        in-flight client handlers. Call ``wait_closed()`` to block until the
        accept-loop tealet and handlers have finished.
        """

        if self._closed:
            return
        accept_task = self._accept_task
        if accept_task is not None and not accept_task.done():
            if get_current() is not None:
                accept_task.cancel()
            else:
                # close() may run from main or a foreign thread after the
                # scheduler has stopped (for example pytest teardown).
                self._scheduler.call_soon_threadsafe(accept_task.cancel)
        # cancel() before the accept tealet's first slice skips its ``finally``.
        if not self._closed:
            self._finish_close()

    def _finish_close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for sock in self._sockets:
            sock.close()

    def _start_accept_loop(
        self,
        sock: socket.socket,
        client_handler: _ClientHandler,
        *,
        limit: int,
        stream_factory: _StreamFactoryArg,
        async_: bool,
    ) -> None:
        io = cast(ServerIO, self._io)

        def accept_loop() -> None:
            try:
                while True:

                    def on_accept(streams: _AcceptedStreams) -> None:
                        if self._closed:
                            _reader, writer = streams
                            _shutdown_stream_writer(writer, best_effort=True)
                            return
                        reader, writer = streams
                        self._dispatch_streams(
                            reader,
                            writer,
                            client_handler=client_handler,
                            async_=async_,
                        )

                    try:
                        io.accept_many_streams(
                            sock,
                            on_accept,
                            limit=limit,
                            stream_factory=stream_factory,
                            async_=async_,
                        ).wait()
                    except (OSError, RuntimeError):
                        if self._closed:
                            return
                        raise
            finally:
                self._finish_close()

        self._accept_task = self._scheduler.spawn(accept_loop)

    def _wait_until_closed(self) -> None:
        while not self._closed:
            self._scheduler.yield_()

    def wait_closed(self) -> None:
        """Block until the accept loop has exited and handlers are done."""

        accept_task = self._accept_task
        if accept_task is not None and not accept_task.done():
            try:
                accept_task.wait()
            except CancelledError:
                pass
        if not self._closed:
            self._finish_close()

        for handler in tuple(self._handler_tasks):
            if not handler.done():
                handler.wait()

    def serve_forever(self) -> None:
        """Block until ``close()`` is called.

        Accept handling is already active from ``start_server()``; this only
        parks the current tealet. It does not install signal handlers — use
        ``tealetio.run()`` / ``Runner`` for that.
        """

        if self._closed:
            raise RuntimeError("server is closed")
        self._wait_until_closed()

    def _dispatch_client(
        self,
        conn: socket.socket,
        *,
        limit: int,
        stream_factory: _StreamFactoryArg,
        client_handler: _ClientHandler,
        async_: bool,
    ) -> None:
        if self._closed:
            conn.close()
            return

        reader, writer = _open_streams(
            self._io,
            conn,
            limit=limit,
            stream_factory=stream_factory,
            async_=async_,
        )
        self._dispatch_streams(reader, writer, client_handler=client_handler, async_=async_)

    def _dispatch_streams(
        self,
        reader: StreamReader | AsyncStreamReader,
        writer: StreamWriter | AsyncStreamWriter,
        *,
        client_handler: _ClientHandler,
        async_: bool,
    ) -> None:
        def dispatch() -> None:
            if self._closed:
                _shutdown_stream_writer(writer, best_effort=True)
                return

            def serve() -> None:
                try:
                    if self._closed:
                        return
                    if async_:
                        run_coro(
                            cast(_AsyncClientHandler, client_handler)(
                                cast(AsyncStreamReader, reader),
                                cast(AsyncStreamWriter, writer),
                            )
                        )
                    else:
                        cast(_NativeClientHandler, client_handler)(
                            cast(StreamReader, reader),
                            cast(StreamWriter, writer),
                        )
                finally:
                    _shutdown_stream_writer(writer)

            try:
                handler_task = self._scheduler.spawn(serve)
            except Exception as spawn_exc:
                _shutdown_stream_writer(writer, best_effort=True)
                raise spawn_exc

            self._handler_tasks.add(handler_task)

            def drop_handler(_task: Task[Any]) -> None:
                self._handler_tasks.discard(handler_task)

            handler_task.add_done_callback(drop_handler)

        self._scheduler.call_soon(dispatch)


def _start_stream_server(
    scheduler: BaseScheduler,
    sock: socket.socket,
    client_handler: _ClientHandler,
    *,
    limit: int = _DEFAULT_LIMIT,
    stream_factory: _StreamFactoryArg = None,
    async_: bool = False,
) -> StreamServer:
    """Start accept handling on a listening socket and return a ``StreamServer``.

    Requires ``ServerIO`` (blocking ``SocketIO`` plus ``proactor`` submission).
    Accepts deliver stream pairs via ``accept_many_streams``; each connection
    arms ``recv_many`` when streams open on the accept delivery thread.
    """

    if stream_factory is None:
        stream_factory = _default_server_stream_factory(async_=async_)

    server = StreamServer(scheduler, [sock])
    server._start_accept_loop(
        sock,
        client_handler,
        limit=limit,
        stream_factory=stream_factory,
        async_=async_,
    )
    return server


def _start_server(
    scheduler: BaseScheduler,
    client_handler: _ClientHandler,
    *,
    addr: tuple[str | None, int] | None = None,
    path: str | None = None,
    sock: socket.socket | None = None,
    family: int = socket.AF_INET,
    backlog: int = 100,
    reuse_address: bool | None = None,
    reuse_port: bool | None = None,
    limit: int = _DEFAULT_LIMIT,
    stream_factory: _StreamFactoryArg = None,
    async_: bool = False,
) -> StreamServer:
    io = _require_proactor_io(scheduler)
    if sock is not None:
        if addr is not None or path is not None:
            raise ValueError("addr/path and sock cannot be specified at the same time")
        listen_sock = _prepare_listen_socket(sock, backlog=backlog)
    elif path is not None:
        if addr is not None:
            raise TypeError("start_server() accepts addr= or path=, not both")
        listen_sock = _bind_unix_socket(io, path, backlog=backlog)
    elif addr is not None:
        listen_sock = _bind_tcp_socket(
            io,
            addr,
            family=family,
            backlog=backlog,
            reuse_address=reuse_address,
            reuse_port=reuse_port,
        )
    else:
        raise TypeError("start_server() requires addr=, path=, or sock=")
    return _start_stream_server(
        scheduler,
        listen_sock,
        client_handler,
        limit=limit,
        stream_factory=stream_factory,
        async_=async_,
    )


@overload
def start_server(
    client_handler: Callable[[StreamReader, StreamWriter], Any],
    *,
    addr: tuple[str | None, int],
    family: int = socket.AF_INET,
    backlog: int = 100,
    reuse_address: bool | None = None,
    reuse_port: bool | None = None,
    limit: int = _DEFAULT_LIMIT,
    stream_factory: StreamFactory | None = None,
    async_: Literal[False] = False,
) -> StreamServer: ...


@overload
def start_server(
    client_handler: Callable[[AsyncStreamReader, AsyncStreamWriter], Coroutine[Any, Any, Any]],
    *,
    addr: tuple[str | None, int],
    family: int = socket.AF_INET,
    backlog: int = 100,
    reuse_address: bool | None = None,
    reuse_port: bool | None = None,
    limit: int = _DEFAULT_LIMIT,
    stream_factory: AsyncStreamFactory | None = None,
    async_: Literal[True],
) -> StreamServer: ...


@overload
def start_server(
    client_handler: Callable[[StreamReader, StreamWriter], Any],
    *,
    path: str,
    backlog: int = 100,
    limit: int = _DEFAULT_LIMIT,
    stream_factory: StreamFactory | None = None,
    async_: Literal[False] = False,
) -> StreamServer: ...


@overload
def start_server(
    client_handler: Callable[[AsyncStreamReader, AsyncStreamWriter], Coroutine[Any, Any, Any]],
    *,
    path: str,
    backlog: int = 100,
    limit: int = _DEFAULT_LIMIT,
    stream_factory: AsyncStreamFactory | None = None,
    async_: Literal[True],
) -> StreamServer: ...


@overload
def start_server(
    client_handler: Callable[[StreamReader, StreamWriter], Any],
    *,
    sock: socket.socket,
    backlog: int = 100,
    limit: int = _DEFAULT_LIMIT,
    stream_factory: StreamFactory | None = None,
    async_: Literal[False] = False,
) -> StreamServer: ...


@overload
def start_server(
    client_handler: Callable[[AsyncStreamReader, AsyncStreamWriter], Coroutine[Any, Any, Any]],
    *,
    sock: socket.socket,
    backlog: int = 100,
    limit: int = _DEFAULT_LIMIT,
    stream_factory: AsyncStreamFactory | None = None,
    async_: Literal[True],
) -> StreamServer: ...


def start_server(
    client_handler: _ClientHandler,
    *,
    addr: tuple[str | None, int] | None = None,
    path: str | None = None,
    sock: socket.socket | None = None,
    family: int = socket.AF_INET,
    backlog: int = 100,
    reuse_address: bool | None = None,
    reuse_port: bool | None = None,
    limit: int = _DEFAULT_LIMIT,
    stream_factory: _StreamFactoryArg = None,
    async_: bool = False,
    scheduler: BaseScheduler | None = None,
) -> StreamServer:
    """Start a stream server that dispatches each accept to ``client_handler``.

    Pass ``addr=(host, port)`` for a TCP listener, ``path`` for Unix-domain, or
    ``sock`` for a caller-prepared stream socket. Use ``addr=(None, port)`` or
    ``addr=("", port)`` to bind all interfaces. When ``sock`` is passed, do not
    also pass ``addr`` or ``path``; the socket is made non-blocking and
    ``listen(backlog)`` is called, matching ``asyncio.loop.create_server()``.
    ``reuse_address`` and ``reuse_port`` apply only when binding via ``addr``;
    when ``reuse_address`` is ``None``, it defaults to ``True`` on POSIX
    platforms other than Cygwin, like asyncio.
    ``async_=False`` uses native stream types and calls the handler directly;
    ``async_=True`` uses asyncio-shaped streams and drives the handler through
    ``run_coro()``. Pair ``async_`` with the handler shape encoded in the
    overloads (sync handler + ``async_=False``, or ``async def`` + ``async_=True``).
    An explicit ``stream_factory`` must match those stream types; ``async_`` only
    picks the default factory when it is omitted. When ``stream_factory`` is
    omitted, ``start_server()`` uses ``pooled_default_stream_factory`` so each
    accepted connection gets its own provided-buffer pool (avoiding shared-pool
    pressure across concurrent clients). ``open_streams()`` / ``open_connection()``
    still default to the scheduler shared pool for single-connection use.

    Accepts use ``scheduler.io.accept_many_streams()`` (``ProactorIOManager``),
    so ``UringProactor`` can service connections through multishot accept when
    the runtime probe allows it. Each accepted connection arms ``recv_many`` on
    the accept delivery thread before the handler callback runs, so inbound data
    can be ingested while the handler is still queued. A peer that connects and
    never sends leaves ``recv_many`` pending without blocking delivery to the
    handler; use handler-side read timeouts or idle policy when that matters.
    Per-connection pools (the default here) bound memory under high accept rates.

    Late accept deliveries can still reach handlers unless you close listeners
    and discard them in the accept callback (``StreamServer`` checks ``_closed``).
    Custom servers using ``scheduler.io.accept_many()`` directly must apply the
    same pattern.

    Accept callbacks are marshalled onto the scheduler thread before handler
    tealets are spawned. Handler exceptions propagate
    in the handler tealet and do not stop the listener. ``spawn()`` failures
    during dispatch are reported through the scheduler exception handler.
    """

    return _start_server(
        _resolve_scheduler(scheduler),
        client_handler,
        addr=addr,
        path=path,
        sock=sock,
        family=family,
        backlog=backlog,
        reuse_address=reuse_address,
        reuse_port=reuse_port,
        limit=limit,
        stream_factory=stream_factory,
        async_=async_,
    )

"""Tealet-native stream helpers with optional asyncio-compatible facades."""

from __future__ import annotations

import asyncio
import os
import socket
from collections.abc import Callable, Coroutine
from typing import Any, Literal, Protocol, TypeVar, overload

from asynkit import coro_drive

from .io_manager import ProactorIOManager
from .locks import Condition
from .operations import ContinuousOperation
from .scheduler import BaseScheduler

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
    "StreamServer",
    "default_stream_factory",
    "default_async_stream_factory",
    "open_connection",
    "open_streams",
    "start_server",
    "run_coro",
]


class StreamFactory(Protocol):
    """Build a native ``(StreamReader, StreamWriter)`` pair for a connected socket."""

    def __call__(
        self,
        io: ProactorIOManager,
        sock: socket.socket,
        *,
        limit: int = _DEFAULT_LIMIT,
    ) -> tuple[StreamReader, StreamWriter]: ...


class AsyncStreamFactory(Protocol):
    """Build an asyncio-shaped ``(AsyncStreamReader, AsyncStreamWriter)`` pair."""

    def __call__(
        self,
        io: ProactorIOManager,
        sock: socket.socket,
        *,
        limit: int = _DEFAULT_LIMIT,
    ) -> tuple[AsyncStreamReader, AsyncStreamWriter]: ...


def run_coro(coro: Coroutine[Any, Any, T]) -> T:
    """Drive an async-stream coroutine without an asyncio event loop.

    ``AsyncStream*`` methods are ``async def`` for handler compatibility, but
    they block through ``scheduler.io.wait_operation`` rather than yielding asyncio
    futures. Unexpected yields surface as ``RuntimeError``.
    """

    def on_yield(value: object) -> object:
        raise RuntimeError(f"tealetio stream coroutine yielded unexpectedly: {value!r}")

    return coro_drive(coro, on_yield)


class SocketTransport:
    """Blocking socket I/O through a scheduler IO facade."""

    def __init__(self, io: ProactorIOManager, sock: socket.socket) -> None:
        self._io = io
        self._sock = sock
        self._closed = False

    @property
    def sock(self) -> socket.socket:
        return self._sock

    def recv(self, n: int) -> bytes:
        return self._io.sock_recv(self._sock, n)

    def recv_into(self, buf: Any) -> int:
        return self._io.sock_recv_into(self._sock, buf)

    def sendall(self, data: bytes | bytearray | memoryview) -> None:
        self._io.sock_sendall(self._sock, data)

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

    def readinto(self, b: Any) -> int:
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
            if len(self._buffer) >= self._limit:
                raise asyncio.LimitOverrunError(
                    "Separator is not found, and chunk exceed the limit",
                    len(self._buffer),
                )
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

    def readinto(self, b: Any) -> int:
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

    async def readinto(self, b: Any) -> int:
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
    io: ProactorIOManager,
    sock: socket.socket,
    *,
    limit: int = _DEFAULT_LIMIT,
) -> tuple[StreamReader, StreamWriter]:
    """Construct the default native stream pair for a connected socket."""

    transport = SocketTransport(io, sock)
    reader = StreamReader(transport, limit=limit)
    writer = StreamWriter(transport, reader)
    return reader, writer


def default_async_stream_factory(
    io: ProactorIOManager,
    sock: socket.socket,
    *,
    limit: int = _DEFAULT_LIMIT,
) -> tuple[AsyncStreamReader, AsyncStreamWriter]:
    """Construct the default asyncio-shaped stream pair for a connected socket."""

    transport = SocketTransport(io, sock)
    reader = AsyncStreamReader(transport, limit=limit)
    writer = AsyncStreamWriter(transport, reader)
    return reader, writer


def _resolve_scheduler(scheduler: BaseScheduler | None) -> BaseScheduler:
    if scheduler is not None:
        return scheduler
    from .scheduler import get_running_scheduler

    return get_running_scheduler()


def _open_streams(
    io: ProactorIOManager,
    sock: socket.socket,
    *,
    limit: int = _DEFAULT_LIMIT,
    stream_factory: Any = None,
    async_: bool = False,
) -> tuple[Any, Any]:
    # ``async_`` only selects the default stream factory when ``stream_factory`` is
    # omitted. An explicit factory must already match the intended stream types.
    if stream_factory is None:
        factory = default_async_stream_factory if async_ else default_stream_factory
    else:
        factory = stream_factory
    return factory(io, sock, limit=limit)


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
    stream_factory: Any = None,
    async_: bool = False,
    scheduler: BaseScheduler | None = None,
) -> tuple[Any, Any]:
    """Wrap a connected non-blocking socket as stream endpoints.

    ``async_=False`` returns native ``StreamReader`` / ``StreamWriter`` pairs;
    ``async_=True`` returns asyncio-shaped ``AsyncStream*`` endpoints. The flag
    only selects the default factory when ``stream_factory`` is omitted.
    """

    return _open_streams(
        _resolve_scheduler(scheduler).io,
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
    stream_factory: Any = None,
    async_: bool = False,
) -> tuple[Any, Any]:
    io = scheduler.io
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
    for addr_family, socktype, addr_proto, _canonname, sockaddr in infos:
        sock: socket.socket | None = None
        try:
            sock = io.sock_create(addr_family, socktype, addr_proto)
            io.sock_connect(sock, sockaddr)
            return _open_streams(
                io,
                sock,
                limit=limit,
                stream_factory=stream_factory,
                async_=async_,
            )
        except OSError as exc:
            last_error = exc
            if sock is not None:
                sock.close()
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
    async_: Literal[True],
) -> tuple[AsyncStreamReader, AsyncStreamWriter]: ...


@overload
def open_connection(
    *,
    path: str,
    limit: int = _DEFAULT_LIMIT,
    stream_factory: StreamFactory | None = None,
    async_: Literal[False] = False,
) -> tuple[StreamReader, StreamWriter]: ...


@overload
def open_connection(
    *,
    path: str,
    limit: int = _DEFAULT_LIMIT,
    stream_factory: AsyncStreamFactory | None = None,
    async_: Literal[True],
) -> tuple[AsyncStreamReader, AsyncStreamWriter]: ...


def open_connection(
    *,
    addr: tuple[str, int] | None = None,
    path: str | None = None,
    family: int = socket.AF_UNSPEC,
    proto: int = 0,
    limit: int = _DEFAULT_LIMIT,
    stream_factory: Any = None,
    async_: bool = False,
    scheduler: BaseScheduler | None = None,
) -> tuple[Any, Any]:
    """Connect and return stream endpoints.

    Pass ``addr=(host, port)`` for TCP, or ``path`` for a Unix-domain socket.
    The host may be a hostname or literal IP; resolution goes through
    ``scheduler.ensure_resolved()``, which skips the executor for literal
    addresses and uses ``getaddrinfo`` otherwise. Results are tried in order
    (no happy eyeballs). ``async_=False`` returns native streams;
    ``async_=True`` returns asyncio-shaped streams. The flag only selects the
    default factory when ``stream_factory`` is omitted.
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
    )


def _connect_unix_streams(
    scheduler: BaseScheduler,
    path: str,
    *,
    limit: int = _DEFAULT_LIMIT,
    stream_factory: Any = None,
    async_: bool = False,
) -> tuple[Any, Any]:
    if not hasattr(socket, "AF_UNIX"):
        raise RuntimeError("AF_UNIX is not supported on this platform")

    io = scheduler.io
    sock = io.sock_create(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        io.sock_connect(sock, path)
    except OSError:
        sock.close()
        raise
    return _open_streams(
        io,
        sock,
        limit=limit,
        stream_factory=stream_factory,
        async_=async_,
    )


def _bind_tcp_socket(
    io: ProactorIOManager,
    addr: tuple[str | None, int],
    *,
    family: int = socket.AF_INET,
    backlog: int,
) -> socket.socket:
    host, port = addr
    sock = io.sock_create(family, socket.SOCK_STREAM)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        bind_host = "" if host is None else host
        sock.bind((bind_host, port))
        sock.listen(backlog)
    except OSError:
        sock.close()
        raise
    return sock


def _bind_unix_socket(io: ProactorIOManager, path: str, *, backlog: int) -> socket.socket:
    if not hasattr(socket, "AF_UNIX"):
        raise RuntimeError("AF_UNIX is not supported on this platform")

    try:
        os.unlink(path)
    except FileNotFoundError:
        pass

    sock = io.sock_create(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.bind(path)
        sock.listen(backlog)
    except OSError:
        sock.close()
        raise
    return sock


class StreamServer:
    """Listening stream server backed by a continuous ``accept_many`` operation.

    ``close()`` stops accepting and closes the listening socket(s). Handler
    tealets already spawned for accepted connections keep running until they
    finish on their own; ``close()`` does not cancel them. ``wait_closed()``
    blocks until the server is closed and every dispatched handler tealet has
    finished. Accept callbacks may run on proactor worker threads. An early,
    lock-free ``_closed`` check in ``_dispatch_client()`` closes stray
    connections without touching ``Condition`` state. Queued ``dispatch()``
    turns re-check ``_closed`` under ``_shutdown`` and close without
    incrementing ``_active_handlers`` when the server is already shut down.

    Use as a context manager to call ``close()`` and ``wait_closed()`` on
    scope exit. ``serve_forever()`` blocks the current tealet until
    ``close()`` is called; pair with ``wait_closed()`` or the context manager
    to drain in-flight handlers.
    """

    def __init__(
        self,
        scheduler: BaseScheduler,
        sockets: list[socket.socket],
        accept_operation: ContinuousOperation[tuple[socket.socket, Any]],
    ) -> None:
        self._scheduler = scheduler
        self._io = scheduler.io
        self._sockets = tuple(sockets)
        self._accept_operation = accept_operation
        self._shutdown = Condition()
        self._closed = False
        self._active_handlers = 0

    def __enter__(self) -> StreamServer:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()
        self.wait_closed()

    @property
    def sockets(self) -> tuple[socket.socket, ...]:
        return self._sockets

    @property
    def accept_operation(self) -> ContinuousOperation[tuple[socket.socket, Any]]:
        return self._accept_operation

    def close(self) -> None:
        """Stop accepting and close listening socket(s).

        Does not interrupt in-flight client handlers. Call ``wait_closed()`` to
        block until they finish.
        """

        if self._closed:
            return
        with self._shutdown:
            self._closed = True
            self._shutdown.notify_all()
        self._accept_operation.cancel()
        for sock in self._sockets:
            sock.close()

    def wait_closed(self) -> None:
        """Block until this server is closed and all handler tealets have exited."""

        with self._shutdown:
            self._shutdown.swait_for(lambda: self._closed and self._active_handlers == 0)

    def serve_forever(self) -> None:
        """Block until ``close()`` is called.

        Accept handling is already active from ``start_server()``; this only
        parks the current tealet. It does not install signal handlers — use
        ``tealetio.run()`` / ``Runner`` for that.
        """

        if self._closed:
            raise RuntimeError("server is closed")
        with self._shutdown:
            self._shutdown.swait_for(lambda: self._closed)

    def _dispatch_client(
        self,
        conn: socket.socket,
        *,
        limit: int,
        stream_factory: Any,
        client_handler: Callable[..., Any],
        async_: bool,
    ) -> None:
        if self._closed:
            conn.close()
            return

        def dispatch() -> None:
            def serve() -> None:
                writer: Any = None
                try:
                    with self._shutdown:
                        if self._closed:
                            return
                    reader, writer = _open_streams(
                        self._io,
                        conn,
                        limit=limit,
                        stream_factory=stream_factory,
                        async_=async_,
                    )
                    if async_:
                        run_coro(client_handler(reader, writer))
                    else:
                        client_handler(reader, writer)
                finally:
                    if writer is not None:
                        writer.close()
                    else:
                        conn.close()
                    with self._shutdown:
                        self._active_handlers -= 1
                        self._shutdown.notify_all()

            with self._shutdown:
                if self._closed:
                    conn.close()
                    return
                self._active_handlers += 1

            try:
                self._scheduler.spawn(serve)
            except Exception:
                with self._shutdown:
                    self._active_handlers -= 1
                    self._shutdown.notify_all()
                conn.close()
                raise

        self._scheduler.call_soon_threadsafe(dispatch)


def _start_stream_server(
    scheduler: BaseScheduler,
    sock: socket.socket,
    client_handler: Callable[..., Any],
    *,
    limit: int = _DEFAULT_LIMIT,
    stream_factory: Any = None,
    async_: bool = False,
) -> StreamServer:
    server: StreamServer | None = None

    def on_accept(accepted: tuple[socket.socket, Any]) -> None:
        conn, _address = accepted
        if server is None:
            # accept_many may deliver on a worker thread before StreamServer
            # exists; drop the connection (extremely unlikely race).
            conn.close()
            return
        server._dispatch_client(
            conn,
            limit=limit,
            stream_factory=stream_factory,
            client_handler=client_handler,
            async_=async_,
        )

    io = scheduler.io
    accept_operation = io.proactor.accept_many(sock, on_accept)
    server = StreamServer(scheduler, [sock], accept_operation)
    return server


def _start_server(
    scheduler: BaseScheduler,
    client_handler: Callable[..., Any],
    *,
    addr: tuple[str | None, int] | None = None,
    path: str | None = None,
    family: int = socket.AF_INET,
    backlog: int = 100,
    limit: int = _DEFAULT_LIMIT,
    stream_factory: Any = None,
    async_: bool = False,
) -> StreamServer:
    io = scheduler.io
    if path is not None:
        if addr is not None:
            raise TypeError("start_server() accepts addr= or path=, not both")
        sock = _bind_unix_socket(io, path, backlog=backlog)
    elif addr is not None:
        sock = _bind_tcp_socket(io, addr, family=family, backlog=backlog)
    else:
        raise TypeError("start_server() requires addr= or path=")
    return _start_stream_server(
        scheduler,
        sock,
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


def start_server(
    client_handler: Callable[..., Any],
    *,
    addr: tuple[str | None, int] | None = None,
    path: str | None = None,
    family: int = socket.AF_INET,
    backlog: int = 100,
    limit: int = _DEFAULT_LIMIT,
    stream_factory: Any = None,
    async_: bool = False,
    scheduler: BaseScheduler | None = None,
) -> StreamServer:
    """Start a stream server that dispatches each accept to ``client_handler``.

    Pass ``addr=(host, port)`` for a TCP listener, or ``path`` for Unix-domain.
    Use ``addr=(None, port)`` or ``addr=("", port)`` to bind all interfaces.
    ``async_=False`` uses native stream types and calls the handler directly;
    ``async_=True`` uses asyncio-shaped streams and drives the handler through
    ``run_coro()``. Pair ``async_`` with the handler shape encoded in the
    overloads (sync handler + ``async_=False``, or ``async def`` + ``async_=True``).
    An explicit ``stream_factory`` must match those stream types; ``async_`` only
    picks the default factory when it is omitted.

    Accepts use ``proactor.accept_many()``, so ``UringProactor`` can service
    connections through multishot accept when the runtime probe allows it.
    Handler tealets are spawned through ``call_soon_threadsafe`` because accept
    callbacks may run on completion worker threads. Handler exceptions propagate
    in the handler tealet and do not stop the listener. ``spawn()`` failures
    during dispatch are reported through the scheduler exception handler.
    """

    return _start_server(
        _resolve_scheduler(scheduler),
        client_handler,
        addr=addr,
        path=path,
        family=family,
        backlog=backlog,
        limit=limit,
        stream_factory=stream_factory,
        async_=async_,
    )

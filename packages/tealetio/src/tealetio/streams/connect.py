"""Connect helpers and public ``open_streams`` / ``open_connection`` entry points."""

from __future__ import annotations

import socket
import ssl
from typing import Literal, cast, overload

from ..happy_eyeballs import HAPPY_EYEBALLS_DELAY
from ..io_manager import ProactorIOManager, SocketSendBuffer
from ..scheduler import BaseScheduler
from ..socket_helpers import abortive_close
from .common import require_proactor_io, resolve_scheduler
from .open import (
    AsyncStreamFactory,
    AsyncStreamPair,
    NativeStreamPair,
    StreamFactory,
    StreamFactoryArg,
)
from .open import (
    open_streams as build_streams,
)
from .reader import AsyncStreamReader, ReadStream
from .ssl import _client_ssl_params, _require_native_ssl, check_ssl_handshake_timeout, ssl_stream_factory
from .util import DEFAULT_LIMIT
from .writer import AsyncStreamWriter, WriteStream, shutdown_stream_writer


@overload
def open_streams(
    sock: socket.socket,
    *,
    limit: int = DEFAULT_LIMIT,
    stream_factory: StreamFactory | None = None,
    async_: Literal[False] = False,
) -> tuple[ReadStream, WriteStream]: ...


@overload
def open_streams(
    sock: socket.socket,
    *,
    limit: int = DEFAULT_LIMIT,
    stream_factory: AsyncStreamFactory | None = None,
    async_: Literal[True],
) -> tuple[AsyncStreamReader, AsyncStreamWriter]: ...


def open_streams(
    sock: socket.socket,
    *,
    limit: int = DEFAULT_LIMIT,
    stream_factory: StreamFactoryArg = None,
    async_: bool = False,
    scheduler: BaseScheduler | None = None,
) -> NativeStreamPair | AsyncStreamPair:
    """Wrap a connected non-blocking socket as stream endpoints.

    ``async_=False`` returns a native ``(ReadStream, WriteStream)`` pair
    (default concrete types are ``StreamReader`` / ``StreamWriter``);
    ``async_=True`` returns asyncio-shaped ``AsyncStream*`` endpoints. The flag
    only selects the default factory when ``stream_factory`` is omitted.

    Default factories on proactor schedulers receive through ``recv_many`` via
    ``sock_recv_iter`` and the scheduler shared provided-buffer pool. Use
    ``pooled_default_stream_factory`` or a custom ``stream_factory`` for
    dedicated pool sizing.
    """

    return build_streams(
        require_proactor_io(resolve_scheduler(scheduler)),
        sock,
        limit=limit,
        stream_factory=stream_factory,
        async_=async_,
    )


def _handshake_connected_pair(
    pair: NativeStreamPair | AsyncStreamPair,
    *,
    ssl_handshake_timeout: float | None = None,
) -> NativeStreamPair | AsyncStreamPair:
    """Run ``writer.handshake()`` on the connecting tealet after streams exist.

    Plaintext ``handshake()`` is a no-op; TLS does the record-layer handshake.
    """

    _reader, writer = pair
    try:
        writer.handshake(timeout=ssl_handshake_timeout)
    except BaseException:
        shutdown_stream_writer(writer, best_effort=True)
        raise
    return pair


def _apply_client_ssl_factory(
    stream_factory: StreamFactoryArg,
    ssl_arg: ssl.SSLContext | bool | None,
    *,
    server_hostname: str | None,
    host: str | None,
    async_: bool,
    initial_send: SocketSendBuffer | None,
) -> StreamFactoryArg:
    """Install ``ssl_stream_factory`` when ``ssl=`` is set.

    ``initial_send`` is written on the connection before this factory runs,
    so a context rejects it.
    """

    context, hostname = _client_ssl_params(ssl_arg, server_hostname=server_hostname, host=host)
    if context is None:
        return stream_factory
    _require_native_ssl(async_=async_)
    if initial_send is not None:
        raise ValueError("initial_send is not supported with ssl")
    inner = None if stream_factory is None else cast(StreamFactory, stream_factory)
    return ssl_stream_factory(context, server_side=False, server_hostname=hostname, inner=inner)


def _streams_on_socket(
    io: ProactorIOManager,
    sock: socket.socket,
    *,
    initial_send: SocketSendBuffer | None,
    limit: int,
    stream_factory: StreamFactoryArg,
    async_: bool,
    ssl_handshake_timeout: float | None,
) -> NativeStreamPair | AsyncStreamPair:
    """Send ``initial_send`` once, open one stream, then handshake on this tealet."""

    try:
        if initial_send:
            io.sock_sendall(sock, initial_send).wait()
        pair = build_streams(
            io,
            sock,
            limit=limit,
            stream_factory=stream_factory,
            async_=async_,
        )
    except BaseException:
        abortive_close(sock)
        raise
    return _handshake_connected_pair(pair, ssl_handshake_timeout=ssl_handshake_timeout)


def connect_tcp_streams(
    scheduler: BaseScheduler,
    addr: tuple[str, int],
    *,
    family: int = socket.AF_UNSPEC,
    proto: int = 0,
    limit: int = DEFAULT_LIMIT,
    stream_factory: StreamFactoryArg = None,
    async_: bool = False,
    initial_send: SocketSendBuffer | None = None,
    ssl: ssl.SSLContext | bool | None = None,
    server_hostname: str | None = None,
    ssl_handshake_timeout: float | None = None,
    happy_eyeballs: bool = True,
    happy_eyeballs_delay: float | None = HAPPY_EYEBALLS_DELAY,
    interleave: int | None = None,
) -> NativeStreamPair | AsyncStreamPair:
    check_ssl_handshake_timeout(ssl, ssl_handshake_timeout)
    stream_factory = _apply_client_ssl_factory(
        stream_factory,
        ssl,
        server_hostname=server_hostname,
        host=addr[0],
        async_=async_,
        initial_send=initial_send,
    )
    # TCP only: TLS handshake is ssl.SSLError / TimeoutError (OSError
    # subclasses) and must not retry the next A/AAAA record. The race is
    # over, and the handshake runs on the winner.
    io = require_proactor_io(scheduler)
    sock = io.connect_tcp(
        addr,
        family=family,
        proto=proto,
        happy_eyeballs=happy_eyeballs,
        happy_eyeballs_delay=happy_eyeballs_delay,
        interleave=interleave,
    )
    return _streams_on_socket(
        io,
        sock,
        initial_send=initial_send,
        limit=limit,
        stream_factory=stream_factory,
        async_=async_,
        ssl_handshake_timeout=ssl_handshake_timeout if ssl else None,
    )


def connect_unix_streams(
    scheduler: BaseScheduler,
    path: str,
    *,
    limit: int = DEFAULT_LIMIT,
    stream_factory: StreamFactoryArg = None,
    async_: bool = False,
    initial_send: SocketSendBuffer | None = None,
    ssl: ssl.SSLContext | bool | None = None,
    server_hostname: str | None = None,
    ssl_handshake_timeout: float | None = None,
) -> NativeStreamPair | AsyncStreamPair:
    if not hasattr(socket, "AF_UNIX"):
        raise RuntimeError("AF_UNIX is not supported on this platform")

    check_ssl_handshake_timeout(ssl, ssl_handshake_timeout)
    stream_factory = _apply_client_ssl_factory(
        stream_factory,
        ssl,
        server_hostname=server_hostname,
        host=None,
        async_=async_,
        initial_send=initial_send,
    )
    io = require_proactor_io(scheduler)
    sock = io.sock_create(socket.AF_UNIX, socket.SOCK_STREAM, connect_to=path).wait()
    return _streams_on_socket(
        io,
        sock,
        initial_send=initial_send,
        limit=limit,
        stream_factory=stream_factory,
        async_=async_,
        ssl_handshake_timeout=ssl_handshake_timeout if ssl else None,
    )


@overload
def open_connection(
    *,
    addr: tuple[str, int],
    family: int = socket.AF_UNSPEC,
    proto: int = 0,
    limit: int = DEFAULT_LIMIT,
    stream_factory: StreamFactory | None = None,
    initial_send: SocketSendBuffer | None = None,
    async_: Literal[False] = False,
    ssl: ssl.SSLContext | bool | None = None,
    server_hostname: str | None = None,
    ssl_handshake_timeout: float | None = None,
    happy_eyeballs: bool = True,
    happy_eyeballs_delay: float | None = HAPPY_EYEBALLS_DELAY,
    interleave: int | None = None,
) -> tuple[ReadStream, WriteStream]: ...


@overload
def open_connection(
    *,
    addr: tuple[str, int],
    family: int = socket.AF_UNSPEC,
    proto: int = 0,
    limit: int = DEFAULT_LIMIT,
    stream_factory: AsyncStreamFactory | None = None,
    initial_send: SocketSendBuffer | None = None,
    async_: Literal[True],
    happy_eyeballs: bool = True,
    happy_eyeballs_delay: float | None = HAPPY_EYEBALLS_DELAY,
    interleave: int | None = None,
) -> tuple[AsyncStreamReader, AsyncStreamWriter]: ...


@overload
def open_connection(
    *,
    path: str,
    limit: int = DEFAULT_LIMIT,
    stream_factory: StreamFactory | None = None,
    initial_send: SocketSendBuffer | None = None,
    async_: Literal[False] = False,
    ssl: ssl.SSLContext | bool | None = None,
    server_hostname: str | None = None,
    ssl_handshake_timeout: float | None = None,
) -> tuple[ReadStream, WriteStream]: ...


@overload
def open_connection(
    *,
    path: str,
    limit: int = DEFAULT_LIMIT,
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
    limit: int = DEFAULT_LIMIT,
    stream_factory: StreamFactoryArg = None,
    initial_send: SocketSendBuffer | None = None,
    async_: bool = False,
    ssl: ssl.SSLContext | bool | None = None,
    server_hostname: str | None = None,
    ssl_handshake_timeout: float | None = None,
    happy_eyeballs: bool = True,
    happy_eyeballs_delay: float | None = HAPPY_EYEBALLS_DELAY,
    interleave: int | None = None,
    scheduler: BaseScheduler | None = None,
) -> NativeStreamPair | AsyncStreamPair:
    """Connect and return stream endpoints.

    Pass ``addr=(host, port)`` for TCP, or ``path`` for a Unix-domain socket.
    The host may be a hostname or literal IP; resolution goes through
    ``scheduler.ensure_resolved()``, which skips the executor for literal
    addresses and uses ``getaddrinfo`` otherwise. TCP goes through
    ``connect_tcp`` (RFC 8305 happy eyeballs, default delay 0.25s): a failed
    attempt starts the next immediately, and the first connected socket wins.
    One stream is opened on that socket. Pass ``happy_eyeballs=False`` to
    walk ``getaddrinfo`` order one address at a time and raise the last
    error, as ``socket.create_connection`` does. Pass
    ``happy_eyeballs_delay=None`` for sequential tries that can still be
    reordered with ``interleave``. When the race is on, ``interleave``
    defaults to 1 (round-robin address families, matching asyncio). Pass
    ``interleave=0`` to keep ``getaddrinfo`` order.
    ``async_=False`` returns native streams; ``async_=True`` returns asyncio-shaped
    streams. The flag only selects the default factory when ``stream_factory``
    is omitted.

    ``ssl`` matches asyncio: ``True`` uses ``ssl.create_default_context()``, an
    ``SSLContext`` is used as-is, and ``server_hostname`` defaults to the
    ``addr`` host. ``ssl=`` is native-only (not ``async_=True``). ``writer.handshake()``
    runs on this tealet before the pair is returned, with
    ``ssl_handshake_timeout`` (default 60s, asyncio-shaped). ``initial_send`` is
    written once on the connected socket before the stream opens and before
    any TLS handshake, so it cannot be combined with ``ssl``. It is the first
    write of a protocol that has no TLS.
    """

    sched = resolve_scheduler(scheduler)
    if path is not None:
        if addr is not None:
            raise TypeError("open_connection() accepts addr= or path=, not both")
        return connect_unix_streams(
            sched,
            path,
            limit=limit,
            stream_factory=stream_factory,
            async_=async_,
            initial_send=initial_send,
            ssl=ssl,
            server_hostname=server_hostname,
            ssl_handshake_timeout=ssl_handshake_timeout,
        )
    if addr is None:
        raise TypeError("open_connection() requires addr= or path=")
    return connect_tcp_streams(
        sched,
        addr,
        family=family,
        proto=proto,
        limit=limit,
        stream_factory=stream_factory,
        async_=async_,
        initial_send=initial_send,
        ssl=ssl,
        server_hostname=server_hostname,
        ssl_handshake_timeout=ssl_handshake_timeout,
        happy_eyeballs=happy_eyeballs,
        happy_eyeballs_delay=happy_eyeballs_delay,
        interleave=interleave,
    )

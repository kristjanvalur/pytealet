"""Connect helpers and public ``open_streams`` / ``open_connection`` entry points."""

from __future__ import annotations

import ssl
from typing import Any, Literal, cast, overload

from ..happy_eyeballs import HAPPY_EYEBALLS_DELAY
from ..io_manager import ProactorIOManager, SocketSendBuffer
from ..scheduler import BaseScheduler
from ..socket_helpers import abortive_close, socket
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
from .ssl import (
    _client_ssl_params,
    _prepare_client_handshake,
    _require_native_ssl,
    _ssl_factory_with_handshake,
    check_ssl_handshake_timeout,
)
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
) -> tuple[StreamFactoryArg, SocketSendBuffer | None]:
    """Return the stream factory and the bytes to send with connect.

    With ``ssl=``, the client handshake's first flight is that payload.
    The factory continues the same ``SSLObject``. A caller ``initial_send``
    is not a TLS record, so it is rejected.
    """

    context, hostname = _client_ssl_params(ssl_arg, server_hostname=server_hostname, host=host)
    if context is None:
        return stream_factory, initial_send
    _require_native_ssl(async_=async_)
    if initial_send is not None:
        raise ValueError("initial_send is not supported with ssl")
    started = _prepare_client_handshake(context, hostname)
    inner = None if stream_factory is None else cast(StreamFactory, stream_factory)
    factory = _ssl_factory_with_handshake(
        context,
        server_hostname=hostname,
        started=started,
        inner=inner,
    )
    return factory, started.flight or None


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
    fastopen: bool = False,
    ssl: ssl.SSLContext | bool | None = None,
    server_hostname: str | None = None,
    ssl_handshake_timeout: float | None = None,
    happy_eyeballs: bool = True,
    happy_eyeballs_delay: float | None = HAPPY_EYEBALLS_DELAY,
    interleave: int | None = None,
    source_address: tuple[Any, ...] | None = None,
) -> NativeStreamPair | AsyncStreamPair:
    check_ssl_handshake_timeout(ssl, ssl_handshake_timeout)
    stream_factory, connect_initial = _apply_client_ssl_factory(
        stream_factory,
        ssl,
        server_hostname=server_hostname,
        host=addr[0],
        async_=async_,
        initial_send=initial_send,
    )
    # TCP only: TLS handshake is ssl.SSLError / TimeoutError (OSError
    # subclasses) and must not retry the next A/AAAA record. The race is
    # over, and the rest of the handshake runs on the winner. The first
    # flight, when ssl= is set, is already connect_initial.
    io = require_proactor_io(scheduler)
    sock = io.connect_tcp(
        addr,
        family=family,
        proto=proto,
        happy_eyeballs=happy_eyeballs,
        happy_eyeballs_delay=happy_eyeballs_delay,
        interleave=interleave,
        source_address=source_address,
        initial=connect_initial,
        fastopen=fastopen,
    )
    return _streams_on_socket(
        io,
        sock,
        initial_send=None,
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
    fastopen: bool = False,
    ssl: ssl.SSLContext | bool | None = None,
    server_hostname: str | None = None,
    ssl_handshake_timeout: float | None = None,
) -> NativeStreamPair | AsyncStreamPair:
    if not hasattr(socket, "AF_UNIX"):
        raise RuntimeError("AF_UNIX is not supported on this platform")

    check_ssl_handshake_timeout(ssl, ssl_handshake_timeout)
    stream_factory, connect_initial = _apply_client_ssl_factory(
        stream_factory,
        ssl,
        server_hostname=server_hostname,
        host=None,
        async_=async_,
        initial_send=initial_send,
    )
    io = require_proactor_io(scheduler)
    sock = io.sock_create(
        socket.AF_UNIX,
        socket.SOCK_STREAM,
        connect_to=path,
        initial_data=connect_initial,
        fastopen=fastopen,
    ).wait()
    return _streams_on_socket(
        io,
        sock,
        initial_send=None,
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
    fastopen: bool = False,
    async_: Literal[False] = False,
    ssl: ssl.SSLContext | bool | None = None,
    server_hostname: str | None = None,
    ssl_handshake_timeout: float | None = None,
    happy_eyeballs: bool = True,
    happy_eyeballs_delay: float | None = HAPPY_EYEBALLS_DELAY,
    interleave: int | None = None,
    source_address: tuple[Any, ...] | None = None,
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
    fastopen: bool = False,
    async_: Literal[True],
    happy_eyeballs: bool = True,
    happy_eyeballs_delay: float | None = HAPPY_EYEBALLS_DELAY,
    interleave: int | None = None,
    source_address: tuple[Any, ...] | None = None,
) -> tuple[AsyncStreamReader, AsyncStreamWriter]: ...


@overload
def open_connection(
    *,
    path: str,
    limit: int = DEFAULT_LIMIT,
    stream_factory: StreamFactory | None = None,
    initial_send: SocketSendBuffer | None = None,
    fastopen: bool = False,
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
    fastopen: bool = False,
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
    fastopen: bool = False,
    async_: bool = False,
    ssl: ssl.SSLContext | bool | None = None,
    server_hostname: str | None = None,
    ssl_handshake_timeout: float | None = None,
    happy_eyeballs: bool = True,
    happy_eyeballs_delay: float | None = HAPPY_EYEBALLS_DELAY,
    interleave: int | None = None,
    source_address: tuple[Any, ...] | None = None,
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
    ``source_address`` is passed to ``connect_tcp``.
    ``async_=False`` returns native streams; ``async_=True`` returns asyncio-shaped
    streams. The flag only selects the default factory when ``stream_factory``
    is omitted.

    ``ssl`` matches asyncio: ``True`` uses ``ssl.create_default_context()``, an
    ``SSLContext`` is used as-is, and ``server_hostname`` defaults to the
    ``addr`` host. ``ssl=`` is native-only (not ``async_=True``). Before
    connect, the client handshake runs on a ``MemoryBIO`` until it needs the
    peer. That first flight is the connect's initial data, whether or not
    ``fastopen`` is set. ``writer.handshake()`` continues the same
    ``SSLObject`` on this tealet before the pair is returned, with
    ``ssl_handshake_timeout`` (default 60s, asyncio-shaped). A caller
    ``initial_send`` cannot be combined with ``ssl``: those bytes would sit
    outside the TLS session. Without ``ssl``, ``initial_send`` is written on
    the winner before the stream opens. The default connects, then writes
    the whole buffer. ``fastopen=True`` races ``sock_fastopen``: a cached
    cookie may put a prefix on the SYN, and the winner writes the rest. A
    Unix path passes the bytes to ``sock_create``.
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
            fastopen=fastopen,
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
        fastopen=fastopen,
        ssl=ssl,
        server_hostname=server_hostname,
        ssl_handshake_timeout=ssl_handshake_timeout,
        happy_eyeballs=happy_eyeballs,
        happy_eyeballs_delay=happy_eyeballs_delay,
        interleave=interleave,
        source_address=source_address,
    )

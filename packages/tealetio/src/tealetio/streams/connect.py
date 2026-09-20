"""Connect helpers and public ``open_streams`` / ``open_connection`` entry points."""

from __future__ import annotations

import socket
import ssl
from typing import Any, Literal, cast, overload

from ..io_manager import ServerIO, SocketSendBuffer
from ..locks import Event, TimeoutError, timeout
from ..scheduler import BaseScheduler
from ..taskgroups import ExceptionGroup, TaskGroup
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

HAPPY_EYEBALLS_DELAY = 0.25

_AddrInfo = tuple[int, int, int, str, tuple[Any, ...]]


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

    Stream factories run on the connect completion worker and must not park.
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
    context, hostname = _client_ssl_params(ssl_arg, server_hostname=server_hostname, host=host)
    if context is None:
        return stream_factory
    _require_native_ssl(async_=async_)
    if initial_send is not None:
        raise ValueError("initial_send is not supported with ssl")
    inner = None if stream_factory is None else cast(StreamFactory, stream_factory)
    return ssl_stream_factory(context, server_side=False, server_hostname=hostname, inner=inner)


def _sock_create_stream_pair(
    server_io: ServerIO,
    info: _AddrInfo,
    *,
    initial_send: SocketSendBuffer | None,
    limit: int,
    stream_factory: StreamFactoryArg,
    async_: bool,
) -> NativeStreamPair | AsyncStreamPair:
    addr_family, socktype, addr_proto, _canonname, sockaddr = info
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


def _connect_tcp_sequential(
    server_io: ServerIO,
    infos: list[_AddrInfo],
    *,
    initial_send: SocketSendBuffer | None,
    limit: int,
    stream_factory: StreamFactoryArg,
    async_: bool,
) -> NativeStreamPair | AsyncStreamPair:
    last_error: OSError | None = None
    for info in infos:
        try:
            return _sock_create_stream_pair(
                server_io,
                info,
                initial_send=initial_send,
                limit=limit,
                stream_factory=stream_factory,
                async_=async_,
            )
        except OSError as exc:
            last_error = exc
    if last_error is not None:
        raise last_error
    raise OSError("open_connection failed without address resolution results")


def _connect_tcp_happy_eyeballs(
    server_io: ServerIO,
    infos: list[_AddrInfo],
    *,
    delay: float,
    initial_send: SocketSendBuffer | None,
    limit: int,
    stream_factory: StreamFactoryArg,
    async_: bool,
) -> NativeStreamPair | AsyncStreamPair:
    winner: NativeStreamPair | AsyncStreamPair | None = None
    errors: list[OSError] = []

    with TaskGroup() as group:
        for index, info in enumerate(infos):
            if winner is not None:
                break
            finished = Event()

            def attempt(info: _AddrInfo = info, finished: Event = finished) -> None:
                nonlocal winner
                try:
                    pair = _sock_create_stream_pair(
                        server_io,
                        info,
                        initial_send=initial_send,
                        limit=limit,
                        stream_factory=stream_factory,
                        async_=async_,
                    )
                except OSError as exc:
                    errors.append(exc)
                    finished.set()
                    return
                if winner is None:
                    winner = pair
                    group.cancel()
                else:
                    _reader, writer = pair
                    shutdown_stream_writer(writer, best_effort=True)
                finished.set()

            group.spawn(attempt)
            if winner is not None or index + 1 >= len(infos):
                break
            try:
                with timeout(delay):
                    finished.swait()
            except TimeoutError:
                pass
        if winner is not None:
            group.cancel()

    if winner is not None:
        return winner
    if len(errors) == 1:
        raise errors[0]
    if errors:
        raise ExceptionGroup("open_connection failed", errors)
    raise OSError("open_connection failed without address resolution results")


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
    happy_eyeballs_delay: float | None = HAPPY_EYEBALLS_DELAY,
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
    io = require_proactor_io(scheduler)
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

    server_io = cast(ServerIO, io)
    if happy_eyeballs_delay is not None and happy_eyeballs_delay < 0:
        raise ValueError("happy_eyeballs_delay must be None or >= 0")
    # TCP only: TLS handshake is ssl.SSLError / TimeoutError (OSError
    # subclasses) and must not retry the next A/AAAA record.
    if happy_eyeballs_delay is None or len(infos) == 1:
        pair = _connect_tcp_sequential(
            server_io,
            infos,
            initial_send=initial_send,
            limit=limit,
            stream_factory=stream_factory,
            async_=async_,
        )
    else:
        pair = _connect_tcp_happy_eyeballs(
            server_io,
            infos,
            delay=happy_eyeballs_delay,
            initial_send=initial_send,
            limit=limit,
            stream_factory=stream_factory,
            async_=async_,
        )
    return _handshake_connected_pair(
        pair,
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
    io = cast(ServerIO, require_proactor_io(scheduler))
    return _handshake_connected_pair(
        io.sock_create_streams(
            socket.AF_UNIX,
            socket.SOCK_STREAM,
            connect_to=path,
            initial_data=initial_send,
            limit=limit,
            stream_factory=stream_factory,
            async_=async_,
        ).wait(),
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
    happy_eyeballs_delay: float | None = HAPPY_EYEBALLS_DELAY,
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
    happy_eyeballs_delay: float | None = HAPPY_EYEBALLS_DELAY,
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
    happy_eyeballs_delay: float | None = HAPPY_EYEBALLS_DELAY,
    scheduler: BaseScheduler | None = None,
) -> NativeStreamPair | AsyncStreamPair:
    """Connect and return stream endpoints.

    Pass ``addr=(host, port)`` for TCP, or ``path`` for a Unix-domain socket.
    The host may be a hostname or literal IP; resolution goes through
    ``scheduler.ensure_resolved()``, which skips the executor for literal
    addresses and uses ``getaddrinfo`` otherwise. TCP addresses are tried with
    RFC 8305 happy eyeballs (default delay 0.25s): a failed attempt starts the
    next immediately, and the first success cancels the rest. Pass
    ``happy_eyeballs_delay=None`` for sequential tries. ``async_=False`` returns
    native streams; ``async_=True`` returns asyncio-shaped streams. The flag
    only selects the default factory when ``stream_factory`` is omitted.

    ``ssl`` matches asyncio: ``True`` uses ``ssl.create_default_context()``, an
    ``SSLContext`` is used as-is, and ``server_hostname`` defaults to the
    ``addr`` host. ``ssl=`` is native-only (not ``async_=True``). ``writer.handshake()``
    runs on this tealet before the pair is returned, with
    ``ssl_handshake_timeout`` (default 60s, asyncio-shaped). ``initial_send`` is TCP
    payload before TLS and cannot be combined with ``ssl``.

    ``initial_send`` is flushed during the connect chain before streams are
    returned.
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
        happy_eyeballs_delay=happy_eyeballs_delay,
    )

"""Connect helpers and public ``open_streams`` / ``open_connection`` entry points."""

from __future__ import annotations

import socket
import ssl
from typing import Literal, cast, overload

from ..io_manager import ServerIO, SocketSendBuffer
from ..scheduler import BaseScheduler
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
from .ssl import _client_ssl_params, _require_native_ssl, ssl_stream_factory
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
) -> NativeStreamPair | AsyncStreamPair:
    """Run ``writer.handshake()`` on the connecting tealet after streams exist.

    Stream factories run on the connect completion worker and must not park.
    Plaintext ``handshake()`` is a no-op; TLS does the record-layer handshake.
    """

    _reader, writer = pair
    try:
        writer.handshake()
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
) -> NativeStreamPair | AsyncStreamPair:
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

    last_error: OSError | None = None
    server_io = cast(ServerIO, io)
    for addr_family, socktype, addr_proto, _canonname, sockaddr in infos:
        try:
            return _handshake_connected_pair(
                server_io.sock_create_streams(
                    addr_family,
                    socktype,
                    addr_proto,
                    connect_to=sockaddr,
                    initial_data=initial_send,
                    limit=limit,
                    stream_factory=stream_factory,
                    async_=async_,
                ).wait()
            )
        except OSError as exc:
            last_error = exc
    if last_error is not None:
        raise last_error
    raise OSError("open_connection failed without address resolution results")


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
) -> NativeStreamPair | AsyncStreamPair:
    if not hasattr(socket, "AF_UNIX"):
        raise RuntimeError("AF_UNIX is not supported on this platform")

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
        ).wait()
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
) -> tuple[AsyncStreamReader, AsyncStreamWriter]: ...


@overload
def open_connection(
    *,
    path: str,
    limit: int = DEFAULT_LIMIT,
    stream_factory: StreamFactory | None = None,
    initial_send: SocketSendBuffer | None = None,
    async_: Literal[False] = False,
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
    scheduler: BaseScheduler | None = None,
) -> NativeStreamPair | AsyncStreamPair:
    """Connect and return stream endpoints.

    Pass ``addr=(host, port)`` for TCP, or ``path`` for a Unix-domain socket.
    The host may be a hostname or literal IP; resolution goes through
    ``scheduler.ensure_resolved()``, which skips the executor for literal
    addresses and uses ``getaddrinfo`` otherwise. Results are tried in order
    (no happy eyeballs). ``async_=False`` returns native streams;
    ``async_=True`` returns asyncio-shaped streams. The flag only selects the
    default factory when ``stream_factory`` is omitted.

    ``ssl`` matches asyncio: ``True`` uses ``ssl.create_default_context()``, an
    ``SSLContext`` is used as-is, and ``server_hostname`` defaults to the
    ``addr`` host. ``ssl=`` is native-only (not ``async_=True``). ``writer.handshake()``
    runs on this tealet before the pair is returned. ``initial_send`` is TCP
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
    )

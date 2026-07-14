"""Connect helpers and public ``open_streams`` / ``open_connection`` entry points."""

from __future__ import annotations

import socket
from typing import Literal, cast, overload

from ..io_manager import ServerIO, SocketSendBuffer
from ..scheduler import BaseScheduler
from .common import DEFAULT_LIMIT, require_proactor_io, resolve_scheduler
from .open import (
    AsyncStreamFactory,
    AsyncStreamPair,
    NativeStreamPair,
    StreamFactory,
    StreamFactoryArg,
    open_streams as build_streams,
)
from .reader import AsyncStreamReader, StreamReader
from .writer import AsyncStreamWriter, StreamWriter


@overload
def open_streams(
    sock: socket.socket,
    *,
    limit: int = DEFAULT_LIMIT,
    stream_factory: StreamFactory | None = None,
    async_: Literal[False] = False,
) -> tuple[StreamReader, StreamWriter]: ...


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

    ``async_=False`` returns native ``StreamReader`` / ``StreamWriter`` pairs;
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
) -> NativeStreamPair | AsyncStreamPair:
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


def connect_unix_streams(
    scheduler: BaseScheduler,
    path: str,
    *,
    limit: int = DEFAULT_LIMIT,
    stream_factory: StreamFactoryArg = None,
    async_: bool = False,
    initial_send: SocketSendBuffer | None = None,
) -> NativeStreamPair | AsyncStreamPair:
    if not hasattr(socket, "AF_UNIX"):
        raise RuntimeError("AF_UNIX is not supported on this platform")

    io = cast(ServerIO, require_proactor_io(scheduler))
    return io.sock_create_streams(
        socket.AF_UNIX,
        socket.SOCK_STREAM,
        connect_to=path,
        initial_data=initial_send,
        limit=limit,
        stream_factory=stream_factory,
        async_=async_,
    ).wait()


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
) -> tuple[StreamReader, StreamWriter]: ...


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
) -> tuple[StreamReader, StreamWriter]: ...


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
    )

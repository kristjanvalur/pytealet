"""Listening stream servers and socket bind helpers."""

from __future__ import annotations

import os
import socket
import ssl
import sys
from collections.abc import Callable
from typing import Any, Literal, cast, overload

from ..delivery import AcceptStreamsDelivery as AcceptedStreams
from ..io_manager import ProactorIOManager, SocketIO
from ..io_waiter import IOWaiter
from ..scheduler import BaseScheduler
from ..socket_helpers import abortive_close, run_accept_loop, set_tcp_nodelay
from ..stream_diag import (
    accept_marshal,
    accept_scheduler,
    accept_spawn,
    accept_streams_opened,
    accept_worker_conn,
)
from ..tasks import CancelledError, Task, get_current
from .common import require_proactor_io, resolve_scheduler
from .open import (
    AsyncClientHandler,
    ClientHandler,
    NativeClientHandler,
    StreamFactoryArg,
    default_server_stream_factory,
    open_streams,
)
from .reader import AsyncStreamReader, ReadStream, StreamReader
from .ssl import (
    _require_native_ssl,
    _server_ssl_context,
    check_ssl_handshake_timeout,
    wrap_ssl,
)
from .util import run_coro
from .writer import AsyncStreamWriter, StreamWriter, WriteStream, shutdown_stream_writer


def default_reuse_address() -> bool:
    return os.name == "posix" and sys.platform != "cygwin"


def set_reuseport(sock: socket.socket) -> None:
    if not hasattr(socket, "SO_REUSEPORT"):
        raise ValueError("reuse_port not supported by socket module")
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    except OSError as exc:
        raise ValueError(
            "reuse_port not supported by socket module, SO_REUSEPORT defined but not implemented."
        ) from exc


def apply_listen_socket_contract(sock: socket.socket) -> None:
    sock.setblocking(False)
    os.set_inheritable(sock.fileno(), False)


def prepare_listen_socket(sock: socket.socket, *, backlog: int) -> socket.socket:
    if sock.type != socket.SOCK_STREAM:
        raise ValueError(f"A stream socket was expected, got {sock!r}")
    apply_listen_socket_contract(sock)
    sock.listen(backlog)
    return sock


def bind_tcp_socket(
    io: SocketIO,
    addr: tuple[str | None, int],
    *,
    family: int = socket.AF_INET,
    backlog: int,
    reuse_address: bool | None = None,
    reuse_port: bool | None = None,
) -> socket.socket:
    if reuse_address is None:
        reuse_address = default_reuse_address()
    host, port = addr
    sock = io.sock_create(family, socket.SOCK_STREAM).wait()
    try:
        if reuse_address:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if reuse_port:
            set_reuseport(sock)
        bind_host = "" if host is None else host
        sock.bind((bind_host, port))
        sock.listen(backlog)
    except OSError:
        sock.close()
        raise
    return sock


def bind_unix_socket(io: SocketIO, path: str, *, backlog: int) -> socket.socket:
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


def _accept_open_streams(
    io: ProactorIOManager,
    sock: socket.socket,
    callback: Callable[[AcceptedStreams], object],
    *,
    limit: int = 2**16,
    stream_factory: StreamFactoryArg = None,
    async_: bool = False,
) -> IOWaiter[None]:
    """Open streams on the accept delivery thread and marshal the pair.

    A TCP socket has Nagle disabled before streams open. ``recv_many`` is
    armed there. The scheduler callback may still be queued when ``wait()``
    returns. A scheduler that refuses the post leaves the opened pair intact.
    """

    def open_and_deliver(conn: socket.socket) -> AcceptedStreams:
        try:
            return open_streams(
                io,
                conn,
                limit=limit,
                stream_factory=stream_factory,
                async_=async_,
            )
        except BaseException:
            abortive_close(conn)
            raise

    def deliver_streams(streams: AcceptedStreams) -> None:
        reader, writer = streams
        try:
            callback((reader, writer))
        except BaseException:
            try:
                writer.close()
            except BaseException:
                abortive_close(writer.get_extra_info("socket"))
            raise

    def thread_handler(accepted: socket.socket) -> None:
        fd = accepted.fileno()
        accept_worker_conn(fd)
        try:
            # worker thread that just accepted. do this before any send.
            set_tcp_nodelay(accepted)
            streams = open_and_deliver(accepted)
        except BaseException:
            # open_and_deliver already closed; nodelay can fail first.
            if accepted.fileno() != -1:
                abortive_close(accepted)
            raise

        accept_streams_opened(fd)
        accept_marshal(fd)

        def on_scheduler() -> None:
            _reader, writer = streams
            peer = writer.get_extra_info("socket")
            if peer is not None:
                accept_scheduler(peer.fileno())
            deliver_streams(streams)

        io._marshal_on_scheduler(on_scheduler)

    return io.accept_sockets(sock, thread_handler)


class Server:
    """Listening socket server. Each accept is handed to ``thread_handler``.

    ``create_server`` binds the listener and runs this accept loop. The
    handler runs on the thread that delivers the completion and owns the
    accepted socket. It must not park or touch the scheduler; marshal with
    ``call_on_scheduler`` when the work needs a tealet. ``close()``
    cancels the accept-loop tealet. It does not close sockets the handler
    has already taken.
    """

    _io: ProactorIOManager

    def __init__(self, scheduler: BaseScheduler, sockets: list[socket.socket]) -> None:
        self._scheduler = scheduler
        self._io = require_proactor_io(scheduler)
        self._sockets = tuple(sockets)
        self._accept_task: Task | None = None
        self._closed = False
        self._listen_sock: socket.socket | None = None
        self._thread_handler: Callable[[socket.socket], object] | None = None

    def __enter__(self) -> Server:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()
        self.wait_closed()

    @property
    def sockets(self) -> tuple[socket.socket, ...]:
        return self._sockets

    @property
    def accept_task(self) -> Task | None:
        return self._accept_task

    def close(self) -> None:
        if self._closed:
            return
        accept_task = self._accept_task
        if accept_task is not None and not accept_task.done():
            self._closed = True
            if get_current() is not None:
                accept_task.cancel()
            else:
                self._scheduler.call_soon_threadsafe(accept_task.cancel)
            self._io.proactor.wake_wait()
            return
        self._finish_close()

    def _finish_close(self) -> None:
        self._closed = True
        for sock in self._sockets:
            if sock.fileno() != -1:
                sock.close()

    def _start_accept_loop(
        self,
        sock: socket.socket,
        thread_handler: Callable[[socket.socket], object],
    ) -> None:
        self._listen_sock = sock
        self._thread_handler = thread_handler
        self._accept_task = self._scheduler.spawn(self._accept_loop)

    def _accept_loop(self) -> None:
        assert self._listen_sock is not None
        listen_sock = self._listen_sock
        user_handler = self._thread_handler
        assert user_handler is not None

        def accept_once() -> None:
            def thread_handler(accepted: socket.socket) -> None:
                try:
                    # worker thread that just accepted. do this before any send.
                    set_tcp_nodelay(accepted)
                except BaseException:
                    abortive_close(accepted)
                    raise
                user_handler(accepted)

            self._io.accept_sockets(listen_sock, thread_handler).wait()

        run_accept_loop(
            self._scheduler,
            listen_sock,
            lambda: self._closed,
            accept_once,
            self._finish_close,
        )

    def wait_closed(self) -> None:
        accept_task = self._accept_task
        if accept_task is not None and not accept_task.done():
            try:
                accept_task.wait()
            except CancelledError:
                pass
        self._finish_close()

    def serve_forever(self) -> None:
        if self._closed:
            raise RuntimeError("server is closed")
        assert self._accept_task is not None
        try:
            self._accept_task.wait()
        except CancelledError:
            pass


def _listen_socket(
    io: ProactorIOManager,
    *,
    who: str,
    addr: tuple[str | None, int] | None,
    path: str | None,
    sock: socket.socket | None,
    family: int,
    backlog: int,
    reuse_address: bool | None,
    reuse_port: bool | None,
) -> socket.socket:
    if sock is not None:
        if addr is not None or path is not None:
            raise ValueError("addr/path and sock cannot be specified at the same time")
        return prepare_listen_socket(sock, backlog=backlog)
    if path is not None:
        if addr is not None:
            raise TypeError(f"{who}() accepts addr= or path=, not both")
        return bind_unix_socket(io, path, backlog=backlog)
    if addr is not None:
        return bind_tcp_socket(
            io,
            addr,
            family=family,
            backlog=backlog,
            reuse_address=reuse_address,
            reuse_port=reuse_port,
        )
    raise TypeError(f"{who}() requires addr=, path=, or sock=")


def create_server(
    thread_handler: Callable[[socket.socket], object],
    *,
    addr: tuple[str | None, int] | None = None,
    path: str | None = None,
    sock: socket.socket | None = None,
    family: int = socket.AF_INET,
    backlog: int = 100,
    reuse_address: bool | None = None,
    reuse_port: bool | None = None,
    scheduler: BaseScheduler | None = None,
) -> Server:
    """Accept connections and pass each socket to ``thread_handler``.

    Bind kwargs match ``start_server`` (``addr`` / ``path`` / ``sock``).
    ``sock`` is a caller-prepared stream socket; it is made non-blocking and
    ``listen(backlog)`` is called. The handler runs on the thread that
    delivers the accept, after Nagle is disabled on a TCP socket, and owns
    that socket. A failure while it still owns the socket is the handler's
    to close. It must not park or touch the scheduler.
    ``call_on_scheduler`` the work that needs a tealet. This server does
    not post a receive and does not build a stream or a connection.
    """

    resolved = resolve_scheduler(scheduler)
    io = require_proactor_io(resolved)
    listen_sock = _listen_socket(
        io,
        who="create_server",
        addr=addr,
        path=path,
        sock=sock,
        family=family,
        backlog=backlog,
        reuse_address=reuse_address,
        reuse_port=reuse_port,
    )
    server = Server(resolved, [listen_sock])
    server._start_accept_loop(listen_sock, thread_handler)
    return server


class StreamServer:
    """Stream server on top of ``create_server``.

    The delivery thread opens a stream pair from the accepted socket and
    marshals it onto the scheduler, which spawns the handler tealet.
    Transient accept errors (``ECONNABORTED``, ``EMFILE``, …) arrive as
    ``OSError`` on ``wait()``; the accept loop ignores aborted clients and
    pauses before re-arming under fd pressure. ``close()`` cancels that
    accept-loop tealet synchronously. Listening sockets close when the tealet
    exits. Handler tealets already spawned keep running until they finish.
    A pair that arrives after ``close()`` is discarded. ``wait_closed()``
    blocks until the accept loop has exited and every dispatched handler
    tealet has finished.

    Use as a context manager to call ``close()`` and ``wait_closed()`` on
    scope exit. ``serve_forever()`` blocks the current tealet until
    ``close()`` is called; pair with ``wait_closed()`` or the context manager
    to drain in-flight handlers.
    """

    _io: ProactorIOManager

    def __init__(
        self,
        scheduler: BaseScheduler,
        sockets: list[socket.socket],
    ) -> None:
        self._scheduler = scheduler
        self._io = require_proactor_io(scheduler)
        self._sockets = tuple(sockets)
        self._accept_task: Task | None = None
        self._handler_tasks: set[Task] = set()
        self._closed = False
        self._client_handler: ClientHandler | None = None
        self._accept_async = False
        self._accept_limit = 2**16
        self._handler_eager_start = False
        self._ssl_handshake_timeout: float | None = None
        self._accept_server: Server | None = None

    @property
    def handler_eager_start(self) -> bool:
        """Whether accepted connections spawn handler tealets with ``eager_start=True``.

        Default is false: the delivery thread has already opened the streams.
        Eager start would run the handler on the marshal stack (and could
        inherit a scheduler-wide eager factory). Opt in only if that is what
        you want.
        """

        return self._handler_eager_start

    @handler_eager_start.setter
    def handler_eager_start(self, value: bool) -> None:
        self._handler_eager_start = bool(value)

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
        inner = self._accept_server
        if inner is not None:
            # the accept server cancels the accept tealet and closes listeners
            self._closed = True
            inner.close()
            return
        accept_task = self._accept_task
        if accept_task is not None and not accept_task.done():
            # Mark shutdown so late accepts discard, but keep listening socket(s)
            # open until the accept-loop tealet exits. Closing them here while the
            # tealet is blocked in ``accept_many().wait()`` can strand threaded
            # selector proactor worker threads.
            self._closed = True
            if get_current() is not None:
                accept_task.cancel()
            else:
                # close() may run from main or a foreign thread after the
                # scheduler has stopped (for example pytest teardown).
                self._scheduler.call_soon_threadsafe(accept_task.cancel)
            # Unblock threaded selector proactors parked in accept_many().wait().
            self._io.proactor.wake_wait()
            return
        self._finish_close()

    def _finish_close(self) -> None:
        self._closed = True
        for sock in self._sockets:
            if sock.fileno() != -1:
                sock.close()

    def _on_accept(self, streams: AcceptedStreams) -> None:
        """Handle one marshalled accept delivery: discard, or spawn a handler tealet."""

        if self._closed:
            _reader, writer = streams
            shutdown_stream_writer(writer, best_effort=True)
            return

        reader, writer = streams
        sock = writer.get_extra_info("socket")
        if sock is not None:
            accept_spawn(sock.fileno())
        client_handler = self._client_handler
        assert client_handler is not None
        async_ = self._accept_async

        def serve() -> None:
            try:
                if self._closed:
                    return
                writer.handshake(timeout=self._ssl_handshake_timeout)
                if self._closed:
                    return
                if async_:
                    run_coro(
                        cast(AsyncClientHandler, client_handler)(
                            cast(AsyncStreamReader, reader),
                            cast(AsyncStreamWriter, writer),
                        )
                    )
                else:
                    cast(NativeClientHandler, client_handler)(
                        cast(StreamReader, reader),
                        cast(StreamWriter, writer),
                    )
            finally:
                shutdown_stream_writer(writer)

        try:
            # pass False explicitly: spawn(None) would honour a factory eager default
            handler_task = self._scheduler.spawn(serve, eager_start=self._handler_eager_start)
        except Exception as spawn_exc:
            shutdown_stream_writer(writer, best_effort=True)
            self._scheduler.call_exception_handler(
                {
                    "message": "Exception spawning stream server handler",
                    "exception": spawn_exc,
                    "scheduler": self._scheduler,
                    "handle": None,
                }
            )
            return

        self._handler_tasks.add(handler_task)

        def drop_handler(_task) -> None:
            self._handler_tasks.discard(handler_task)

        handler_task.add_done_callback(drop_handler)

    def wait_closed(self) -> None:
        """Block until the accept loop has exited and handlers are done."""

        inner = self._accept_server
        if inner is not None:
            inner.wait_closed()
        else:
            accept_task = self._accept_task
            if accept_task is not None and not accept_task.done():
                try:
                    accept_task.wait()
                except CancelledError:
                    pass
            self._finish_close()

        for handler in tuple(self._handler_tasks):
            if not handler.done():
                handler.wait()

    def serve_forever(self) -> None:
        """Block until the accept-loop tealet exits.

        Accept handling is already active from ``start_server()``; this waits
        on that tealet (until ``close()`` cancels it and ``_finish_close()``
        runs in the loop's ``finally``). It does not install signal handlers —
        use ``tealetio.run()`` / ``Runner`` for that.
        """

        if self._closed:
            raise RuntimeError("server is closed")
        assert self._accept_task is not None
        try:
            self._accept_task.wait()
        except CancelledError:
            pass


def _open_accepted_streams(
    io: ProactorIOManager,
    accepted: socket.socket,
    *,
    limit: int,
    async_: bool,
    sslcontext: ssl.SSLContext | None,
) -> AcceptedStreams:
    """Open a stream pair from an accepted socket. Does not post a second recv.

    The default server factory checks a buffer group out of the IO manager
    idle stack and arms ``recv_many``. TLS wraps that pair without
    handshaking.
    """

    factory = default_server_stream_factory(async_=async_)
    writer_to_close = None
    try:
        reader, writer = factory(io, accepted, limit=limit)
        writer_to_close = writer
        if sslcontext is not None:
            # ssl is rejected for asyncio-shaped streams before this runs
            reader, writer = wrap_ssl(
                cast(ReadStream, reader),
                cast(WriteStream, writer),
                sslcontext,
                server_side=True,
                limit=limit,
            )
            writer_to_close = writer
    except BaseException:
        if writer_to_close is not None:
            shutdown_stream_writer(writer_to_close, best_effort=True)
        elif accepted.fileno() != -1:
            abortive_close(accepted)
        raise
    return reader, writer


def start_server_impl(
    scheduler: BaseScheduler,
    client_handler: ClientHandler,
    *,
    addr: tuple[str | None, int] | None = None,
    path: str | None = None,
    sock: socket.socket | None = None,
    family: int = socket.AF_INET,
    backlog: int = 100,
    reuse_address: bool | None = None,
    reuse_port: bool | None = None,
    limit: int = 2**16,
    async_: bool = False,
    handler_eager_start: bool = False,
    ssl: ssl.SSLContext | bool | None = None,
    ssl_handshake_timeout: float | None = None,
) -> StreamServer:
    check_ssl_handshake_timeout(ssl, ssl_handshake_timeout)
    sslcontext = _server_ssl_context(ssl)
    if sslcontext is not None:
        _require_native_ssl(async_=async_)
    io = require_proactor_io(scheduler)
    if sock is not None:
        if addr is not None or path is not None:
            raise ValueError("addr/path and sock cannot be specified at the same time")
        listen_sock = prepare_listen_socket(sock, backlog=backlog)
    elif path is not None:
        if addr is not None:
            raise TypeError("start_server() accepts addr= or path=, not both")
        listen_sock = bind_unix_socket(io, path, backlog=backlog)
    elif addr is not None:
        listen_sock = bind_tcp_socket(
            io,
            addr,
            family=family,
            backlog=backlog,
            reuse_address=reuse_address,
            reuse_port=reuse_port,
        )
    else:
        raise TypeError("start_server() requires addr=, path=, or sock=")

    server = StreamServer(scheduler, [listen_sock])
    server.handler_eager_start = handler_eager_start
    server._client_handler = client_handler
    server._accept_async = async_
    server._accept_limit = limit
    server._ssl_handshake_timeout = ssl_handshake_timeout if ssl else None

    def thread_handler(accepted: socket.socket) -> None:
        fd = accepted.fileno()
        accept_worker_conn(fd)
        reader, writer = _open_accepted_streams(
            io,
            accepted,
            limit=limit,
            async_=async_,
            sslcontext=sslcontext,
        )
        accept_streams_opened(fd)
        accept_marshal(fd)

        def deliver() -> None:
            if server._closed:
                shutdown_stream_writer(writer, best_effort=True)
                return
            peer = writer.get_extra_info("socket")
            if peer is not None:
                accept_scheduler(peer.fileno())
            server._on_accept((reader, writer))

        # a scheduler that refuses the post leaves the opened pair intact
        scheduler.call_on_scheduler(deliver)

    # already listened above; create_server listens again, so pass the same backlog
    inner = create_server(
        thread_handler,
        sock=listen_sock,
        backlog=backlog,
        scheduler=scheduler,
    )
    server._accept_server = inner
    server._accept_task = inner.accept_task
    server._sockets = inner.sockets
    return server


@overload
def start_server(
    client_handler: Any,
    *,
    addr: tuple[str | None, int],
    family: int = socket.AF_INET,
    backlog: int = 100,
    reuse_address: bool | None = None,
    reuse_port: bool | None = None,
    limit: int = 2**16,
    async_: Literal[False] = False,
    ssl: ssl.SSLContext | None = None,
    ssl_handshake_timeout: float | None = None,
) -> StreamServer: ...


@overload
def start_server(
    client_handler: Any,
    *,
    addr: tuple[str | None, int],
    family: int = socket.AF_INET,
    backlog: int = 100,
    reuse_address: bool | None = None,
    reuse_port: bool | None = None,
    limit: int = 2**16,
    async_: Literal[True],
) -> StreamServer: ...


@overload
def start_server(
    client_handler: Any,
    *,
    path: str,
    backlog: int = 100,
    limit: int = 2**16,
    async_: Literal[False] = False,
    ssl: ssl.SSLContext | None = None,
    ssl_handshake_timeout: float | None = None,
) -> StreamServer: ...


@overload
def start_server(
    client_handler: Any,
    *,
    path: str,
    backlog: int = 100,
    limit: int = 2**16,
    async_: Literal[True],
) -> StreamServer: ...


@overload
def start_server(
    client_handler: Any,
    *,
    sock: socket.socket,
    backlog: int = 100,
    limit: int = 2**16,
    async_: Literal[False] = False,
    ssl: ssl.SSLContext | None = None,
    ssl_handshake_timeout: float | None = None,
) -> StreamServer: ...


@overload
def start_server(
    client_handler: Any,
    *,
    sock: socket.socket,
    backlog: int = 100,
    limit: int = 2**16,
    async_: Literal[True],
) -> StreamServer: ...


def start_server(
    client_handler: ClientHandler,
    *,
    addr: tuple[str | None, int] | None = None,
    path: str | None = None,
    sock: socket.socket | None = None,
    family: int = socket.AF_INET,
    backlog: int = 100,
    reuse_address: bool | None = None,
    reuse_port: bool | None = None,
    limit: int = 2**16,
    async_: bool = False,
    handler_eager_start: bool = False,
    ssl: ssl.SSLContext | bool | None = None,
    ssl_handshake_timeout: float | None = None,
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
    ``ssl`` matches asyncio ``create_server``: an ``SSLContext`` (not ``True``).
    Build one with ``ssl_server_context(certfile, keyfile)`` or
    ``ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)`` plus
    ``load_cert_chain``. ``ssl=`` is native-only. Handshake runs on the handler
    tealet before ``client_handler``, with ``ssl_handshake_timeout`` (default 60s,
    asyncio-shaped).

    Listens with ``create_server``. On the delivery thread each accepted
    socket is opened as a stream pair, which posts ``recv_many``, and the pair
    is marshalled onto the scheduler. ``async_=False`` uses native stream
    types and calls the handler directly; ``async_=True`` uses asyncio-shaped
    streams and drives the handler through ``run_coro()``. Each connection
    checks out its own provided-buffer pool from the IO manager idle stack.
    ``open_streams()`` / ``open_connection()`` still default to the scheduler
    shared pool for a single connection. A peer that connects and never sends
    leaves ``recv_many`` pending; the handler still receives the pair and can
    apply read timeouts or idle close policy.

    Late deliveries after ``close()`` see ``_closed`` and are discarded.
    The scheduler spawns handler tealets with explicit ``eager_start=False``
    (``handler_eager_start`` defaults to false) so an eager task factory cannot
    run the handler on the accept stack. Pass ``handler_eager_start=True`` to
    opt in. Handler exceptions stay in the handler tealet and do not stop the
    listener. ``spawn()`` failures are reported through the scheduler
    exception handler.
    """

    return start_server_impl(
        resolve_scheduler(scheduler),
        client_handler,
        addr=addr,
        path=path,
        sock=sock,
        family=family,
        backlog=backlog,
        reuse_address=reuse_address,
        reuse_port=reuse_port,
        limit=limit,
        async_=async_,
        handler_eager_start=handler_eager_start,
        ssl=ssl,
        ssl_handshake_timeout=ssl_handshake_timeout,
    )

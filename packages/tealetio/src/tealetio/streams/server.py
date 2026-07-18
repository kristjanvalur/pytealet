"""Listening stream servers and socket bind helpers."""

from __future__ import annotations

import os
import socket
import sys
from typing import Any, Literal, cast, overload

from ..continuous_callbacks import AcceptStreamsDelivery as AcceptedStreams
from ..io_manager import ProactorIOManager, ServerIO, SocketIO
from ..scheduler import BaseScheduler
from ..operations import is_io_cancellation
from ..tasks import CancelledError, Task, get_current
from .common import require_proactor_io, resolve_scheduler
from .open import (
    AsyncClientHandler,
    AsyncStreamFactory,
    ClientHandler,
    NativeClientHandler,
    StreamFactory,
    StreamFactoryArg,
    default_server_stream_factory,
)
from .util import run_coro
from .reader import AsyncStreamReader, StreamReader
from .writer import AsyncStreamWriter, StreamWriter, shutdown_stream_writer


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


class StreamServer:
    """Listening stream server with a scheduler accept-loop tealet.

    ``start_server()`` spawns a tealet that repeatedly ``wait()``s on
    ``accept_many_streams`` (one emulated accept per iteration, or one multishot
    leg until cancel/error). ``close()`` cancels that accept-loop tealet
    synchronously; it does not close listening socket(s) itself. The accept-loop
    tealet wraps its main loop in ``try``/``finally`` so ``CancelledError`` from
    ``cancel()`` or ``OSError(errno.ECANCELED)`` from IO cancel runs cleanup that
    sets ``_closed`` and closes listeners. Handler
    tealets already spawned for accepted connections keep running until they
    finish on their own. Late accepts delivered while shutting down see
    ``_closed`` and are discarded. ``wait_closed()`` blocks until the accept-loop
    tealet has exited and every dispatched handler tealet has finished. Accept
    callbacks are marshalled onto the scheduler thread, which spawns the handler
    tealet directly.

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
        self._listen_sock: socket.socket | None = None
        self._client_handler: ClientHandler | None = None
        self._accept_async = False
        self._accept_limit = 2**16
        self._stream_factory: StreamFactoryArg = None
        self._handler_eager_start = True

    @property
    def handler_eager_start(self) -> bool:
        """Whether accepted connections spawn handler tealets with ``eager_start=True``."""

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

    def _start_accept_loop(
        self,
        sock: socket.socket,
        client_handler: ClientHandler,
        *,
        limit: int,
        stream_factory: StreamFactoryArg,
        async_: bool,
    ) -> None:
        self._listen_sock = sock
        self._client_handler = client_handler
        self._accept_async = async_
        self._accept_limit = limit
        self._stream_factory = stream_factory
        self._accept_task = self._scheduler.spawn(self._accept_loop)

    def _accept_loop(self) -> None:
        io = cast(ServerIO, self._io)
        assert self._listen_sock is not None

        try:
            # Emulated oneshot accept_many finishes soft EMFILE/etc. without
            # exception so this loop re-arms. Under sustained fd pressure that
            # can busy-loop (listen fd stays readable); known tradeoff vs
            # killing the server — see socket_helpers soft-accept note.
            while not self._closed:
                try:
                    io.accept_many_streams(
                        self._listen_sock,
                        self._on_accept,
                        limit=self._accept_limit,
                        stream_factory=self._stream_factory,
                        async_=self._accept_async,
                    ).wait()
                except CancelledError:
                    return
                except OSError as exc:
                    if is_io_cancellation(exc):
                        return
                    if self._closed:
                        return
                    raise
                except RuntimeError:
                    if self._closed:
                        return
                    raise
        finally:
            self._finish_close()

    def _on_accept(self, streams: AcceptedStreams) -> None:
        """Handle one marshalled accept delivery: discard, or spawn a handler tealet."""

        if self._closed:
            _reader, writer = streams
            shutdown_stream_writer(writer, best_effort=True)
            return

        reader, writer = streams
        client_handler = self._client_handler
        assert client_handler is not None
        async_ = self._accept_async

        def serve() -> None:
            try:
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

        def drop_handler(_task: Task[Any]) -> None:
            self._handler_tasks.discard(handler_task)

        handler_task.add_done_callback(drop_handler)

    def wait_closed(self) -> None:
        """Block until the accept loop has exited and handlers are done."""

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


def start_stream_server(
    scheduler: BaseScheduler,
    sock: socket.socket,
    client_handler: ClientHandler,
    *,
    limit: int = 2**16,
    stream_factory: StreamFactoryArg = None,
    async_: bool = False,
    handler_eager_start: bool = True,
) -> StreamServer:
    """Start accept handling on a listening socket and return a ``StreamServer``.

    Requires ``ServerIO`` (blocking ``SocketIO`` plus ``proactor`` submission).
    Accepts deliver stream pairs via ``accept_many_streams``; each connection
    arms ``recv_many`` when streams open on the accept delivery thread.
    """

    if stream_factory is None:
        stream_factory = default_server_stream_factory(async_=async_)

    server = StreamServer(scheduler, [sock])
    server.handler_eager_start = handler_eager_start
    server._start_accept_loop(
        sock,
        client_handler,
        limit=limit,
        stream_factory=stream_factory,
        async_=async_,
    )
    return server


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
    stream_factory: StreamFactoryArg = None,
    async_: bool = False,
    handler_eager_start: bool = True,
) -> StreamServer:
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
    return start_stream_server(
        scheduler,
        listen_sock,
        client_handler,
        limit=limit,
        stream_factory=stream_factory,
        async_=async_,
        handler_eager_start=handler_eager_start,
    )


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
    stream_factory: StreamFactory | None = None,
    async_: Literal[False] = False,
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
    stream_factory: AsyncStreamFactory | None = None,
    async_: Literal[True],
) -> StreamServer: ...


@overload
def start_server(
    client_handler: Any,
    *,
    path: str,
    backlog: int = 100,
    limit: int = 2**16,
    stream_factory: StreamFactory | None = None,
    async_: Literal[False] = False,
) -> StreamServer: ...


@overload
def start_server(
    client_handler: Any,
    *,
    path: str,
    backlog: int = 100,
    limit: int = 2**16,
    stream_factory: AsyncStreamFactory | None = None,
    async_: Literal[True],
) -> StreamServer: ...


@overload
def start_server(
    client_handler: Any,
    *,
    sock: socket.socket,
    backlog: int = 100,
    limit: int = 2**16,
    stream_factory: StreamFactory | None = None,
    async_: Literal[False] = False,
) -> StreamServer: ...


@overload
def start_server(
    client_handler: Any,
    *,
    sock: socket.socket,
    backlog: int = 100,
    limit: int = 2**16,
    stream_factory: AsyncStreamFactory | None = None,
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
    stream_factory: StreamFactoryArg = None,
    async_: bool = False,
    handler_eager_start: bool = True,
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

    Accept callbacks are marshalled onto the scheduler thread, which spawns
    handler tealets directly (``handler_eager_start`` defaults to true).
    Handler exceptions propagate
    in the handler tealet and do not stop the listener. ``spawn()`` failures
    during dispatch are reported through the scheduler exception handler.
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
        stream_factory=stream_factory,
        async_=async_,
        handler_eager_start=handler_eager_start,
    )

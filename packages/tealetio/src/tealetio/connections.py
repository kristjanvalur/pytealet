"""Low-level accepted connections and a create_server-shaped listener.

A ``Connection`` is a socket plus one outstanding oneshot recv into a pooled
64 KiB buffer. The recv slot has a single consumer: the default is to stash
the first completion; ``set_recv_callback`` or ``open_streams`` takes it.
If the oneshot has already finished, the new consumer is invoked immediately
(copied into the stream when wrapping). ``recv_many`` starts only after that
upgrade.
"""

from __future__ import annotations

import socket
from collections.abc import Callable
from typing import Any

from .delivery import is_io_cancellation
from .io_manager import (
    DEFAULT_CONNECTION_RECV_SIZE,
    ProactorIOManager,
)
from .scheduler import BaseScheduler
from .socket_helpers import (
    ACCEPT_RETRY_DELAY,
    abortive_close,
    is_accept_resource_error,
    is_soft_accept_error,
)
from .streams.common import require_proactor_io, resolve_scheduler
from .streams.open import NativeStreamPair
from .streams.reader import StreamReader
from .streams.server import (
    bind_tcp_socket,
    bind_unix_socket,
    prepare_listen_socket,
)
from .streams.util import DEFAULT_LIMIT
from .streams.writer import StreamWriter
from .tasks import CancelledError, Task, get_current

__all__ = [
    "Connection",
    "ConnectionServer",
    "start_connection_server",
]

RecvCallback = Callable[["Connection", memoryview | None, BaseException | None], object]


class Connection:
    """Accepted socket with one outstanding oneshot recv from the 64 KiB pool.

    The first recv is posted when the connection is created (accept CQE
    thread). Completions are delivered on the scheduler. ``data`` in the recv
    callback is a view into the pool buffer, valid only during the call; copy
    if you need to keep it. Empty data is EOF. ``data is None`` with ``exc``
    set is a transport error.

    Binding a second consumer after the first chunk was delivered raises.
    ``open_streams`` is that consumer: it feeds the oneshot into the reader
    (now or when it arrives) and then arms ``recv_many``.
    """

    def __init__(
        self,
        io: ProactorIOManager,
        sock: socket.socket,
        buf: bytearray,
        *,
        recv_size: int,
    ) -> None:
        self._io = io
        self._sock = sock
        self._buf: bytearray | None = buf
        self._view = memoryview(buf)[:recv_size]
        self._recv_size = recv_size
        self._recv_handle: Any = None
        self._recv_callback: RecvCallback | None = None
        self._pending: tuple[int, BaseException | None] | None = None
        self._taken = False
        self._streams_opened = False
        self._recv_buffer: Any = None
        self._closed = False

    @classmethod
    def start(
        cls,
        io: ProactorIOManager,
        sock: socket.socket,
        *,
        recv_size: int = DEFAULT_CONNECTION_RECV_SIZE,
    ) -> Connection:
        """Checkout a pool buffer, post ``recv_into``, and return the connection."""

        buf = io.acquire_recv_into_buffer()
        conn = cls(io, sock, buf, recv_size=recv_size)
        try:
            conn._recv_handle = io.proactor.recv_into(sock, conn._view, conn._on_recv_raw)
        except BaseException:
            conn._buf = None
            io.release_recv_into_buffer(buf)
            raise
        return conn

    @property
    def sock(self) -> socket.socket:
        return self._sock

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def peername(self) -> Any:
        try:
            return self._sock.getpeername()
        except OSError:
            return None

    def _on_recv_raw(self, nbytes: int | None, exception: BaseException | None) -> None:
        n = 0 if nbytes is None else nbytes

        def deliver() -> None:
            self._complete_recv(n, exception)

        scheduler = self._io._scheduler
        if scheduler is None:
            deliver()
            return
        scheduler.call_soon_threadsafe(deliver, immediate=True)

    def _complete_recv(self, nbytes: int, exception: BaseException | None) -> None:
        self._recv_handle = None
        if self._closed:
            self._release_buffer()
            return
        if self._recv_callback is not None:
            self._taken = True
            callback = self._recv_callback
            self._invoke(callback, nbytes, exception)
            self._release_buffer()
            return
        self._pending = (nbytes, exception)

    def _invoke(self, callback: RecvCallback, nbytes: int, exception: BaseException | None) -> None:
        if exception is not None:
            callback(self, None, exception)
            return
        buf = self._buf
        assert buf is not None
        callback(self, memoryview(buf)[:nbytes], None)

    def _release_buffer(self) -> None:
        buf = self._buf
        if buf is None:
            return
        self._buf = None
        self._io.release_recv_into_buffer(buf)

    def set_recv_callback(self, callback: RecvCallback) -> None:
        """Install the recv consumer.

        If the oneshot has already completed, ``callback`` runs immediately
        on this stack. If it is still in flight, the completion invokes
        ``callback`` later on the scheduler. Replacing a consumer after the
        first chunk was delivered raises ``RuntimeError``.
        """

        if self._closed:
            raise RuntimeError("Connection is closed")
        if self._taken:
            raise RuntimeError("recv already consumed")
        if self._recv_callback is not None:
            raise RuntimeError("recv callback already bound")
        self._recv_callback = callback
        pending = self._pending
        if pending is None:
            return
        self._taken = True
        self._pending = None
        nbytes, exception = pending
        self._invoke(callback, nbytes, exception)
        self._release_buffer()

    def open_streams(self, *, limit: int = DEFAULT_LIMIT) -> NativeStreamPair:
        """Wrap as ``StreamReader`` / ``StreamWriter`` and steal the recv slot.

        Feeds the oneshot chunk into the reader when it is already pending, or
        when it arrives; then arms ``recv_many``. Parks in ``read`` / ``readline``
        until that first chunk if the oneshot is still in flight.
        """

        if self._closed:
            raise RuntimeError("Connection is closed")
        if self._streams_opened:
            raise RuntimeError("streams already opened")
        if self._recv_callback is not None or self._taken:
            raise RuntimeError("recv callback already bound")

        pool = self._io.acquire_recv_buffer_pool()
        recv_buffer = self._io._open_sock_recv_iter(self._sock, pool, owns_pool=True, start=False)
        send_buffer = self._io._open_send_buffer(self._sock)
        reader = StreamReader(limit=limit, recv_buffer=recv_buffer)
        writer = StreamWriter(send_buffer=send_buffer, sock=self._sock, io=self._io, reader=reader)
        self._recv_buffer = recv_buffer
        self._streams_opened = True

        def feed(_conn: Connection, data: memoryview | None, exc: BaseException | None) -> None:
            if exc is not None:
                recv_buffer.feed_initial(exception=exc)
                return
            eof = data is not None and len(data) == 0
            recv_buffer.feed_initial(data, eof=eof)

        self.set_recv_callback(feed)
        return reader, writer

    def send_nowait(self, data: bytes | bytearray | memoryview) -> None:
        """Fire-and-forget sendall. Safe from the accept callback (does not park)."""

        if self._closed:
            raise RuntimeError("Connection is closed")
        self._io.sock_send_nowait(self._sock, data)

    def close(self) -> None:
        """Cancel an in-flight oneshot, close streams if opened, and close the socket."""

        if self._closed:
            return
        self._closed = True
        recv_buffer = self._recv_buffer
        self._recv_buffer = None
        if recv_buffer is not None:
            recv_buffer.close()
        handle = self._recv_handle
        in_flight = handle is not None and self._pending is None and not self._taken
        if in_flight:
            self._io.cancel_nowait(handle)
            # pool buffer is released when the cancelled recv completes
        else:
            self._recv_handle = None
            self._release_buffer()
        try:
            self._io.sock_close(self._sock)
        except OSError:
            abortive_close(self._sock)


class ConnectionServer:
    """Listening server that delivers ``Connection`` objects, not stream pairs.

    The accept callback runs on the scheduler (no handler tealet). Park only
    after ``spawn`` or ``open_streams`` into a task. ``close()`` cancels the
    accept-loop tealet; in-flight connections are not cancelled.
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
        self._closed = False
        self._listen_sock: socket.socket | None = None
        self._callback: Callable[[Connection], object] | None = None
        self._recv_size = DEFAULT_CONNECTION_RECV_SIZE

    def __enter__(self) -> ConnectionServer:
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
        callback: Callable[[Connection], object],
        *,
        recv_size: int,
    ) -> None:
        self._listen_sock = sock
        self._callback = callback
        self._recv_size = recv_size
        self._accept_task = self._scheduler.spawn(self._accept_loop)

    def _accept_loop(self) -> None:
        io = self._io
        assert self._listen_sock is not None

        try:
            while not self._closed:
                try:
                    io.accept_many_connections(
                        self._listen_sock,
                        self._on_accept,
                        recv_size=self._recv_size,
                    ).wait()
                except CancelledError:
                    return
                except OSError as exc:
                    if is_io_cancellation(exc):
                        return
                    if self._closed:
                        return
                    if is_soft_accept_error(exc):
                        if is_accept_resource_error(exc):
                            self._scheduler.call_exception_handler(
                                {
                                    "message": "socket.accept() out of system resource",
                                    "exception": exc,
                                    "socket": self._listen_sock,
                                }
                            )
                            try:
                                self._scheduler.sleep(ACCEPT_RETRY_DELAY)
                            except CancelledError:
                                return
                        continue
                    raise
                except RuntimeError:
                    if self._closed:
                        return
                    raise
        finally:
            self._finish_close()

    def _on_accept(self, conn: Connection) -> None:
        if self._closed:
            conn.close()
            return
        callback = self._callback
        assert callback is not None
        callback(conn)

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


def start_connection_server(
    callback: Callable[[Connection], object],
    *,
    addr: tuple[str | None, int] | None = None,
    path: str | None = None,
    sock: socket.socket | None = None,
    family: int = socket.AF_INET,
    backlog: int = 100,
    reuse_address: bool | None = None,
    reuse_port: bool | None = None,
    recv_size: int | None = None,
    scheduler: BaseScheduler | None = None,
) -> ConnectionServer:
    """Start a low-level server that delivers ``Connection`` objects.

    Bind kwargs match ``start_server`` (``addr`` / ``path`` / ``sock``).
    ``recv_size`` defaults to 64 KiB (the pool buffer); it cannot exceed that.
    The accept callback runs on the scheduler and must not park unless it
    ``spawn``s a task. Each connection already has a oneshot recv posted.
    """

    resolved = resolve_scheduler(scheduler)
    io = require_proactor_io(resolved)
    size = io._recv_into_cache.buffer_size if recv_size is None else recv_size
    if sock is not None:
        if addr is not None or path is not None:
            raise ValueError("addr/path and sock cannot be specified at the same time")
        listen_sock = prepare_listen_socket(sock, backlog=backlog)
    elif path is not None:
        if addr is not None:
            raise TypeError("start_connection_server() accepts addr= or path=, not both")
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
        raise TypeError("start_connection_server() requires addr=, path=, or sock=")
    server = ConnectionServer(resolved, [listen_sock])
    server._start_accept_loop(listen_sock, callback, recv_size=size)
    return server

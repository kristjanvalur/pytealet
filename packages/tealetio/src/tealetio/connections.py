"""Low-level accepted connections and a create_server-shaped listener.

A ``Connection`` is a socket plus one outstanding oneshot recv into a buffer
from its ``ConnectionServer`` pool (default 64 KiB). The recv slot has a
single consumer: the default is to stash the first completion;
``set_recv_callback`` or ``to_streams`` takes it. If the oneshot has already
finished, the new consumer is invoked immediately (copied into the stream
when wrapping). ``recv_many`` and its buffer group start only when a later
read asks for a chunk past that oneshot. ``to_streams`` hands the socket to
the stream pair; the ``Connection`` is finished and must not be used again.
"""

from __future__ import annotations

import socket
import threading
from collections import deque
from collections.abc import Callable
from typing import Any

from .delivery import is_io_cancellation
from .io_manager import ProactorIOManager
from .scheduler import BaseScheduler
from .socket_helpers import (
    ACCEPT_RETRY_DELAY,
    abortive_close,
    is_accept_resource_error,
    is_soft_accept_error,
)
from .streams.common import require_proactor_io, resolve_scheduler
from .streams.reader import StreamReader
from .streams.server import (
    bind_tcp_socket,
    bind_unix_socket,
    prepare_listen_socket,
)
from .streams.util import DEFAULT_LIMIT
from .streams.writer import StreamWriter, shutdown_stream_writer
from .tasks import CancelledError, Task, get_current

__all__ = [
    "DEFAULT_CONNECTION_RECV_SIZE",
    "Connection",
    "ConnectionServer",
    "ConnectionStreamServer",
    "start_connection_server",
    "start_connection_stream_server",
]

# IOCP-sized first read; each ConnectionServer has its own idle stack
DEFAULT_CONNECTION_RECV_SIZE = 64 * 1024
DEFAULT_MAX_FREE_RECV_INTO_BUFFERS = 64

RecvCallback = Callable[["Connection", memoryview | None, BaseException | None], object]


class RecvIntoBufferCache:
    """Idle stack of bytearrays for oneshot ``recv_into``.

    One cache per ``ConnectionServer``. ``append`` / ``pop`` on the deque are
    treated as thread-safe so accept workers can checkout while the scheduler
    returns buffers without a lock. The idle cap is approximate under concurrency.
    """

    def __init__(
        self,
        *,
        buffer_size: int = DEFAULT_CONNECTION_RECV_SIZE,
        max_free: int | None = DEFAULT_MAX_FREE_RECV_INTO_BUFFERS,
    ) -> None:
        self._buffer_size = buffer_size
        self._max_free = max_free
        self._free: deque[bytearray] = deque()
        self._closed = False

    @property
    def buffer_size(self) -> int:
        return self._buffer_size

    @property
    def max_free(self) -> int | None:
        return self._max_free

    @property
    def free_count(self) -> int:
        return len(self._free)

    def acquire(self) -> bytearray:
        if self._closed:
            raise RuntimeError("recv-into buffer cache is closed")
        try:
            return self._free.pop()
        except IndexError:
            if self._closed:
                raise RuntimeError("recv-into buffer cache is closed") from None
            return bytearray(self._buffer_size)

    def release(self, buf: bytearray) -> None:
        if self._closed:
            return
        if len(buf) != self._buffer_size:
            return
        if self._max_free is not None and len(self._free) >= self._max_free:
            return
        self._free.append(buf)

    def close(self) -> None:
        self._closed = True
        self._free.clear()


class Connection:
    """Accepted socket with one outstanding oneshot recv from the server pool.

    The first recv is posted when the connection is created (accept CQE
    thread). The consumer runs on whichever thread takes the slot: the
    completion thread, if a callback is already installed, or the thread
    that calls ``set_recv_callback`` if the result was already stashed.
    Nothing is marshalled for you. Copy the view and schedule the copy
    yourself if it must run on the scheduler. ``data`` is valid only during
    the call. Empty data is EOF. ``data is None`` with ``exc`` set is a
    transport error.

    The slot lock makes that bind-or-deliver one outcome, including when
    the two sides are different threads.

    ``to_streams`` is that consumer: it feeds the oneshot into the reader
    (now or when it arrives) and returns the stream pair. One consumer.
    After ``to_streams`` returns, do not call the ``Connection`` again.
    ``recv_many`` is armed only if a later read needs another chunk.
    """

    def __init__(
        self,
        io: ProactorIOManager,
        sock: socket.socket,
        buf: bytearray,
        *,
        pool: RecvIntoBufferCache,
    ) -> None:
        self._io = io
        self._sock = sock
        self._pool = pool
        self._buf: bytearray | None = buf
        self._recv_handle: Any = None
        self._recv_callback: RecvCallback | None = None
        self._pending: tuple[int, BaseException | None] | None = None
        self._closed = False
        # bind and completion may run on different threads; one of them takes the slot
        self._recv_slot = threading.Lock()

    @classmethod
    def start(
        cls,
        io: ProactorIOManager,
        sock: socket.socket,
        *,
        pool: RecvIntoBufferCache,
    ) -> Connection:
        """Checkout a pool buffer, post ``recv_into``, and return the connection."""

        buf = pool.acquire()
        conn = cls(io, sock, buf, pool=pool)
        try:
            conn._recv_handle = io.proactor.recv_into(sock, memoryview(buf), conn._on_recv_raw)
        except BaseException:
            conn._buf = None
            pool.release(buf)
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
        # stay on the completion thread; the user marshals if they need the scheduler
        self._complete_recv(0 if nbytes is None else nbytes, exception)

    def _complete_recv(self, nbytes: int, exception: BaseException | None) -> None:
        with self._recv_slot:
            self._recv_handle = None
            if self._closed:
                buf = self._detach_buffer()
                callback = None
            elif self._recv_callback is not None:
                callback = self._recv_callback
                buf = self._detach_buffer()
            else:
                self._pending = (nbytes, exception)
                return
        try:
            if callback is not None:
                self._invoke(callback, buf, nbytes, exception)
        finally:
            self._pool_release(buf)

    def _invoke(
        self,
        callback: RecvCallback,
        buf: bytearray | None,
        nbytes: int,
        exception: BaseException | None,
    ) -> None:
        if exception is not None:
            callback(self, None, exception)
            return
        assert buf is not None
        callback(self, memoryview(buf)[:nbytes], None)

    def _detach_buffer(self) -> bytearray | None:
        buf = self._buf
        self._buf = None
        return buf

    def _pool_release(self, buf: bytearray | None) -> None:
        if buf is not None:
            self._pool.release(buf)

    def set_recv_callback(self, callback: RecvCallback) -> None:
        """Install the recv consumer.

        Safe from any thread. If the oneshot has already been stashed,
        ``callback`` runs immediately on this stack. If it is still in
        flight, the completion thread invokes ``callback`` later. Neither
        side is marshalled onto the scheduler. One consumer; a second call
        is not supported.
        """

        with self._recv_slot:
            self._recv_callback = callback
            pending = self._pending
            if pending is None:
                return
            self._pending = None
            buf = self._detach_buffer()
        nbytes, exception = pending
        try:
            self._invoke(callback, buf, nbytes, exception)
        finally:
            self._pool_release(buf)

    def to_streams(self, *, limit: int = DEFAULT_LIMIT) -> tuple[StreamReader, StreamWriter]:
        """Hand this connection off as ``StreamReader`` / ``StreamWriter``.

        Feeds the oneshot chunk into the reader when it is already pending, or
        when it arrives. The buffer group and ``recv_many`` wait until a read
        asks for a further chunk. Parks in ``read`` / ``readline`` until that
        first chunk if the oneshot is still in flight. Safe from a worker
        thread. The prefix is copied and applied on the scheduler, because
        the reader waits there. After this returns the socket belongs to the
        writer; do not call this ``Connection`` again.
        """

        recv_buffer = self._io._open_sock_recv_iter(
            self._sock,
            owns_pool=True,
            start=False,
            pool_factory=self._io.acquire_recv_buffer_pool,
        )
        send_buffer = self._io._open_send_buffer(self._sock)
        reader = StreamReader(limit=limit, recv_buffer=recv_buffer)
        writer = StreamWriter(send_buffer=send_buffer, sock=self._sock, io=self._io, reader=reader)

        scheduler = self._io._scheduler

        def feed(_conn: Connection, data: memoryview | None, exc: BaseException | None) -> None:
            # copy before return: the pool buffer is released when feed returns
            copied = None if data is None else bytes(data)

            def apply() -> None:
                if exc is not None:
                    recv_buffer.feed_initial(exception=exc)
                    return
                eof = copied is not None and len(copied) == 0
                recv_buffer.feed_initial(copied, eof=eof)

            if scheduler is None:
                apply()
                return
            scheduler.call_on_scheduler(apply)

        self.set_recv_callback(feed)
        return reader, writer

    def send_nowait(self, data: bytes | bytearray | memoryview) -> None:
        """Fire-and-forget sendall. Safe from the accept callback (does not park)."""

        self._io.sock_send_nowait(self._sock, data)

    def send_close_nowait(self, data: bytes | bytearray | memoryview) -> None:
        """Fire-and-forget sendall then close. Safe from the accept callback."""

        def after(_result: object, _exception: BaseException | None) -> None:
            self.close()

        # close from the send completion; selector sock_send_close drops in-flight payload
        self._io.proactor.send(self._sock, data, after)

    def close(self) -> None:
        """Cancel an in-flight oneshot and close the socket.

        Not for use after ``to_streams``: the stream writer owns the socket.
        """

        with self._recv_slot:
            if self._closed:
                return
            self._closed = True
            handle = self._recv_handle
            in_flight = handle is not None and self._pending is None
            if in_flight:
                # pool buffer is released when the cancelled recv completes
                buf = None
            else:
                self._recv_handle = None
                buf = self._detach_buffer()
        if in_flight:
            self._io.cancel_nowait(handle)
        else:
            self._pool_release(buf)
        try:
            self._io.sock_close(self._sock)
        except OSError:
            abortive_close(self._sock)


class ConnectionServer:
    """Listening server that delivers accepted connections, not handler tealets.

    The accept callback runs on the delivery thread, after the oneshot recv
    is posted. It must not park or touch the scheduler; marshal with
    ``call_on_scheduler`` if the work needs a tealet. ``close()`` cancels
    the accept-loop tealet; in-flight connections are not cancelled. Each
    server owns one idle stack of ``recv_size`` recv buffers
    (``DEFAULT_CONNECTION_RECV_SIZE``).
    """

    _io: ProactorIOManager

    def __init__(
        self,
        scheduler: BaseScheduler,
        sockets: list[socket.socket],
        *,
        recv_size: int = DEFAULT_CONNECTION_RECV_SIZE,
    ) -> None:
        if recv_size <= 0:
            raise ValueError("recv_size must be positive")
        self._scheduler = scheduler
        self._io = require_proactor_io(scheduler)
        self._sockets = tuple(sockets)
        self._accept_task: Task | None = None
        self._closed = False
        self._listen_sock: socket.socket | None = None
        self._callback: Callable[[Connection], object] | None = None
        self._recv_pool = RecvIntoBufferCache(buffer_size=recv_size)

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

    @property
    def recv_size(self) -> int:
        """Oneshot recv buffer size for connections accepted by this server."""

        return self._recv_pool.buffer_size

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
        self._recv_pool.close()
        for sock in self._sockets:
            if sock.fileno() != -1:
                sock.close()

    def _start_accept_loop(
        self,
        sock: socket.socket,
        callback: Callable[[Connection], object],
    ) -> None:
        self._listen_sock = sock
        self._callback = callback
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
                        pool=self._recv_pool,
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
    recv_size: int = DEFAULT_CONNECTION_RECV_SIZE,
    scheduler: BaseScheduler | None = None,
) -> ConnectionServer:
    """Start a low-level server that delivers accepted connections.

    Bind kwargs match ``start_server`` (``addr`` / ``path`` / ``sock``).
    ``recv_size`` is the oneshot recv buffer (default
    ``DEFAULT_CONNECTION_RECV_SIZE``, 64 KiB); this server keeps its own idle
    pool of that size. Each connection has that oneshot posted, then
    ``callback`` runs on the accept delivery thread. It may call
    ``to_streams`` or ``set_recv_callback``. It must not park or touch the
    scheduler; ``call_on_scheduler`` the work that needs a tealet. A late
    delivery after ``close()`` is discarded with ``conn.close()``. A
    ``callback`` that raises closes the connection and is reported on the
    scheduler.
    """

    if recv_size <= 0:
        raise ValueError("recv_size must be positive")
    resolved = resolve_scheduler(scheduler)
    io = require_proactor_io(resolved)
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
    server = ConnectionServer(resolved, [listen_sock], recv_size=recv_size)
    server._start_accept_loop(listen_sock, callback)
    return server


class ConnectionStreamServer:
    """Stream server built on ``ConnectionServer``.

    The connection callback calls ``to_streams`` on the accept delivery
    thread, then marshals the pair to the scheduler, which spawns the
    handler tealet. ``recv_many`` stays deferred until a read needs a chunk
    past the oneshot.
    """

    def __init__(self, inner: ConnectionServer, handlers: set[Task]) -> None:
        self._inner = inner
        self._handler_tasks = handlers

    def __enter__(self) -> ConnectionStreamServer:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()
        self.wait_closed()

    @property
    def sockets(self) -> tuple[socket.socket, ...]:
        return self._inner.sockets

    @property
    def accept_task(self) -> Task | None:
        return self._inner.accept_task

    def close(self) -> None:
        self._inner.close()

    def wait_closed(self) -> None:
        self._inner.wait_closed()
        for handler in tuple(self._handler_tasks):
            if not handler.done():
                handler.wait()

    def serve_forever(self) -> None:
        self._inner.serve_forever()


def start_connection_stream_server(
    client_handler: Callable[[StreamReader, StreamWriter], object],
    *,
    addr: tuple[str | None, int] | None = None,
    path: str | None = None,
    sock: socket.socket | None = None,
    family: int = socket.AF_INET,
    backlog: int = 100,
    reuse_address: bool | None = None,
    reuse_port: bool | None = None,
    limit: int = DEFAULT_LIMIT,
    recv_size: int = DEFAULT_CONNECTION_RECV_SIZE,
    handler_eager_start: bool = False,
    scheduler: BaseScheduler | None = None,
) -> ConnectionStreamServer:
    """Start a stream server on top of ``start_connection_server``.

    Bind kwargs match ``start_server``. Each accept posts the oneshot recv.
    The connection callback builds the stream pair on the delivery thread
    and marshals it; the scheduler spawns the handler tealet. Plaintext
    sync handlers only.
    """

    resolved = resolve_scheduler(scheduler)
    handlers: set[Task] = set()
    inner_box: list[ConnectionServer | None] = [None]

    def on_connection(conn: Connection) -> None:
        reader, writer = conn.to_streams(limit=limit)

        def deliver() -> None:
            inner = inner_box[0]
            if inner is not None and inner._closed:
                shutdown_stream_writer(writer, best_effort=True)
                return

            def serve() -> None:
                try:
                    client_handler(reader, writer)
                finally:
                    shutdown_stream_writer(writer)

            try:
                # False, not None: do not inherit a scheduler-wide eager factory
                handler = resolved.spawn(serve, eager_start=handler_eager_start)
            except Exception as exc:
                shutdown_stream_writer(writer, best_effort=True)
                resolved.call_exception_handler(
                    {
                        "message": "Exception spawning connection stream server handler",
                        "exception": exc,
                        "scheduler": resolved,
                        "handle": None,
                    }
                )
                return
            handlers.add(handler)
            handler.add_done_callback(lambda _task: handlers.discard(handler))

        resolved.call_on_scheduler(deliver)

    inner = start_connection_server(
        on_connection,
        addr=addr,
        path=path,
        sock=sock,
        family=family,
        backlog=backlog,
        reuse_address=reuse_address,
        reuse_port=reuse_port,
        recv_size=recv_size,
        scheduler=resolved,
    )
    inner_box[0] = inner
    return ConnectionStreamServer(inner, handlers)

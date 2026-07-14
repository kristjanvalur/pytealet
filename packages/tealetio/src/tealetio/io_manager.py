from __future__ import annotations

import socket
from collections.abc import Callable, Iterable, Iterator
from typing import TYPE_CHECKING, Any, Protocol, TypeVar, cast, runtime_checkable

from .files import IOFile, ProactorFile, parse_open_mode
from .continuous_callbacks import (
    AcceptDelivery,
    AcceptReadResult,
    AcceptRecvErrorCallback,
    AcceptStreamsDelivery,
    finalize_accept_recv_error,
    normalize_accept_recv_size,
    is_cancellation_delivery,
    wrap_accept_delivery,
)
from .io_waiter import (
    IOOperation,
    IOWaiter,
    IOWaitGroup,
    IOWaitGroupChild,
    IOWaitGroupChildProtocol,
    IOWaitable,
)

from .io_buffers import RecvIterBuffer, SendBuffer, _RecvIterProactor, open_recv_iter_buffer, open_send_buffer
from .operations import MultishotDelivery, Operation
from .tasks import CancelledError
from .socket_helpers import abortive_close
from .types import SocketSendBuffer

if TYPE_CHECKING:
    from .proactor import Proactor, RecvBufferPool
    from .scheduler import BaseScheduler, TimerHandle

T = TypeVar("T")


def _finish_or_close_socket(group: IOWaitGroup[Any], sock: socket.socket, result: Any) -> None:
    if not group.finish(result):
        abortive_close(sock)


_ProgressCallback = Callable[[int], object]
_RecvProgressCallback = Callable[[bytes], object]
_RecvIterYield = tuple[int, memoryview]

# sockaddr shapes vary by family; tighten when accept/connect types are unified.
SocketAddress = Any

IO_UNSUPPORTED_ERROR = "operation requires a scheduler with IO support"
SELECTOR_IO_UNSUPPORTED_ERROR = (
    "stream helpers require a proactor scheduler; selector schedulers "
    "use scheduler.sock_* until SelectorIOManager is available"
)

__all__ = [
    "FileIO",
    "IO_UNSUPPORTED_ERROR",
    "IOFile",
    "IOOperation",
    "IOWaiter",
    "IOWaitGroup",
    "IOWaitGroupChild",
    "IOWaitGroupChildProtocol",
    "IOWaitable",
    "PollIO",
    "ProactorAccess",
    "ProactorIOManager",
    "ProactorSocketIO",
    "SELECTOR_IO_UNSUPPORTED_ERROR",
    "ServerIO",
    "SocketAddress",
    "SocketSendBuffer",
    "SocketIO",
    "SupportsProactorIO",
]


class SupportsProactorIO(Protocol):
    """Scheduler that exposes a proactor-backed ``scheduler.io`` facade.

    Use for static typing after narrowing (for example ``isinstance(scheduler,
    ProactorScheduler)``). Do not rely on ``isinstance(..., SupportsProactorIO)``
    at runtime: schedulers without a real IO backend may still define an ``io``
    property that raises.
    """

    @property
    def io(self) -> "ProactorIOManager": ...


class ProactorAccess(Protocol):
    """IO facade with access to proactor submission (``accept_many``, …)."""

    @property
    def proactor(self) -> "Proactor": ...


@runtime_checkable
class SocketIO(Protocol):
    """Asyncio-shaped socket helpers; one-shot methods return ``IOWaiter``."""

    def sock_recv(self, sock: socket.socket, n: int) -> IOWaiter[bytes]: ...

    def sock_recv_into(self, sock: socket.socket, buf: Any) -> IOWaiter[int]: ...

    def sock_recvfrom(self, sock: socket.socket, bufsize: int) -> IOWaiter[tuple[bytes, Any]]: ...

    def sock_recvfrom_into(self, sock: socket.socket, buf: Any, nbytes: int = 0) -> IOWaiter[tuple[int, Any]]: ...

    def sock_sendall(
        self,
        sock: socket.socket,
        data: SocketSendBuffer,
        progress: _ProgressCallback | None = None,
    ) -> IOWaiter[None]: ...

    def sock_send_iter(
        self,
        sock: socket.socket,
        chunks: Iterable[SocketSendBuffer],
    ) -> None: ...

    def sock_sendto(self, sock: socket.socket, data: SocketSendBuffer, address: Any) -> IOWaiter[int]: ...

    def sock_accept(
        self,
        sock: socket.socket,
        n: int | None = None,
    ) -> IOWaitable[AcceptDelivery]: ...

    def sock_connect(
        self,
        sock: socket.socket,
        address: Any,
        *,
        initial: SocketSendBuffer | None = None,
    ) -> IOWaitable[None]: ...

    def sock_create(
        self,
        family: int,
        type: int,
        proto: int = 0,
        *,
        flags: int = 0,
        connect_to: Any | None = None,
        initial_data: SocketSendBuffer | None = None,
    ) -> IOWaitable[socket.socket]: ...

    def sock_recv_iter(
        self, sock: socket.socket, buffer_pool: "RecvBufferPool | None" = None
    ) -> Iterator[_RecvIterYield]: ...

    def sock_recvall(
        self,
        sock: socket.socket,
        progress: _RecvProgressCallback | None = None,
        *,
        buffer_pool: "RecvBufferPool | None" = None,
    ) -> bytes: ...

    def sock_shutdown(self, sock: socket.socket, how: int) -> IOWaiter[None]: ...

    def sock_close(self, sock: socket.socket) -> IOWaiter[None]: ...

    def create_recv_buffer_pool(self, buffer_size: int, buffer_count: int) -> "RecvBufferPool": ...

    def shared_recv_buffer_pool(self) -> "RecvBufferPool": ...

    def set_shared_recv_buffer_pool(self, pool: "RecvBufferPool") -> None: ...


# Stream-facing protocol slices live in ``streams.open`` / ``streams.writer`` as
# ``StreamOpenIO`` and ``StreamWriterIO``. ``ProactorIOManager`` satisfies both
# structurally without inheriting those protocols.


@runtime_checkable
class PollIO(Protocol):
    """Poll helpers over a scheduler IO backend."""

    def poll(self, fd: int, mask: int) -> IOWaiter[int]: ...

    def poll_many(
        self,
        fd: int,
        mask: int,
        callback: Callable[[MultishotDelivery], object],
    ) -> IOWaitable[None]: ...


@runtime_checkable
class FileIO(Protocol):
    """Positioned binary file open helper over a scheduler IO backend."""

    def open(self, path: str, mode: str = "rb") -> IOWaiter[IOFile]: ...


class ServerIO(SocketIO, ProactorAccess, Protocol):
    """Socket IO plus proactor submission for stream servers.

    Static typing only: ``proactor`` is a property (same limitation as ``IOFile``).
    At runtime use ``isinstance(io, SocketIO)`` and ``io.proactor``; do not rely
    on ``isinstance(io, ServerIO)`` or ``isinstance(io, ProactorSocketIO)``.
    """

    def accept_many(
        self,
        sock: socket.socket,
        callback: Callable[[AcceptDelivery], object],
        *,
        recv_size: int | None = None,
        recv_timeout: float | None = None,
        on_recv_error: AcceptRecvErrorCallback | None = None,
    ) -> IOWaitable[None]: ...

    def accept_many_streams(
        self,
        sock: socket.socket,
        callback: Callable[[AcceptStreamsDelivery], object],
        *,
        limit: int = 2**16,
        stream_factory: Any | None = None,
        async_: bool = False,
    ) -> IOWaitable[None]: ...

    def sock_create_streams(
        self,
        family: int,
        type: int,
        proto: int = 0,
        *,
        connect_to: Any,
        flags: int = 0,
        initial_data: SocketSendBuffer | None = None,
        limit: int = 2**16,
        stream_factory: Any | None = None,
        async_: bool = False,
    ) -> IOWaiter[AcceptStreamsDelivery]: ...


ProactorSocketIO = ServerIO


class ProactorIOManager:
    """IO facade over a ``Proactor`` backend.

    One-shot helpers return ``IOWaiter``; call ``wait()`` to block the current
    tealet. Continuous helpers (``accept_many``, ``poll_many``) return
    ``IOWaitable[None]``; call ``wait()`` to block until the stream ends.
    ``sock_recv_iter`` remains a blocking iterator over ``recv_many`` chunks.
    Always owned by a proactor scheduler.

    Structurally implements ``StreamOpenIO`` and ``StreamWriterIO`` (defined in
    ``streams.open`` / ``streams.writer``) for stream-pair construction.
    """

    def __init__(self, scheduler: BaseScheduler, proactor: Proactor) -> None:
        self._scheduler = scheduler
        self._proactor = proactor
        self._closed = False

    @property
    def proactor(self) -> Proactor:
        self._check_open()
        return self._proactor

    def close(self) -> None:
        """Release scheduler ownership; called from ``ProactorScheduler.close()``."""

        self._closed = True
        self._scheduler = None

    def _check_open(self) -> None:
        if self._closed:
            raise RuntimeError("IO manager is closed")

    def _marshal_on_scheduler(self, thunk: Callable[[], object]) -> None:
        self._check_open()
        assert self._scheduler is not None
        self._scheduler.call_soon_threadsafe(thunk)

    def _cancel_operation(self, operation: Operation[Any]) -> IOWaitable[None]:
        """Cancel ``operation`` and return a waitable for its teardown leg.

        Internal helper for io_manager composition paths that hold raw
        ``Operation`` handles (for example accept-time ``recv``). Returns a
        teardown ``IOWaitable``; call ``wait()`` to block until ring cancel
        settles, or ``forget()`` when only the target's terminal state matters.
        The waitable is already complete when the target was done or produced no
        async backend teardown operation.
        """

        return IOWaiter(self, self._proactor.cancel(operation))

    def sock_recv(self, sock: socket.socket, n: int) -> IOWaiter[bytes]:
        return IOWaiter(self, self._proactor.recv(sock, n))

    def create_recv_buffer_pool(self, buffer_size: int, buffer_count: int) -> RecvBufferPool:
        return self._proactor.create_recv_buffer_pool(buffer_size, buffer_count)

    def shared_recv_buffer_pool(self) -> RecvBufferPool:
        return self._proactor.shared_recv_buffer_pool()

    def set_shared_recv_buffer_pool(self, pool: RecvBufferPool) -> None:
        self._proactor.set_shared_recv_buffer_pool(pool)

    def _resolve_recv_buffer_pool(self, buffer_pool: RecvBufferPool | None) -> RecvBufferPool:
        if buffer_pool is None:
            return self._proactor.shared_recv_buffer_pool()
        return buffer_pool

    def _open_sock_recv_iter(self, sock: socket.socket, buffer_pool: RecvBufferPool | None) -> RecvIterBuffer:
        pool = self._resolve_recv_buffer_pool(buffer_pool)
        proactor = cast(_RecvIterProactor, self._proactor)
        return open_recv_iter_buffer(
            sock,
            proactor=proactor,
            buf_group=pool,
            scheduler=self._scheduler,
        )

    def sock_recv_iter(
        self, sock: socket.socket, buffer_pool: RecvBufferPool | None = None
    ) -> Iterator[_RecvIterYield]:
        buffer = self._open_sock_recv_iter(sock, buffer_pool)
        try:
            while True:
                item = buffer.take_next()
                if item is None:
                    break
                yield item
        finally:
            buffer.close()

    def sock_recvall(
        self,
        sock: socket.socket,
        progress: _RecvProgressCallback | None = None,
        *,
        buffer_pool: RecvBufferPool | None = None,
    ) -> bytes:
        if progress is None:
            process = bytes
        else:

            def process(chunk: memoryview) -> bytes:
                cargo = bytes(chunk)
                if cargo:
                    progress(cargo)
                return cargo

        return b"".join(process(chunk) for _index, chunk in self.sock_recv_iter(sock, buffer_pool))

    def sock_recv_into(self, sock: socket.socket, buf: Any) -> IOWaiter[int]:
        return IOWaiter(self, self._proactor.recv_into(sock, buf))

    def sock_recvfrom(self, sock: socket.socket, bufsize: int) -> IOWaiter[tuple[bytes, Any]]:
        return IOWaiter(self, self._proactor.recvfrom(sock, bufsize))

    def sock_recvfrom_into(self, sock: socket.socket, buf: Any, nbytes: int = 0) -> IOWaiter[tuple[int, Any]]:
        return IOWaiter(self, self._proactor.recvfrom_into(sock, buf, nbytes))

    def sock_sendall(self, sock: socket.socket, data: Any, progress: _ProgressCallback | None = None) -> IOWaiter[None]:
        return IOWaiter(self, self._proactor.send(sock, data, progress))

    def _open_send_buffer(self, sock: socket.socket) -> SendBuffer:
        return open_send_buffer(sock, io=self, scheduler=self._scheduler)

    def sock_send_iter(
        self,
        sock: socket.socket,
        chunks: Iterable[bytes | bytearray | memoryview],
    ) -> None:
        for chunk in chunks:
            if not chunk:
                continue
            self.sock_sendall(sock, memoryview(chunk)).wait()

    def sock_sendto(self, sock: socket.socket, data: Any, address: Any) -> IOWaiter[int]:
        return IOWaiter(self, self._proactor.sendto(sock, data, address))

    def sock_shutdown(self, sock: socket.socket, how: int) -> IOWaiter[None]:
        return IOWaiter(self, self._proactor.shutdown(sock, how))

    def sock_close(self, sock: socket.socket) -> IOWaiter[None]:
        return IOWaiter(self, self._proactor.close_socket(sock))

    def sock_accept(
        self,
        sock: socket.socket,
        n: int | None = None,
    ) -> IOWaitable[AcceptDelivery]:
        normalized_recv_size = normalize_accept_recv_size(n)
        if normalized_recv_size is None:
            return IOWaiter(
                self,
                self._proactor.accept(sock),
                map_result=lambda conn: (conn, None),
            )

        group = IOWaitGroup(self)

        def advance_accept(child: IOWaitGroupChildProtocol[socket.socket]) -> None:
            conn = child.value()

            def advance_recv(recv_child: IOWaitGroupChildProtocol[bytes]) -> None:
                data = recv_child.value()
                _finish_or_close_socket(group, conn, (conn, data))

            try:
                group.attach(
                    self._proactor.recv(conn, normalized_recv_size),
                    on_cleanup=lambda fail, _value: abortive_close(conn) if fail else None,
                    advance=advance_recv,
                )
            except BaseException:
                abortive_close(conn)
                raise

        group.attach(
            self._proactor.accept(sock),
            advance=advance_accept,
        )
        return group

    def sock_connect(
        self,
        sock: socket.socket,
        address: Any,
        *,
        initial: SocketSendBuffer | None = None,
    ) -> IOWaitable[None]:
        if initial is None:
            return IOWaiter(self, self._proactor.connect(sock, address))

        payload = memoryview(initial)
        if not payload:
            return IOWaiter(self, self._proactor.connect(sock, address))

        group = IOWaitGroup(self)

        def advance_connect(_child: IOWaitGroupChildProtocol[None]) -> None:
            def advance_send(_send_child: IOWaitGroupChildProtocol[None]) -> None:
                _finish_or_close_socket(group, sock, None)

            group.attach(
                self._proactor.send(sock, payload),
                advance=advance_send,
            )

        group.attach(self._proactor.connect(sock, address), advance=advance_connect)
        return group

    def sock_create(
        self,
        family: int,
        type: int,
        proto: int = 0,
        *,
        flags: int = 0,
        connect_to: Any | None = None,
        initial_data: SocketSendBuffer | None = None,
    ) -> IOWaitable[socket.socket]:
        if initial_data is not None and connect_to is None:
            raise ValueError("initial_data requires connect_to")

        if connect_to is None:
            return IOWaiter(
                self,
                self._proactor.create_socket(
                    family,
                    type,
                    proto,
                    flags=flags,
                ),
            )

        group = IOWaitGroup(self)
        payload = memoryview(initial_data) if initial_data is not None else None

        def advance_connect(child: IOWaitGroupChildProtocol[socket.socket]) -> None:
            sock = child.value()

            def finish_connected(_connect_child: IOWaitGroupChildProtocol[None]) -> None:
                if payload is None or not payload:
                    _finish_or_close_socket(group, sock, sock)
                    return

                def advance_send(_send_child: IOWaitGroupChildProtocol[None]) -> None:
                    _finish_or_close_socket(group, sock, sock)

                group.attach(
                    self._proactor.send(sock, payload),
                    on_cleanup=lambda fail, _value: abortive_close(sock) if fail else None,
                    advance=advance_send,
                )

            group.attach(
                self._proactor.connect(sock, connect_to),
                on_cleanup=lambda fail, _value: abortive_close(sock) if fail else None,
                advance=finish_connected,
            )

        group.attach(
            self._proactor.create_socket(family, type, proto, flags=flags),
            on_cleanup=lambda fail, value: abortive_close(value) if not fail else None,
            advance=advance_connect,
        )
        return group

    def poll(self, fd: int, mask: int) -> IOWaiter[int]:
        return IOWaiter(self, self._proactor.poll(fd, mask))

    def read(self, fd: int, n: int, offset: int) -> IOWaiter[bytes]:
        return IOWaiter(self, self._proactor.read(fd, n, offset))

    def read_into(self, fd: int, buf: Any, offset: int) -> IOWaiter[int]:
        return IOWaiter(self, self._proactor.read_into(fd, buf, offset))

    def write(self, fd: int, data: Any, offset: int) -> IOWaiter[int]:
        return IOWaiter(self, self._proactor.write(fd, data, offset))

    def stat_fdsize(self, fd: int) -> IOWaiter[int]:
        return IOWaiter(self, self._proactor.stat_fdsize(fd))

    def close_fd(self, fd: int) -> IOWaiter[None]:
        return IOWaiter(self, self._proactor.close_fd(fd))

    def poll_many(
        self,
        fd: int,
        mask: int,
        callback: Callable[[MultishotDelivery], object],
    ) -> IOWaitable[None]:
        return IOWaiter(self, self._proactor.poll_many(fd, mask, callback))

    def _schedule_accept_recv_timeout(
        self,
        recv_op: Operation[bytes],
        timer_box: list[TimerHandle | None],
        *,
        timeout: float,
    ) -> None:
        """Arm a scheduler timer that cancels ``recv_op`` when it fires."""

        def arm() -> None:
            if recv_op.done():
                return

            def on_timeout() -> None:
                if not recv_op.done():
                    self._cancel_operation(recv_op).forget()

            assert self._scheduler is not None
            timer_box[0] = self._scheduler.call_later(timeout, on_timeout)

        self._marshal_on_scheduler(arm)

    def _cancel_accept_recv_timeout(self, timer_box: list[TimerHandle | None]) -> None:
        def cancel() -> None:
            handle = timer_box[0]
            if handle is not None:
                handle.cancel()
                timer_box[0] = None

        self._marshal_on_scheduler(cancel)

    def _accept_many_read_on_conn(
        self,
        deliver: Callable[[AcceptReadResult], object],
        *,
        recv_size: int,
        recv_timeout: float | None = None,
    ) -> Callable[[MultishotDelivery], None]:
        """Return a proactor ``accept_many`` callback that pre-reads each accept."""

        def on_conn(delivery: MultishotDelivery) -> None:
            if is_cancellation_delivery(delivery):
                return
            if delivery.exception is not None:
                raise delivery.exception
            conn = delivery.value
            if conn is None:
                return
            recv_op = self._proactor.recv(conn, recv_size)
            timer_box: list[TimerHandle | None] = [None]

            if recv_timeout is not None:
                self._schedule_accept_recv_timeout(
                    recv_op,
                    timer_box,
                    timeout=recv_timeout,
                )

            def on_recv_complete(op: Operation[bytes]) -> None:
                self._cancel_accept_recv_timeout(timer_box)
                exc = op.exception()
                if exc is not None:
                    if isinstance(exc, CancelledError):
                        abortive_close(conn)
                        return
                    deliver((conn, None, exc))
                    return
                deliver((conn, op.result(), None))

            recv_op.add_done_callback(on_recv_complete)

        return on_conn

    def accept_many(
        self,
        sock: socket.socket,
        callback: Callable[[AcceptDelivery], object],
        *,
        recv_size: int | None = None,
        recv_timeout: float | None = None,
        on_recv_error: AcceptRecvErrorCallback | None = None,
    ) -> IOWaitable[None]:
        """Start ``proactor.accept_many`` with optional accept-time pre-read.

        **Shutdown and late deliveries.** Cancelling this ``IOWaitable`` or the
        hosting accept-loop tealet does **not** cancel accept-time ``recv`` legs
        started when ``recv_size`` is set. Late accepts and preread completions
        can still reach ``callback`` after shutdown unless the application
        discards them — close listening sockets, check a shutdown flag in the
        accept callback, and ignore or close unwanted connections (``StreamServer``
        uses ``_closed`` for this). ``recv_timeout`` (requires ``recv_size``)
        bounds each accept-time preread cooperatively; it does not replace
        listener close or callback-side discard.

        Deliveries are marshalled onto the scheduler thread before ``callback``
        runs. Recv failures invoke ``on_recv_error(conn, exc)`` when provided;
        the socket is always closed afterwards. With no ``on_recv_error``, recv
        failures close the socket silently.

        When ``recv_timeout`` is set, each accept-time ``recv`` is cancelled if
        it has not completed by then. Timeout cancel is cooperative/best-effort
        like other cancel paths: teardown legs are not awaited before the socket
        is closed in the recv done callback.

        ``wait()`` on the returned ``IOWaitable`` ends the accept **stream leg**
        only. On non-multishot backends the stream finishes after each accept;
        accept-time ``recv`` and scheduler-marshalled deliveries may still be in
        flight. Re-arm in a loop when more accepts are needed.
        """

        normalized_recv_size = normalize_accept_recv_size(recv_size)
        if recv_timeout is not None:
            if normalized_recv_size is None:
                raise ValueError("recv_timeout requires recv_size")
            if recv_timeout <= 0:
                raise ValueError("recv_timeout must be positive when provided")

        def deliver_wrapped(result: AcceptReadResult) -> None:
            conn, initial_data, recv_error = result

            def run() -> None:
                if recv_error is not None:
                    finalize_accept_recv_error(conn, recv_error, on_recv_error)
                    return
                try:
                    callback((conn, initial_data))
                except BaseException:
                    abortive_close(conn)
                    raise

            self._marshal_on_scheduler(run)

        if normalized_recv_size is not None:
            return IOWaiter(
                self,
                self._proactor.accept_many(
                    sock,
                    self._accept_many_read_on_conn(
                        deliver_wrapped,
                        recv_size=normalized_recv_size,
                        recv_timeout=recv_timeout,
                    ),
                ),
            )

        return IOWaiter(self, self._proactor.accept_many(sock, wrap_accept_delivery(deliver_wrapped)))

    def accept_many_streams(
        self,
        sock: socket.socket,
        callback: Callable[[AcceptStreamsDelivery], object],
        *,
        limit: int = 2**16,
        stream_factory: Any | None = None,
        async_: bool = False,
    ) -> IOWaitable[None]:
        """Start ``proactor.accept_many`` and deliver a stream pair per accept.

        Each accepted socket is wrapped as streams on the accept **delivery**
        thread (``_open_streams`` / ``RecvIterBuffer`` / ``recv_many`` start
        there). Only the user ``callback`` is marshalled onto the scheduler
        thread. Receive begins immediately to reduce latency; a silent peer leaves
        ``recv_many`` pending without withholding the stream pair from the
        handler. Idle or slow-client policy belongs in the handler (read
        timeouts, early close, etc.).

        See ``accept_many()`` for ``wait()`` / accept-stream semantics and the
        shutdown discard responsibilities (close listeners; check a flag in the
        accept callback).
        """

        def deliver_accept(accepted: AcceptReadResult) -> None:
            conn, _initial_data, _recv_error = accepted

            writer: Any = None
            try:
                reader, writer = open_streams(
                    self,
                    conn,
                    limit=limit,
                    stream_factory=stream_factory,
                    async_=async_,
                )
            except BaseException:
                abortive_close(conn)
                raise

            def run() -> None:
                try:
                    callback((reader, writer))
                except BaseException:
                    try:
                        writer.close()
                    except BaseException:
                        abortive_close(conn)
                    raise

            try:
                self._marshal_on_scheduler(run)
            except BaseException:
                # ``writer.close()`` can touch the scheduler via ``RecvIterBuffer``;
                # always abort the accepted socket when marshalling never ran.
                abortive_close(conn)
                try:
                    writer.close()
                except BaseException:
                    pass
                raise

        return IOWaiter(self, self._proactor.accept_many(sock, wrap_accept_delivery(deliver_accept)))

    def sock_create_streams(
        self,
        family: int,
        type: int,
        proto: int = 0,
        *,
        connect_to: Any,
        flags: int = 0,
        initial_data: SocketSendBuffer | None = None,
        limit: int = 2**16,
        stream_factory: Any | None = None,
        async_: bool = False,
    ) -> IOWaitable[AcceptStreamsDelivery]:
        """Create a socket, connect, and return stream endpoints.

        ``initial_data`` is sent on the wire after connect, before streams open.
        """

        group = IOWaitGroup(self)
        payload = memoryview(initial_data) if initial_data is not None else None

        def open_and_finish(sock: socket.socket) -> None:
            try:
                streams = open_streams(
                    self,
                    sock,
                    limit=limit,
                    stream_factory=stream_factory,
                    async_=async_,
                )
            except BaseException:
                abortive_close(sock)
                raise
            if not group.finish(streams):
                _reader, writer = streams
                writer.close()

        def advance_connect(child: IOWaitGroupChildProtocol[socket.socket]) -> None:
            sock = child.value()

            def finish_connected(_connect_child: IOWaitGroupChildProtocol[None]) -> None:
                if payload is None or not payload:
                    open_and_finish(sock)
                    return
                group.attach(
                    self._proactor.send(sock, payload),
                    on_cleanup=lambda fail, _value: abortive_close(sock) if fail else None,
                    advance=lambda _send_child: open_and_finish(sock),
                )

            group.attach(
                self._proactor.connect(sock, connect_to),
                on_cleanup=lambda fail, _value: abortive_close(sock) if fail else None,
                advance=finish_connected,
            )

        group.attach(
            self._proactor.create_socket(family, type, proto, flags=flags),
            on_cleanup=lambda fail, value: abortive_close(value) if not fail else None,
            advance=advance_connect,
        )
        return group

    def open(self, path: str, mode: str = "rb") -> IOWaiter[IOFile]:
        flags, file_mode = parse_open_mode(mode)
        try:
            operation = self._proactor.openat(path, flags, file_mode)
        except NotImplementedError as exc:
            raise NotImplementedError("file I/O requires a proactor with openat support") from exc

        def make_file(fd: int) -> IOFile:
            try:
                return ProactorFile(
                    self,
                    fd,
                    path=path,
                    flags=flags,
                    append="a" in mode,
                )
            except BaseException:
                try:
                    self.close_fd(fd).wait()
                except OSError:
                    pass
                raise

        return IOWaiter(self, operation, map_result=make_file)


from .streams.open import open_streams  # noqa: E402

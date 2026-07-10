from __future__ import annotations

import socket
from collections.abc import Callable, Iterable, Iterator
from typing import TYPE_CHECKING, Any, Protocol, TypeVar, runtime_checkable

from .files import IOFile, ProactorFile, parse_open_mode
from .continuous_callbacks import (
    AcceptManyDelivery,
    AcceptReadResult,
    AcceptRecvErrorCallback,
    AcceptStreamsDelivery,
    accept_read_delivery,
    finalize_accept_recv_error,
    normalize_accept_recv_size,
    wrap_accept_delivery,
)
from .io_waiter import (
    IOOperation,
    IOWaiter,
    IOWaiterChainable,
    IOWaiterChainableProtocol,
    IOWaiterFake,
    IOWaiterProtocol,
)

from .operations import ContinuousOperation, Operation
from .socket_helpers import abortive_close
from .types import SocketSendBuffer

if TYPE_CHECKING:
    from .proactor import Proactor, RecvBufferPool
    from .scheduler import BaseScheduler

T = TypeVar("T")

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
    "IOWaiterChainableProtocol",
    "IOWaiterFake",
    "IOWaiterProtocol",
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
    """Asyncio-shaped socket helpers; each method returns an ``IOOperation``."""

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

    def sock_accept(self, sock: socket.socket) -> IOWaiter[socket.socket]: ...

    def sock_connect(
        self,
        sock: socket.socket,
        address: Any,
        *,
        initial: SocketSendBuffer | None = None,
    ) -> IOWaiter[None]: ...

    def sock_create(
        self,
        family: int,
        type: int,
        proto: int = 0,
        *,
        flags: int = 0,
        connect_to: Any | None = None,
        initial_data: SocketSendBuffer | None = None,
    ) -> IOWaiter[socket.socket]: ...

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


@runtime_checkable
class PollIO(Protocol):
    """Poll helpers over a scheduler IO backend."""

    def poll(self, fd: int, mask: int) -> IOWaiter[int]: ...

    def poll_many(
        self,
        fd: int,
        mask: int,
        callback: Callable[[int], object],
    ) -> ContinuousOperation[int]: ...


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
        callback: Callable[[AcceptManyDelivery], object],
        *,
        recv_size: int | None = None,
        on_recv_error: AcceptRecvErrorCallback | None = None,
    ) -> ContinuousOperation[socket.socket]: ...

    def accept_many_streams(
        self,
        sock: socket.socket,
        callback: Callable[[AcceptStreamsDelivery], object],
        *,
        limit: int = 2**16,
        stream_factory: Any | None = None,
        async_: bool = False,
        recv_size: int | None = None,
        on_recv_error: AcceptRecvErrorCallback | None = None,
    ) -> ContinuousOperation[socket.socket]: ...

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
    tealet. Streaming helpers (``accept_many``, ``poll_many``, ``sock_recv_iter``)
    return long-lived handles. Always owned by a proactor scheduler.
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

    def _waiter(
        self,
        operation: Operation[Any],
        *,
        map_result: Callable[[Any], T] | None = None,
        create_next: Callable[[IOWaiterChainableProtocol[Any]], IOWaiterProtocol[Any]] | None = None,
    ) -> IOWaiter[T]:
        if create_next is not None:
            return IOWaiterChainable(self, operation, map_result=map_result, create_next=create_next)
        return IOWaiter(self, operation, map_result=map_result)

    def sock_recv(self, sock: socket.socket, n: int) -> IOWaiter[bytes]:
        return self._waiter(self._proactor.recv(sock, n))

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

    def _open_sock_recv_iter(self, sock: socket.socket, buffer_pool: RecvBufferPool | None):
        from .recv_iter import RecvIterBuffer

        pool = self._resolve_recv_buffer_pool(buffer_pool)
        buffer = RecvIterBuffer(buf_group=pool)
        stream = self._proactor.recv_many(sock, buffer.on_result, buf_group=pool)
        buffer.attach_stream(stream)
        return buffer

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
        return self._waiter(self._proactor.recv_into(sock, buf))

    def sock_recvfrom(self, sock: socket.socket, bufsize: int) -> IOWaiter[tuple[bytes, Any]]:
        return self._waiter(self._proactor.recvfrom(sock, bufsize))

    def sock_recvfrom_into(self, sock: socket.socket, buf: Any, nbytes: int = 0) -> IOWaiter[tuple[int, Any]]:
        return self._waiter(self._proactor.recvfrom_into(sock, buf, nbytes))

    def sock_sendall(self, sock: socket.socket, data: Any, progress: _ProgressCallback | None = None) -> IOWaiter[None]:
        return self._waiter(self._proactor.send(sock, data, progress))

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
        return self._waiter(self._proactor.sendto(sock, data, address))

    def sock_shutdown(self, sock: socket.socket, how: int) -> IOWaiter[None]:
        return self._waiter(self._proactor.shutdown(sock, how))

    def sock_close(self, sock: socket.socket) -> IOWaiter[None]:
        return self._waiter(self._proactor.close_socket(sock))

    def sock_accept(self, sock: socket.socket) -> IOWaiter[socket.socket]:
        return self._waiter(self._proactor.accept(sock))

    def sock_connect(
        self,
        sock: socket.socket,
        address: Any,
        *,
        initial: SocketSendBuffer | None = None,
    ) -> IOWaiter[None]:
        if initial is None:
            return self._waiter(self._proactor.connect(sock, address))
        from .operation_callbacks import connect_initial_send_operation_factory

        return self._waiter(
            self._proactor.connect(
                sock,
                address,
                operation_factory=connect_initial_send_operation_factory(self._proactor, initial),
            )
        )

    def sock_create(
        self,
        family: int,
        type: int,
        proto: int = 0,
        *,
        flags: int = 0,
        connect_to: Any | None = None,
        initial_data: SocketSendBuffer | None = None,
    ) -> IOWaiter[socket.socket]:
        if initial_data is not None and connect_to is None:
            raise ValueError("initial_data requires connect_to")
        if connect_to is None:
            return self._waiter(
                self._proactor.create_socket(
                    family,
                    type,
                    proto,
                    flags=flags,
                )
            )

        from .operation_callbacks import create_connect_operation_factory

        return self._waiter(
            self._proactor.create_socket(
                family,
                type,
                proto,
                flags=flags,
                operation_factory=create_connect_operation_factory(
                    self._proactor,
                    connect_to,
                    initial_data,
                ),
            )
        )

    def poll(self, fd: int, mask: int) -> IOWaiter[int]:
        return self._waiter(self._proactor.poll(fd, mask))

    def read(self, fd: int, n: int, offset: int) -> IOWaiter[bytes]:
        return self._waiter(self._proactor.read(fd, n, offset))

    def read_into(self, fd: int, buf: Any, offset: int) -> IOWaiter[int]:
        return self._waiter(self._proactor.read_into(fd, buf, offset))

    def write(self, fd: int, data: Any, offset: int) -> IOWaiter[int]:
        return self._waiter(self._proactor.write(fd, data, offset))

    def stat_fdsize(self, fd: int) -> IOWaiter[int]:
        return self._waiter(self._proactor.stat_fdsize(fd))

    def close_fd(self, fd: int) -> IOWaiter[None]:
        return self._waiter(self._proactor.close_fd(fd))

    def poll_many(
        self,
        fd: int,
        mask: int,
        callback: Callable[[int], object],
    ) -> ContinuousOperation[int]:
        return self._proactor.poll_many(fd, mask, callback)

    def _marshal_accept_callback(self, thunk: Callable[[], object]) -> None:
        self._check_open()
        assert self._scheduler is not None
        self._scheduler.call_soon_threadsafe(thunk)

    def accept_many(
        self,
        sock: socket.socket,
        callback: Callable[[AcceptManyDelivery], object],
        *,
        recv_size: int | None = None,
        on_recv_error: AcceptRecvErrorCallback | None = None,
    ) -> ContinuousOperation[socket.socket]:
        """Start ``proactor.accept_many`` with optional accept-time pre-read.

        Deliveries are marshalled onto the scheduler thread before ``callback``
        runs. Recv failures invoke ``on_recv_error(conn, exc)`` when provided;
        the socket is always closed afterwards. With no ``on_recv_error``, recv
        failures close the socket silently.
        """

        normalized_recv_size = normalize_accept_recv_size(recv_size)

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

            self._marshal_accept_callback(run)

        if normalized_recv_size is not None:
            return self._proactor.accept_many(
                sock,
                callback_factory=lambda op: accept_read_delivery(
                    self._proactor,
                    op,
                    deliver_wrapped,
                    recv_size=normalized_recv_size,
                ),
            )

        return self._proactor.accept_many(sock, wrap_accept_delivery(deliver_wrapped))

    def accept_many_streams(
        self,
        sock: socket.socket,
        callback: Callable[[AcceptStreamsDelivery], object],
        *,
        limit: int = 2**16,
        stream_factory: Any | None = None,
        async_: bool = False,
        recv_size: int | None = None,
        on_recv_error: AcceptRecvErrorCallback | None = None,
    ) -> ContinuousOperation[socket.socket]:
        """Start ``proactor.accept_many`` and deliver a stream pair per accept.

        When ``recv_size`` is set, the accept-time read pre-fills the reader
        buffer via ``feed_initial``. Recv failures invoke ``on_recv_error(conn,
        exc)`` when provided (for logging ``getpeername()`` and similar); the
        socket is always closed afterwards. With no ``on_recv_error``, recv
        failures close the socket silently.
        """

        from .streams import _open_streams

        normalized_recv_size = normalize_accept_recv_size(recv_size)

        def deliver_accept(accepted: AcceptReadResult) -> None:
            conn, initial_data, recv_error = accepted

            def run() -> None:
                if recv_error is not None:
                    finalize_accept_recv_error(conn, recv_error, on_recv_error)
                    return

                writer: Any = None
                try:
                    reader, writer = _open_streams(
                        self,
                        conn,
                        limit=limit,
                        stream_factory=stream_factory,
                        async_=async_,
                        initial=initial_data,
                    )
                    callback((reader, writer))
                except BaseException:
                    if writer is not None:
                        writer.close()
                    else:
                        abortive_close(conn)
                    raise

            self._marshal_accept_callback(run)

        if normalized_recv_size is not None:
            return self._proactor.accept_many(
                sock,
                callback_factory=lambda op: accept_read_delivery(
                    self._proactor,
                    op,
                    deliver_accept,
                    recv_size=normalized_recv_size,
                ),
            )

        return self._proactor.accept_many(sock, wrap_accept_delivery(deliver_accept))

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
    ) -> IOWaiter[AcceptStreamsDelivery]:
        """Create a socket, connect, and return stream endpoints.

        ``initial_data`` is sent on the wire after connect, before streams open.
        """

        from .operation_callbacks import create_connect_operation_factory
        from .streams import _open_streams

        # A future create_streams may arm a recv_many so it may itselv be an IOWatier operation.
        # For now, it is a simple non-blocking op so it is wrapped in a Fake IOWaiter for chaining.
        def create_streams(parent: IOWaiterChainableProtocol[socket.socket]) -> IOWaiterProtocol[AcceptStreamsDelivery]:
            sock = parent.value()
            return IOWaiterFake(
                _open_streams(
                    self,
                    sock,
                    limit=limit,
                    stream_factory=stream_factory,
                    async_=async_,
                )
            )

        return self._waiter(
            self._proactor.create_socket(
                family,
                type,
                proto,
                flags=flags,
                operation_factory=create_connect_operation_factory(
                    self._proactor,
                    connect_to,
                    initial_data,
                ),
            ),
            create_next=create_streams,
        )

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

        return self._waiter(operation, map_result=make_file)

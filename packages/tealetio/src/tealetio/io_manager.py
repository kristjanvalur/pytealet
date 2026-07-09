from __future__ import annotations

import os
import socket
from collections.abc import Callable, Iterable, Iterator
from typing import TYPE_CHECKING, Any, Protocol, TypeVar, cast, runtime_checkable

from .files import IOFile, ProactorFile, parse_open_mode
from .locks import ThreadsafeEvent
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
    """Blocking IO facade with access to proactor submission (``accept_many``, …)."""

    @property
    def proactor(self) -> "Proactor": ...


@runtime_checkable
class SocketIO(Protocol):
    """Blocking asyncio-shaped socket helpers over a scheduler IO backend."""

    def sock_recv(self, sock: socket.socket, n: int) -> bytes: ...

    def sock_recv_into(self, sock: socket.socket, buf: Any) -> int: ...

    def sock_recvfrom(self, sock: socket.socket, bufsize: int) -> tuple[bytes, Any]: ...

    def sock_recvfrom_into(self, sock: socket.socket, buf: Any, nbytes: int = 0) -> tuple[int, Any]: ...

    def sock_sendall(
        self,
        sock: socket.socket,
        data: SocketSendBuffer,
        progress: _ProgressCallback | None = None,
    ) -> None: ...

    def sock_send_iter(
        self,
        sock: socket.socket,
        chunks: Iterable[SocketSendBuffer],
    ) -> None: ...

    def sock_sendto(self, sock: socket.socket, data: SocketSendBuffer, address: Any) -> int: ...

    def sock_accept(self, sock: socket.socket) -> socket.socket: ...

    def sock_connect(
        self,
        sock: socket.socket,
        address: Any,
        *,
        initial: SocketSendBuffer | None = None,
    ) -> None: ...

    def sock_create(
        self,
        family: int,
        type: int,
        proto: int = 0,
        *,
        flags: int = 0,
        connect_to: Any | None = None,
        initial_data: SocketSendBuffer | None = None,
    ) -> socket.socket: ...

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

    def create_recv_buffer_pool(self, buffer_size: int, buffer_count: int) -> "RecvBufferPool": ...

    def shared_recv_buffer_pool(self) -> "RecvBufferPool": ...

    def set_shared_recv_buffer_pool(self, pool: "RecvBufferPool") -> None: ...


@runtime_checkable
class PollIO(Protocol):
    """Blocking poll helpers over a scheduler IO backend."""

    def poll(self, fd: int, mask: int) -> int: ...

    def poll_many(
        self,
        fd: int,
        mask: int,
        callback: Callable[[int], object],
    ) -> ContinuousOperation[int]: ...


@runtime_checkable
class FileIO(Protocol):
    """Positioned binary file open helper over a blocking IO backend."""

    def open(self, path: str, mode: str = "rb") -> IOFile: ...


class ServerIO(SocketIO, ProactorAccess, Protocol):
    """Blocking socket IO plus proactor submission for stream servers.

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
        flags: int = 0,
        connect_to: Any | None = None,
        initial_data: SocketSendBuffer | None = None,
        limit: int = 2**16,
        stream_factory: Any | None = None,
        async_: bool = False,
    ) -> AcceptStreamsDelivery: ...


ProactorSocketIO = ServerIO


class ProactorIOManager:
    """Blocking IO facade over a ``Proactor`` backend.

    Structurally implements ``SocketIO``, ``ServerIO``, ``PollIO``, and
    ``FileIO``, plus ``wait_operation`` for blocking on submitted ``Operation``
    objects. Always owned by a proactor scheduler and holds a direct reference
    to that scheduler instead of querying thread-local scheduler state.
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

    def wait_operation(self, operation: Operation[T]) -> T:
        """Block until ``operation`` completes.

        Park the current tealet through ``ThreadsafeEvent`` until the operation
        callback fires. Call only from scheduler-owned tealets.
        """

        if operation.done():
            return operation.result()

        ready = ThreadsafeEvent(self._scheduler)

        def wake(_operation: Operation[Any]) -> None:
            ready.set()

        operation.add_done_callback(wake)
        try:
            ready.swait()
        finally:
            if not operation.done():
                operation.remove_done_callback(wake)
                operation.cancel()
        return operation.result()

    def sock_recv(self, sock: socket.socket, n: int) -> bytes:
        return self.wait_operation(self._proactor.recv(sock, n))

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

        return b"".join((process(chunk) for _, chunk in self.sock_recv_iter(sock, buffer_pool)))

    def sock_recv_into(self, sock: socket.socket, buf: Any) -> int:
        return self.wait_operation(self._proactor.recv_into(sock, buf))

    def sock_recvfrom(self, sock: socket.socket, bufsize: int) -> tuple[bytes, Any]:
        return self.wait_operation(self._proactor.recvfrom(sock, bufsize))

    def sock_recvfrom_into(self, sock: socket.socket, buf: Any, nbytes: int = 0) -> tuple[int, Any]:
        return self.wait_operation(self._proactor.recvfrom_into(sock, buf, nbytes))

    def sock_sendall(self, sock: socket.socket, data: Any, progress: _ProgressCallback | None = None) -> None:
        return self.wait_operation(self._proactor.send(sock, data, progress))

    def sock_send_iter(
        self,
        sock: socket.socket,
        chunks: Iterable[bytes | bytearray | memoryview],
    ) -> None:
        for chunk in chunks:
            if not chunk:
                continue
            self.sock_sendall(sock, memoryview(chunk))

    def sock_sendto(self, sock: socket.socket, data: Any, address: Any) -> int:
        return self.wait_operation(self._proactor.sendto(sock, data, address))

    def sock_accept(self, sock: socket.socket) -> socket.socket:
        return self.wait_operation(self._proactor.accept(sock))

    def sock_connect(
        self,
        sock: socket.socket,
        address: Any,
        *,
        initial: SocketSendBuffer | None = None,
    ) -> None:
        if initial is None:
            self.wait_operation(self._proactor.connect(sock, address))
            return
        from .operation_callbacks import connect_initial_send_operation_factory

        self.wait_operation(
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
    ) -> socket.socket:
        if initial_data is not None and connect_to is None:
            raise ValueError("initial_data requires connect_to")
        if connect_to is None:
            return self.wait_operation(
                self._proactor.create_socket(
                    family,
                    type,
                    proto,
                    flags=flags,
                )
            )

        from .operation_callbacks import create_connect_operation_factory

        operation = self._proactor.create_socket(
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
        return self.wait_operation(operation)

    def poll(self, fd: int, mask: int) -> int:
        return self.wait_operation(self._proactor.poll(fd, mask))

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
        flags: int = 0,
        connect_to: Any | None = None,
        initial_data: SocketSendBuffer | None = None,
        limit: int = 2**16,
        stream_factory: Any | None = None,
        async_: bool = False,
    ) -> AcceptStreamsDelivery:
        """Create a socket and return stream endpoints.

        When ``connect_to`` is set, the connect chain runs first and
        ``initial_data`` is sent on the wire before streams are returned.
        """

        from .streams import _open_streams

        if initial_data is not None and connect_to is None:
            raise ValueError("initial_data requires connect_to")

        def open_streams(sock: socket.socket) -> AcceptStreamsDelivery:
            return _open_streams(
                self,
                sock,
                limit=limit,
                stream_factory=stream_factory,
                async_=async_,
            )

        if connect_to is None:
            sock = self.wait_operation(
                self._proactor.create_socket(
                    family,
                    type,
                    proto,
                    flags=flags,
                )
            )
            return open_streams(sock)

        from .operation_callbacks import create_connect_operation_factory

        operation = self._proactor.create_socket(
            family,
            type,
            proto,
            flags=flags,
            operation_factory=create_connect_operation_factory(
                self._proactor,
                connect_to,
                initial_data,
                open_streams,
            ),
        )
        return self.wait_operation(cast(Operation[AcceptStreamsDelivery], operation))

    def open(self, path: str, mode: str = "rb") -> IOFile:
        flags, file_mode = parse_open_mode(mode)
        try:
            fd = self.wait_operation(self._proactor.openat(path, flags, file_mode))
        except NotImplementedError as exc:
            raise NotImplementedError("file I/O requires a proactor with openat support") from exc
        try:
            return ProactorFile(
                self,
                self._proactor,
                fd,
                path=path,
                flags=flags,
                append="a" in mode,
            )
        except BaseException:
            try:
                os.close(fd)
            except OSError:
                pass
            raise

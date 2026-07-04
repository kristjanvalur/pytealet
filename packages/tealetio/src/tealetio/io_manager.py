from __future__ import annotations

import os
import socket
from collections.abc import Callable, Iterable, Iterator
from typing import TYPE_CHECKING, Any, Protocol, TypeVar, runtime_checkable

from .files import IOFile, ProactorFile, parse_open_mode
from .locks import ThreadsafeEvent
from .operations import ContinuousOperation, Operation
from .socket_helpers import configure_scheduler_socket

if TYPE_CHECKING:
    from .proactor import Proactor, RecvBufferPool

T = TypeVar("T")

_ProgressCallback = Callable[[int], object]
_RecvProgressCallback = Callable[[bytes], object]
_RecvIterYield = tuple[int, memoryview]

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

    def sock_sendall(self, sock: socket.socket, data: Any, progress: _ProgressCallback | None = None) -> None: ...

    def sock_send_iter(
        self,
        sock: socket.socket,
        chunks: Iterable[bytes | bytearray | memoryview],
    ) -> None: ...

    def sock_sendto(self, sock: socket.socket, data: Any, address: Any) -> int: ...

    def sock_accept(self, sock: socket.socket) -> tuple[socket.socket, Any]: ...

    def sock_connect(self, sock: socket.socket, address: Any) -> None: ...

    def sock_create(
        self,
        family: int,
        type: int,
        proto: int = 0,
        *,
        flags: int = 0,
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
    """Blocking socket IO plus proactor submission for stream servers."""


ProactorSocketIO = ServerIO


class ProactorIOManager:
    """Blocking IO facade over a ``Proactor`` backend.

    Implements ``SocketIO``, ``PollIO``, and ``FileIO``, plus
    ``wait_operation`` for blocking on submitted ``Operation`` objects. The
    scheduler keeps the driver; this object only blocks the current tealet on
    submitted operations.
    """

    def __init__(self, proactor: Proactor) -> None:
        self._proactor = proactor

    @property
    def proactor(self) -> Proactor:
        return self._proactor

    def wait_operation(self, operation: Operation[T]) -> T:
        """Block the current tealet until ``operation`` completes."""

        if operation.done():
            return operation.result()

        ready = ThreadsafeEvent()

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
        return self.wait_operation(self._proactor.sendall(sock, data, progress))

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

    def sock_accept(self, sock: socket.socket) -> tuple[socket.socket, Any]:
        return self.wait_operation(self._proactor.accept(sock))

    def sock_connect(self, sock: socket.socket, address: Any) -> None:
        return self.wait_operation(self._proactor.connect(sock, address))

    def sock_create(
        self,
        family: int,
        type: int,
        proto: int = 0,
        *,
        flags: int = 0,
    ) -> socket.socket:
        del flags
        return configure_scheduler_socket(socket.socket(family, type, proto))

    def poll(self, fd: int, mask: int) -> int:
        return self.wait_operation(self._proactor.poll(fd, mask))

    def poll_many(
        self,
        fd: int,
        mask: int,
        callback: Callable[[int], object],
    ) -> ContinuousOperation[int]:
        return self._proactor.poll_many(fd, mask, callback)

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

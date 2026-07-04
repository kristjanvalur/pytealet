from __future__ import annotations

import os
import socket
from collections.abc import Callable, Iterable, Iterator
from typing import TYPE_CHECKING, Any, TypeVar

from .files import ProactorFile, parse_open_mode
from .locks import ThreadsafeEvent
from .operations import ContinuousOperation, Operation

if TYPE_CHECKING:
    from .proactor import Proactor, RecvBufferPool

T = TypeVar("T")

_ProgressCallback = Callable[[int], object]
_RecvProgressCallback = Callable[[bytes], object]
_RecvIterYield = tuple[int, memoryview]

__all__ = ["ProactorIOManager"]


def _configure_scheduler_socket(sock: socket.socket) -> socket.socket:
    """Apply the scheduler socket contract: non-blocking and close-on-exec."""

    sock.setblocking(False)
    os.set_inheritable(sock.fileno(), False)
    return sock


class ProactorIOManager:
    """Blocking IO facade over a ``Proactor`` backend.

    Owns ``wait_operation`` and the asyncio-shaped ``sock_*``, ``poll*``, and
    positioned file helpers. The scheduler keeps the driver; this object only
    blocks the current tealet on submitted operations.
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
        from .proactor import _RecvIterBuffer

        pool = self._resolve_recv_buffer_pool(buffer_pool)
        buffer = _RecvIterBuffer(buf_group=pool)
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
        return _configure_scheduler_socket(socket.socket(family, type, proto))

    def poll(self, fd: int, mask: int) -> int:
        return self.wait_operation(self._proactor.poll(fd, mask))

    def poll_many(
        self,
        fd: int,
        mask: int,
        callback: Callable[[int], object],
    ) -> ContinuousOperation[int]:
        return self._proactor.poll_many(fd, mask, callback)

    def open(self, path: str, mode: str = "rb") -> ProactorFile:
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
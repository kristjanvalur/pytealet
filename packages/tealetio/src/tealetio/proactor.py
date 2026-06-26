from __future__ import annotations

import errno
import selectors
import socket
from collections.abc import Callable
from concurrent.futures import CancelledError
from dataclasses import dataclass
from typing import Any, Generic, Protocol, TypeVar, cast

from .locks import Event
from .scheduler import BaseScheduler, RunnableQueueFactory, Scheduler

T = TypeVar("T")

__all__ = [
    "Operation",
    "Proactor",
    "ProactorFactory",
    "ProactorScheduler",
    "SelectorProactor",
]


class InvalidStateError(Exception):
    """Raised when an operation result is requested before completion."""


_DoneCallback = Callable[["Operation[Any]"], object]
_CancelCallback = Callable[["Operation[Any]"], bool]
_WakeupCallback = Callable[[], object]


class Proactor(Protocol):
    """Minimal completion-oriented IO backend used by `ProactorScheduler`."""

    def close(self) -> None: ...

    def break_wait(self) -> None: ...

    def has_pending_operations(self) -> bool: ...

    def wait(self, timeout: float | None = None) -> list[Operation[Any]]: ...

    def recv(self, sock: socket.socket, n: int) -> Operation[bytes]: ...

    def recv_into(self, sock: socket.socket, buf: Any) -> Operation[int]: ...

    def recvfrom(self, sock: socket.socket, bufsize: int) -> Operation[tuple[bytes, Any]]: ...

    def recvfrom_into(self, sock: socket.socket, buf: Any, nbytes: int = 0) -> Operation[tuple[int, Any]]: ...

    def sendall(self, sock: socket.socket, data: Any) -> Operation[None]: ...

    def sendto(self, sock: socket.socket, data: Any, address: Any) -> Operation[int]: ...

    def accept(self, sock: socket.socket) -> Operation[tuple[socket.socket, Any]]: ...

    def connect(self, sock: socket.socket, address: Any) -> Operation[None]: ...


ProactorFactory = Callable[[], Proactor]


class Operation(Generic[T]):
    """Future-shaped IO operation owned by a proactor backend."""

    def __init__(self, *, kind: str, fileobj: object | None = None) -> None:
        self.kind = kind
        self.fileobj = fileobj
        self._done = False
        self._cancelled = False
        self._result: T | None = None
        self._exception: BaseException | None = None
        self._callbacks: list[_DoneCallback] = []
        self._cancel_callback: _CancelCallback | None = None
        self._attempt: Callable[[], T] | None = None

    def done(self) -> bool:
        """Return True if the operation has completed."""

        return self._done

    def cancelled(self) -> bool:
        """Return True if the operation completed by cancellation."""

        return self._cancelled

    def cancel(self) -> bool:
        """Cancel the operation if it has not completed yet."""

        if self._done:
            return False
        if self._cancel_callback is not None:
            return self._cancel_callback(self)
        self._set_cancelled()
        return True

    def result(self) -> T:
        """Return the operation result, or raise its completion exception."""

        if not self._done:
            raise InvalidStateError("operation result is not ready")
        if self._exception is not None:
            raise self._exception
        return cast(T, self._result)

    def exception(self) -> BaseException | None:
        """Return the operation exception, or None for successful completion."""

        if not self._done:
            raise InvalidStateError("operation exception is not ready")
        return self._exception

    def add_done_callback(self, callback: _DoneCallback) -> None:
        """Register `callback` to run when the operation completes."""

        if self._done:
            callback(self)
            return
        self._callbacks.append(callback)

    def remove_done_callback(self, callback: _DoneCallback) -> int:
        """Remove matching done callbacks and return the number removed."""

        removed = 0
        kept: list[_DoneCallback] = []
        for stored_callback in self._callbacks:
            if stored_callback is callback:
                removed += 1
            else:
                kept.append(stored_callback)
        self._callbacks = kept
        return removed

    def _set_cancel_callback(self, callback: _CancelCallback) -> None:
        self._cancel_callback = callback

    def _set_result(self, result: T) -> None:
        if self._done:
            raise InvalidStateError("operation already done")
        self._result = result
        self._done = True
        self._run_done_callbacks()

    def _set_exception(self, exc: BaseException) -> None:
        if self._done:
            raise InvalidStateError("operation already done")
        self._exception = exc
        self._done = True
        self._run_done_callbacks()

    def _set_cancelled(self) -> None:
        self._cancelled = True
        self._set_exception(CancelledError())

    def _run_done_callbacks(self) -> None:
        callbacks = self._callbacks[:]
        self._callbacks.clear()
        for callback in callbacks:
            callback(self)


@dataclass
class _FdEntry:
    reader: Operation[Any] | None = None
    writer: Operation[Any] | None = None

    def empty(self) -> bool:
        return self.reader is None and self.writer is None


class SelectorProactor:
    """Completion-oriented proactor prototype backed by a selector."""

    def __init__(
        self,
        selector: selectors.BaseSelector | None = None,
        *,
        wakeup_callback: _WakeupCallback | None = None,
    ) -> None:
        self._selector = selector if selector is not None else selectors.DefaultSelector()
        self._fd_operations: dict[int, _FdEntry] = {}
        self._closed = False
        self._wakeup_callback = wakeup_callback
        self._wakeup_reader, self._wakeup_writer = socket.socketpair()
        self._wakeup_reader.setblocking(False)
        self._wakeup_writer.setblocking(False)
        self._selector.register(self._wakeup_reader.fileno(), selectors.EVENT_READ, None)

    def set_wakeup_callback(self, callback: _WakeupCallback | None) -> None:
        """Set the callback used to notify an owner that the proactor needs pumping."""

        self._wakeup_callback = callback

    def has_pending_operations(self) -> bool:
        """Return True if operations are waiting for backend completion."""

        return bool(self._fd_operations)

    def close(self) -> None:
        """Close selector and wakeup resources."""

        if self._closed:
            return
        self._closed = True
        self._selector.close()
        self._wakeup_reader.close()
        self._wakeup_writer.close()

    def break_wait(self) -> None:
        """Interrupt a thread blocked in `wait` without completing operations."""

        try:
            self._wakeup_writer.send(b"\0")
        except (BlockingIOError, OSError):
            pass

    def poll(self) -> list[Operation[Any]]:
        """Poll ready operations without blocking."""

        return self.wait(0.0)

    def wait(self, timeout: float | None = None) -> list[Operation[Any]]:
        """Wait for ready operations and return the operations completed."""

        self._check_open()
        events = self._selector.select(timeout)
        completed: list[Operation[Any]] = []
        wakeup_fd = self._wakeup_reader.fileno()
        for key, mask in events:
            fd = key.fd
            if fd == wakeup_fd:
                self._drain_wakeup()
                continue
            if mask & selectors.EVENT_READ:
                self._step_fd_operation(fd, selectors.EVENT_READ, completed)
            if mask & selectors.EVENT_WRITE:
                self._step_fd_operation(fd, selectors.EVENT_WRITE, completed)
        if completed:
            self._notify_wakeup()
        return completed

    def recv(self, sock: socket.socket, n: int) -> Operation[bytes]:
        """Submit a socket receive operation."""

        operation = Operation[bytes](kind="recv", fileobj=sock)

        def attempt() -> bytes:
            return sock.recv(n)

        self._submit_socket_operation(sock, selectors.EVENT_READ, operation, attempt)
        return operation

    def recv_into(self, sock: socket.socket, buf: Any) -> Operation[int]:
        """Submit a socket receive-into operation."""

        operation = Operation[int](kind="recv_into", fileobj=sock)

        def attempt() -> int:
            return sock.recv_into(buf)

        self._submit_socket_operation(sock, selectors.EVENT_READ, operation, attempt)
        return operation

    def recvfrom(self, sock: socket.socket, bufsize: int) -> Operation[tuple[bytes, Any]]:
        """Submit a datagram receive operation."""

        operation = Operation[tuple[bytes, Any]](kind="recvfrom", fileobj=sock)

        def attempt() -> tuple[bytes, Any]:
            return sock.recvfrom(bufsize)

        self._submit_socket_operation(sock, selectors.EVENT_READ, operation, attempt)
        return operation

    def recvfrom_into(self, sock: socket.socket, buf: Any, nbytes: int = 0) -> Operation[tuple[int, Any]]:
        """Submit a datagram receive-into operation."""

        operation = Operation[tuple[int, Any]](kind="recvfrom_into", fileobj=sock)

        def attempt() -> tuple[int, Any]:
            if nbytes:
                return sock.recvfrom_into(buf, nbytes)
            return sock.recvfrom_into(buf)

        self._submit_socket_operation(sock, selectors.EVENT_READ, operation, attempt)
        return operation

    def send(self, sock: socket.socket, data: Any) -> Operation[int]:
        """Submit a socket send operation."""

        operation = Operation[int](kind="send", fileobj=sock)

        def attempt() -> int:
            return sock.send(data)

        self._submit_socket_operation(sock, selectors.EVENT_WRITE, operation, attempt)
        return operation

    def sendto(self, sock: socket.socket, data: Any, address: Any) -> Operation[int]:
        """Submit a datagram send operation."""

        operation = Operation[int](kind="sendto", fileobj=sock)

        def attempt() -> int:
            return sock.sendto(data, address)

        self._submit_socket_operation(sock, selectors.EVENT_WRITE, operation, attempt)
        return operation

    def sendall(self, sock: socket.socket, data: Any) -> Operation[None]:
        """Submit a socket send-all operation."""

        operation = Operation[None](kind="sendall", fileobj=sock)
        view = memoryview(data)
        offset = 0

        def attempt() -> None:
            nonlocal offset
            while offset < len(view):
                sent = sock.send(view[offset:])
                if sent == 0:
                    raise BlockingIOError(errno.EWOULDBLOCK, "socket send returned zero bytes")
                offset += sent
            return None

        self._submit_socket_operation(sock, selectors.EVENT_WRITE, operation, attempt)
        return operation

    def accept(self, sock: socket.socket) -> Operation[tuple[socket.socket, Any]]:
        """Submit a socket accept operation."""

        operation = Operation[tuple[socket.socket, Any]](kind="accept", fileobj=sock)

        def attempt() -> tuple[socket.socket, Any]:
            conn, address = sock.accept()
            conn.setblocking(False)
            return conn, address

        self._submit_socket_operation(sock, selectors.EVENT_READ, operation, attempt)
        return operation

    def connect(self, sock: socket.socket, address: Any) -> Operation[None]:
        """Submit a non-blocking socket connect operation."""

        operation = Operation[None](kind="connect", fileobj=sock)
        started = False

        def attempt() -> None:
            nonlocal started
            if not started:
                started = True
                try:
                    sock.connect(address)
                    return None
                except (BlockingIOError, InterruptedError):
                    raise BlockingIOError(errno.EINPROGRESS, "connect in progress") from None
                except OSError as exc:
                    if exc.errno in (errno.EINPROGRESS, errno.EWOULDBLOCK, errno.EALREADY):
                        raise BlockingIOError(exc.errno, exc.strerror) from None
                    raise
            err = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            if err == 0:
                return None
            if err in (errno.EINPROGRESS, errno.EWOULDBLOCK, errno.EALREADY):
                raise BlockingIOError(err, errno.errorcode.get(err, "connect in progress"))
            raise OSError(err, errno.errorcode.get(err, "socket connect failed"))

        self._submit_socket_operation(sock, selectors.EVENT_WRITE, operation, attempt)
        return operation

    def _submit_socket_operation(
        self,
        sock: socket.socket,
        event: int,
        operation: Operation[T],
        attempt: Callable[[], T],
    ) -> None:
        self._check_open()
        self._check_socket(sock)
        fd = sock.fileno()
        self._reserve_fd_operation(fd, event, operation)
        operation._attempt = attempt
        operation._set_cancel_callback(self._cancel_operation)
        self._update_selector_registration(fd)
        self.break_wait()
        self._step_fd_operation(fd, event, [])

    def _reserve_fd_operation(self, fd: int, event: int, operation: Operation[Any]) -> None:
        entry = self._fd_operations.setdefault(fd, _FdEntry())
        current = entry.reader if event == selectors.EVENT_READ else entry.writer
        if current is not None:
            raise RuntimeError("an operation is already pending for this fd and direction")
        if event == selectors.EVENT_READ:
            entry.reader = operation
        else:
            entry.writer = operation

    def _cancel_operation(self, operation: Operation[Any]) -> bool:
        removed = self._remove_operation(operation)
        if not removed:
            return False
        operation._set_cancelled()
        self.break_wait()
        return True

    def _remove_operation(self, operation: Operation[Any]) -> bool:
        for fd, entry in list(self._fd_operations.items()):
            removed = False
            if entry.reader is operation:
                entry.reader = None
                removed = True
            if entry.writer is operation:
                entry.writer = None
                removed = True
            if removed:
                if entry.empty():
                    del self._fd_operations[fd]
                self._update_selector_registration(fd)
                return True
        return False

    def _step_fd_operation(self, fd: int, event: int, completed: list[Operation[Any]]) -> None:
        entry = self._fd_operations.get(fd)
        if entry is None:
            return
        operation = entry.reader if event == selectors.EVENT_READ else entry.writer
        if operation is None or operation.done():
            return
        attempt = cast(Callable[[], Any], operation._attempt)
        assert attempt is not None
        try:
            result = attempt()
        except (BlockingIOError, InterruptedError):
            self._update_selector_registration(fd)
            return
        except BaseException as exc:
            self._remove_operation(operation)
            operation._set_exception(exc)
        else:
            self._remove_operation(operation)
            operation._set_result(result)
        completed.append(operation)

    def _selector_mask_for_fd(self, fd: int) -> int:
        entry = self._fd_operations.get(fd)
        if entry is None:
            return 0
        mask = 0
        if entry.reader is not None:
            mask |= selectors.EVENT_READ
        if entry.writer is not None:
            mask |= selectors.EVENT_WRITE
        return mask

    def _update_selector_registration(self, fd: int) -> None:
        if self._closed:
            return
        mask = self._selector_mask_for_fd(fd)
        try:
            self._selector.get_key(fd)
        except KeyError:
            if mask:
                self._selector.register(fd, mask, fd)
            return
        if mask:
            self._selector.modify(fd, mask, fd)
            return
        try:
            self._selector.unregister(fd)
        except (KeyError, ValueError, OSError):
            pass

    def _drain_wakeup(self) -> None:
        while True:
            try:
                if not self._wakeup_reader.recv(4096):
                    return
            except BlockingIOError:
                return
            except OSError:
                return

    def _notify_wakeup(self) -> None:
        callback = self._wakeup_callback
        if callback is not None:
            callback()

    def _check_open(self) -> None:
        if self._closed:
            raise RuntimeError("proactor is closed")

    def _check_socket(self, sock: socket.socket) -> None:
        if sock.getblocking():
            raise ValueError("socket must be non-blocking")
        if sock.fileno() < 0:
            raise ValueError("socket is closed")


class ProactorScheduler(Scheduler):
    """Synchronous scheduler whose IO wait point is a proactor backend."""

    def __init__(
        self,
        proactor_factory: ProactorFactory | None = None,
        *,
        runnable_queue_factory: RunnableQueueFactory | None = None,
    ) -> None:
        super().__init__(runnable_queue_factory=runnable_queue_factory)
        if proactor_factory is None:
            proactor_factory = SelectorProactor
        self._proactor = proactor_factory()

    @property
    def proactor(self) -> Proactor:
        """Return the proactor backend owned by this scheduler."""

        return self._proactor

    def close(self) -> None:
        """Close proactor and scheduler-owned resources."""

        self._proactor.close()
        BaseScheduler.close(self)

    # -- Driver wakeup -------------------------------------------------

    def _break_wait_threadsafe(self) -> None:
        self._proactor.break_wait()

    def _break_wait(self) -> None:
        self._proactor.break_wait()

    def _wait_thread(self) -> None:
        deadline = self._next_timer_deadline()
        timeout = None if deadline is None else self._delay_until(deadline)
        self._proactor.wait(timeout)

    # -- Operation waits ----------------------------------------------

    def wait_operation(self, operation: Operation[T]) -> T:
        """Block the current tealet until `operation` completes."""

        if operation.done():
            return operation.result()

        ready = Event()
        active = True

        def wake(_operation: Operation[Any]) -> None:
            nonlocal active
            if not active:
                return
            active = False
            ready.set()

        operation.add_done_callback(wake)
        try:
            ready.swait()
        finally:
            if active:
                active = False
                operation.remove_done_callback(wake)
                operation.cancel()
        return operation.result()

    # -- Asyncio-style socket helpers ---------------------------------

    def sock_recv(self, sock: socket.socket, n: int) -> bytes:
        """Receive up to `n` bytes from a non-blocking socket."""

        return self.wait_operation(self._proactor.recv(sock, n))

    def sock_recv_into(self, sock: socket.socket, buf: Any) -> int:
        """Receive bytes from a non-blocking socket into `buf`."""

        return self.wait_operation(self._proactor.recv_into(sock, buf))

    def sock_recvfrom(self, sock: socket.socket, bufsize: int) -> tuple[bytes, Any]:
        """Receive datagram bytes and address from a non-blocking socket."""

        return self.wait_operation(self._proactor.recvfrom(sock, bufsize))

    def sock_recvfrom_into(self, sock: socket.socket, buf: Any, nbytes: int = 0) -> tuple[int, Any]:
        """Receive datagram bytes into `buf` from a non-blocking socket."""

        return self.wait_operation(self._proactor.recvfrom_into(sock, buf, nbytes))

    def sock_sendall(self, sock: socket.socket, data: Any) -> None:
        """Send all `data` through a non-blocking socket."""

        return self.wait_operation(self._proactor.sendall(sock, data))

    def sock_sendto(self, sock: socket.socket, data: Any, address: Any) -> int:
        """Send one datagram through a non-blocking socket."""

        return self.wait_operation(self._proactor.sendto(sock, data, address))

    def sock_accept(self, sock: socket.socket) -> tuple[socket.socket, Any]:
        """Accept one connection from a non-blocking listening socket."""

        return self.wait_operation(self._proactor.accept(sock))

    def sock_connect(self, sock: socket.socket, address: Any) -> None:
        """Connect a non-blocking socket to `address`."""

        return self.wait_operation(self._proactor.connect(sock, address))

    def _has_pending_driver_work(self) -> bool:
        return self._proactor.has_pending_operations() or BaseScheduler._has_pending_driver_work(self)

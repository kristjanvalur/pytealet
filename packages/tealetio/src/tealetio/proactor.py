from __future__ import annotations

import asyncio as _asyncio
import errno
import selectors
import socket
import threading
import time
from collections import deque
from collections.abc import Callable
from concurrent.futures import CancelledError
from dataclasses import dataclass
from typing import Any, Generic, Protocol, TypeVar, cast

from .locks import Event
from .scheduler import (
    AsyncDrivingMixin,
    AsyncSchedulerDrivingAPI,
    BaseScheduler,
    RunnableQueueFactory,
    SyncDrivingMixin,
    SyncSchedulerDrivingAPI,
)

T = TypeVar("T")

__all__ = [
    "Operation",
    "AsyncProactorScheduler",
    "Proactor",
    "ProactorFactory",
    "ProactorScheduler",
    "SelectorProactor",
    "SyncProactorScheduler",
    "ThreadedSelectorProactor",
]


class InvalidStateError(Exception):
    """Raised when an operation result is requested before completion."""


_DoneCallback = Callable[["Operation[Any]"], object]
_CancelCallback = Callable[["Operation[Any]"], bool]
_CompletionCallback = Callable[[], object]
_Clock = Callable[[], float]


class Proactor(Protocol):
    """Minimal completion-oriented IO backend used by `ProactorScheduler`."""

    def close(self) -> None: ...

    def break_wait(self) -> None: ...

    def set_completion_callback(self, callback: _CompletionCallback | None) -> None: ...

    def get_time(self) -> float: ...

    def set_clock(self, clock: _Clock) -> None: ...

    def has_pending_operations(self) -> bool: ...

    def wait(self, deadline: float | None = None) -> list[Operation[Any]]: ...

    async def wait_async(self, deadline: float | None = None) -> list[Operation[Any]]: ...

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
        self._lock = threading.RLock()
        self._done = False
        self._cancelled = False
        self._result: T | None = None
        self._exception: BaseException | None = None
        self._callbacks: list[_DoneCallback] = []
        self._cancel_callback: _CancelCallback | None = None
        self._attempt: Callable[[], T] | None = None

    def done(self) -> bool:
        """Return True if the operation has completed."""

        with self._lock:
            return self._done

    def cancelled(self) -> bool:
        """Return True if the operation completed by cancellation."""

        with self._lock:
            return self._cancelled

    def cancel(self) -> bool:
        """Cancel the operation if it has not completed yet."""

        with self._lock:
            if self._done:
                return False
            cancel_callback = self._cancel_callback
        if cancel_callback is not None:
            return cancel_callback(self)
        return self._set_cancelled(raise_if_done=False)

    def result(self) -> T:
        """Return the operation result, or raise its completion exception."""

        with self._lock:
            if not self._done:
                raise InvalidStateError("operation result is not ready")
            exception = self._exception
            result = self._result
        if exception is not None:
            raise exception
        return cast(T, result)

    def exception(self) -> BaseException | None:
        """Return the operation exception, or None for successful completion."""

        with self._lock:
            if not self._done:
                raise InvalidStateError("operation exception is not ready")
            return self._exception

    def add_done_callback(self, callback: _DoneCallback) -> None:
        """Register `callback` to run when the operation completes."""

        with self._lock:
            if self._done:
                run_now = True
            else:
                self._callbacks.append(callback)
                run_now = False
        if run_now:
            callback(self)

    def remove_done_callback(self, callback: _DoneCallback) -> int:
        """Remove matching done callbacks and return the number removed."""

        with self._lock:
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
        with self._lock:
            self._cancel_callback = callback

    def _set_result(self, result: T) -> None:
        self._finish(result=result)

    def _set_exception(self, exc: BaseException) -> None:
        self._finish(exception=exc)

    def _set_cancelled(self, *, raise_if_done: bool = True) -> bool:
        return self._finish(exception=CancelledError(), cancelled=True, raise_if_done=raise_if_done)

    def _finish(
        self,
        *,
        result: T | None = None,
        exception: BaseException | None = None,
        cancelled: bool = False,
        raise_if_done: bool = True,
    ) -> bool:
        with self._lock:
            if self._done:
                if raise_if_done:
                    raise InvalidStateError("operation already done")
                return False
            self._result = result
            self._exception = exception
            self._cancelled = cancelled
            self._done = True
            callbacks = self._callbacks[:]
            self._callbacks.clear()
        self._run_done_callbacks(callbacks)
        return True

    def _run_done_callbacks(self, callbacks: list[_DoneCallback]) -> None:
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
        completion_callback: _CompletionCallback | None = None,
        wakeup_callback: _CompletionCallback | None = None,
    ) -> None:
        if completion_callback is not None and wakeup_callback is not None:
            raise TypeError("use either completion_callback or wakeup_callback, not both")
        self._lock = threading.RLock()
        self._selector = selector if selector is not None else selectors.DefaultSelector()
        self._fd_operations: dict[int, _FdEntry] = {}
        self._closed = False
        self._completion_callback = completion_callback if completion_callback is not None else wakeup_callback
        self._clock = time.monotonic
        self._wakeup_reader, self._wakeup_writer = socket.socketpair()
        self._wakeup_reader.setblocking(False)
        self._wakeup_writer.setblocking(False)
        self._selector.register(self._wakeup_reader.fileno(), selectors.EVENT_READ, None)

    def set_completion_callback(self, callback: _CompletionCallback | None) -> None:
        """Set the callback invoked when backend completions may be ready."""

        with self._lock:
            self._completion_callback = callback

    def set_wakeup_callback(self, callback: _CompletionCallback | None) -> None:
        """Set the callback invoked when backend completions may be ready."""

        self.set_completion_callback(callback)

    def has_pending_operations(self) -> bool:
        """Return True if operations are waiting for backend completion."""

        with self._lock:
            return bool(self._fd_operations)

    def get_time(self) -> float:
        """Return the proactor clock value."""

        return self._clock()

    def set_clock(self, clock: _Clock) -> None:
        """Set the clock used for deadline-oriented waits."""

        with self._lock:
            self._clock = clock

    def close(self) -> None:
        """Close selector and wakeup resources."""

        self.break_wait()
        with self._lock:
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

    def wait(self, deadline: float | None = None) -> list[Operation[Any]]:
        """Wait until `deadline` for ready operations and return those completed."""

        with self._lock:
            self._check_open()
            completed = self._poll(deadline)
        if completed:
            self._notify_completion()
        return completed

    def _poll(self, deadline: float | None = None) -> list[Operation[Any]]:
        timeout = self._timeout_until_deadline(deadline)
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
        return completed

    async def wait_async(self, deadline: float | None = None) -> list[Operation[Any]]:
        """Wait asynchronously until `deadline` and return completed operations."""

        self._check_open()
        completed = self.wait(0)
        if completed or deadline == 0:
            return completed

        timeout = self._timeout_until_deadline(deadline)
        if timeout == 0:
            return []

        loop = _asyncio.get_running_loop()
        ready = loop.create_future()

        def wake() -> None:
            if not ready.done():
                ready.set_result(None)

        registered: list[tuple[int, int]] = []
        wakeup_fd = self._wakeup_reader.fileno()

        def add_reader(fd: int) -> None:
            loop.add_reader(fd, wake)
            registered.append((fd, selectors.EVENT_READ))

        def add_writer(fd: int) -> None:
            loop.add_writer(fd, wake)
            registered.append((fd, selectors.EVENT_WRITE))

        try:
            add_reader(wakeup_fd)
            with self._lock:
                registered_fds = list(self._fd_operations)
            for fd in registered_fds:
                with self._lock:
                    mask = self._selector_mask_for_fd(fd)
                if mask & selectors.EVENT_READ:
                    add_reader(fd)
                if mask & selectors.EVENT_WRITE:
                    add_writer(fd)
            if timeout is None:
                await ready
            else:
                await _asyncio.wait_for(ready, timeout)
        except _asyncio.TimeoutError:
            return []
        finally:
            for fd, event in registered:
                if event == selectors.EVENT_READ:
                    loop.remove_reader(fd)
                else:
                    loop.remove_writer(fd)
        return self.wait(0)

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
        with self._lock:
            self._check_open()
            self._check_socket(sock)
            fd = sock.fileno()
            self._check_fd_operation_available(fd, event)
            if self._try_complete_operation(operation, attempt):
                return
            self._reserve_fd_operation(fd, event, operation)
            operation._attempt = attempt
            operation._set_cancel_callback(self._cancel_operation)
            self._update_selector_registration(fd)
        self.break_wait()

    def _try_complete_operation(self, operation: Operation[T], attempt: Callable[[], T]) -> bool:
        try:
            result = attempt()
        except (BlockingIOError, InterruptedError):
            return False
        except BaseException as exc:
            operation._set_exception(exc)
        else:
            operation._set_result(result)
        return True

    def _check_fd_operation_available(self, fd: int, event: int) -> None:
        entry = self._fd_operations.get(fd)
        if entry is None:
            return
        current = entry.reader if event == selectors.EVENT_READ else entry.writer
        if current is not None:
            raise RuntimeError("an operation is already pending for this fd and direction")

    def _reserve_fd_operation(self, fd: int, event: int, operation: Operation[Any]) -> None:
        self._check_fd_operation_available(fd, event)
        entry = self._fd_operations.setdefault(fd, _FdEntry())
        if event == selectors.EVENT_READ:
            entry.reader = operation
        else:
            entry.writer = operation

    def _cancel_operation(self, operation: Operation[Any]) -> bool:
        with self._lock:
            removed = self._remove_operation(operation)
        if not removed:
            return False
        cancelled = operation._set_cancelled(raise_if_done=False)
        self.break_wait()
        return cancelled

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

    def _timeout_until_deadline(self, deadline: float | None) -> float | None:
        if deadline is None:
            return None
        if deadline == 0:
            return 0.0
        return max(0.0, deadline - self.get_time())

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

    def _notify_completion(self) -> None:
        with self._lock:
            callback = self._completion_callback
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


class ThreadedSelectorProactor(SelectorProactor):
    """Selector proactor that polls readiness from a worker thread."""

    def __init__(
        self,
        selector: selectors.BaseSelector | None = None,
        *,
        completion_callback: _CompletionCallback | None = None,
        wakeup_callback: _CompletionCallback | None = None,
    ) -> None:
        super().__init__(selector, completion_callback=completion_callback, wakeup_callback=wakeup_callback)
        self._completed: deque[Operation[Any]] = deque()
        self._completed_lock = threading.Lock()
        self._completed_ready = threading.Event()
        self._worker_started = False
        self._worker_stop = threading.Event()
        self._mutation_condition = threading.Condition()
        self._mutation_requested = False
        self._worker = threading.Thread(target=self._worker_main, name="tealetio-selector-proactor", daemon=True)

    def close(self) -> None:
        """Stop the worker thread and close selector resources."""

        self._worker_stop.set()
        self._completed_ready.set()
        self._begin_worker_mutation()
        try:
            if self._closed:
                return
        finally:
            self._end_worker_mutation()
        if self._worker_started and threading.current_thread() is not self._worker:
            self._worker.join()
        super().close()

    def set_completion_callback(self, callback: _CompletionCallback | None) -> None:
        """Set the callback invoked when backend completions may be ready."""

        self._begin_worker_mutation()
        try:
            super().set_completion_callback(callback)
        finally:
            self._end_worker_mutation()

    def _submit_socket_operation(
        self,
        sock: socket.socket,
        event: int,
        operation: Operation[T],
        attempt: Callable[[], T],
    ) -> None:
        self._begin_worker_mutation()
        try:
            super()._submit_socket_operation(sock, event, operation, attempt)
        finally:
            self._end_worker_mutation()

    def _cancel_operation(self, operation: Operation[Any]) -> bool:
        self._begin_worker_mutation()
        try:
            return super()._cancel_operation(operation)
        finally:
            self._end_worker_mutation()

    def wait(self, deadline: float | None = None) -> list[Operation[Any]]:
        """Return completed operations, waiting only for queued completions."""

        self._check_open()
        self._ensure_worker_started()
        completed = self._drain_completed()
        if completed or deadline == 0:
            return completed

        timeout = self._timeout_until_deadline(deadline)
        if timeout == 0:
            return []
        self._wait_for_completed(timeout)
        return self._drain_completed()

    async def wait_async(self, deadline: float | None = None) -> list[Operation[Any]]:
        """Wait asynchronously until completed operations are queued."""

        self._check_open()
        self._ensure_worker_started()
        completed = self._drain_completed()
        if completed or deadline == 0:
            return completed

        timeout = self._timeout_until_deadline(deadline)
        if timeout == 0:
            return []
        loop = _asyncio.get_running_loop()
        await loop.run_in_executor(None, self._wait_for_completed, timeout)
        return self._drain_completed()

    def _queue_completed(self, completed: list[Operation[Any]]) -> None:
        with self._completed_lock:
            self._completed.extend(completed)
            self._completed_ready.set()
        self._notify_completion()

    def _ensure_worker_started(self) -> None:
        with self._lock:
            if self._worker_started:
                return
            self._worker_started = True
            self._worker.start()

    def _worker_main(self) -> None:
        while not self._worker_stop.is_set():
            self._wait_for_worker_mutations()
            if self._worker_stop.is_set():
                return
            try:
                with self._lock:
                    completed = self._poll(None)
            except (OSError, ValueError, RuntimeError):
                return
            if completed:
                self._queue_completed(completed)

    def _drain_completed(self) -> list[Operation[Any]]:
        with self._completed_lock:
            completed = list(self._completed)
            self._completed.clear()
            if not self._completed:
                self._completed_ready.clear()
        return completed

    def _wait_for_completed(self, timeout: float | None) -> None:
        self._completed_ready.wait(timeout)

    def _begin_worker_mutation(self) -> None:
        with self._mutation_condition:
            self._mutation_requested = True
        self.break_wait()
        self._lock.acquire()

    def _end_worker_mutation(self) -> None:
        try:
            self._lock.release()
        finally:
            with self._mutation_condition:
                self._mutation_requested = False
                self._mutation_condition.notify_all()

    def _wait_for_worker_mutations(self) -> None:
        with self._mutation_condition:
            while self._mutation_requested and not self._worker_stop.is_set():
                self._mutation_condition.wait()


class ProactorScheduler(BaseScheduler):
    """Shared proactor-backed cooperative scheduling mechanics."""

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
        self._proactor.set_clock(self.time)

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
        self._proactor.wait(deadline)

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


class SyncProactorScheduler(SyncDrivingMixin, ProactorScheduler, SyncSchedulerDrivingAPI):
    """Synchronous scheduler whose IO wait point is a proactor backend."""

    async def _driver_wait(self) -> None:
        self._wait_thread()


class AsyncProactorScheduler(AsyncDrivingMixin, ProactorScheduler, AsyncSchedulerDrivingAPI):
    """Async-hosted scheduler whose IO wait point is a proactor backend."""

    def __init__(
        self,
        proactor_factory: ProactorFactory | None = None,
        *,
        runnable_queue_factory: RunnableQueueFactory | None = None,
    ) -> None:
        super().__init__(proactor_factory=proactor_factory, runnable_queue_factory=runnable_queue_factory)
        self._wakeup_loop: _asyncio.AbstractEventLoop | None = None

    def bind_loop(self, loop: _asyncio.AbstractEventLoop) -> None:
        """Bind this scheduler to an asyncio event loop clock and completion wakeups."""

        if self._wakeup_loop is not None and self._wakeup_loop is not loop:
            raise RuntimeError("AsyncProactorScheduler is already bound to a different event loop")
        self._wakeup_loop = loop
        self._time = loop.time

        def wake_loop() -> None:
            loop.call_soon_threadsafe(lambda: None)

        self._proactor.set_completion_callback(wake_loop)

    def _lazy_bind_running_loop(self) -> None:
        if self._wakeup_loop is None:
            self.bind_loop(_asyncio.get_running_loop())

    def _before_arun(self) -> None:
        self._lazy_bind_running_loop()

    def close(self) -> None:
        """Close proactor and scheduler-owned resources."""

        self._proactor.set_completion_callback(None)
        super().close()

    async def _driver_wait(self) -> None:
        self._lazy_bind_running_loop()
        deadline = self._next_timer_deadline()
        await self._proactor.wait_async(deadline)

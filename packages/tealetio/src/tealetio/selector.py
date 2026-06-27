from __future__ import annotations

import asyncio as _asyncio
import contextvars
import errno
import selectors
import socket
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable, cast

from .locks import Event
from .scheduler import (
    AsyncDrivingMixin,
    AsyncSchedulerDrivingAPI,
    BaseScheduler,
    RunnableQueueFactory,
    SyncDrivingMixin,
    SyncSchedulerDrivingAPI,
)

_FdCallback = tuple[Callable[..., object], tuple[object, ...], contextvars.Context]

__all__ = [
    "AsyncSelectorScheduler",
    "SelectorMixin",
    "SelectorScheduler",
    "SyncSelectorScheduler",
]


@dataclass
class _FdCallbacks:
    reader: _FdCallback | None = None
    writer: _FdCallback | None = None

    def empty(self) -> bool:
        return self.reader is None and self.writer is None


class SelectorMixin:
    """Selector-backed readiness waits for schedulers."""

    def __init__(self, selector: selectors.BaseSelector | None = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._selector = selector if selector is not None else selectors.DefaultSelector()
        self._fd_callbacks: defaultdict[int, _FdCallbacks] = defaultdict(_FdCallbacks)
        self._selector_wakeup_reader, self._selector_wakeup_writer = socket.socketpair()
        self._selector_wakeup_reader.setblocking(False)
        self._selector_wakeup_writer.setblocking(False)
        self._selector.register(
            self._selector_wakeup_reader.fileno(),
            selectors.EVENT_READ,
            self._selector_wakeup_reader.fileno(),
        )

    # -- Lifecycle -----------------------------------------------------

    def close(self) -> None:
        """Close selector resources and scheduler-owned resources."""

        self._selector.close()
        self._selector_wakeup_reader.close()
        self._selector_wakeup_writer.close()
        BaseScheduler.close(cast(BaseScheduler, self))

    # -- Readiness waits -----------------------------------------------

    def wait_readable(self, fileobj: object) -> None:
        """Block the current tealet until a file descriptor is readable."""

        fd = self._fileobj_to_fd(fileobj)
        ready = Event()
        active = True

        def wake() -> None:
            nonlocal active
            if not active:
                return
            active = False
            self.remove_reader(fd)
            ready.set()

        self.add_reader(fd, wake)

        try:
            ready.swait()
        finally:
            if active:
                active = False
                self.remove_reader(fd)

    def wait_writable(self, fileobj: object) -> None:
        """Block the current tealet until a file descriptor is writable."""

        fd = self._fileobj_to_fd(fileobj)
        ready = Event()
        active = True

        def wake() -> None:
            nonlocal active
            if not active:
                return
            active = False
            self.remove_writer(fd)
            ready.set()

        self.add_writer(fd, wake)

        try:
            ready.swait()
        finally:
            if active:
                active = False
                self.remove_writer(fd)

    # -- File descriptor callbacks -----------------------------------

    def add_reader(self, fd: int, callback: Callable[..., object], *args: object) -> None:
        """Register `callback(*args)` for readability on `fd`."""

        fd = self._fileobj_to_fd(fd)
        entry = self._fd_callbacks[fd]
        previous = entry.reader
        entry.reader = (callback, args, contextvars.copy_context())
        try:
            self._update_selector_registration(fd)
        except Exception:
            entry.reader = previous
            if entry.empty():
                del self._fd_callbacks[fd]
            self._update_selector_registration(fd)
            raise
        self._wake_selector()

    def remove_reader(self, fd: int) -> bool:
        """Remove the readability callback for `fd`."""

        fd = self._fileobj_to_fd(fd)
        entry = self._fd_callbacks.get(fd)
        if entry is None or entry.reader is None:
            return False
        entry.reader = None
        if entry.empty():
            del self._fd_callbacks[fd]
        self._update_selector_registration(fd)
        self._wake_selector()
        return True

    def add_writer(self, fd: int, callback: Callable[..., object], *args: object) -> None:
        """Register `callback(*args)` for writability on `fd`."""

        fd = self._fileobj_to_fd(fd)
        entry = self._fd_callbacks[fd]
        previous = entry.writer
        entry.writer = (callback, args, contextvars.copy_context())
        try:
            self._update_selector_registration(fd)
        except Exception:
            entry.writer = previous
            if entry.empty():
                del self._fd_callbacks[fd]
            self._update_selector_registration(fd)
            raise
        self._wake_selector()

    def remove_writer(self, fd: int) -> bool:
        """Remove the writability callback for `fd`."""

        fd = self._fileobj_to_fd(fd)
        entry = self._fd_callbacks.get(fd)
        if entry is None or entry.writer is None:
            return False
        entry.writer = None
        if entry.empty():
            del self._fd_callbacks[fd]
        self._update_selector_registration(fd)
        self._wake_selector()
        return True

    # -- Asyncio-style socket helpers ---------------------------------

    def sock_recv(self, sock: socket.socket, n: int) -> bytes:
        """Receive up to `n` bytes from a non-blocking socket."""

        self._check_socket(sock)
        while True:
            try:
                return sock.recv(n)
            except (BlockingIOError, InterruptedError):
                self.wait_readable(sock)

    def sock_recv_into(self, sock: socket.socket, buf: Any) -> int:
        """Receive bytes from a non-blocking socket into `buf`."""

        self._check_socket(sock)
        while True:
            try:
                return sock.recv_into(buf)
            except (BlockingIOError, InterruptedError):
                self.wait_readable(sock)

    def sock_recvfrom(self, sock: socket.socket, bufsize: int) -> tuple[bytes, Any]:
        """Receive datagram bytes and address from a non-blocking socket."""

        self._check_socket(sock)
        while True:
            try:
                return sock.recvfrom(bufsize)
            except (BlockingIOError, InterruptedError):
                self.wait_readable(sock)

    def sock_recvfrom_into(self, sock: socket.socket, buf: Any, nbytes: int = 0) -> tuple[int, Any]:
        """Receive datagram bytes into `buf` from a non-blocking socket."""

        self._check_socket(sock)
        while True:
            try:
                if nbytes:
                    return sock.recvfrom_into(buf, nbytes)
                return sock.recvfrom_into(buf)
            except (BlockingIOError, InterruptedError):
                self.wait_readable(sock)

    def sock_sendall(self, sock: socket.socket, data: Any) -> None:
        """Send all `data` through a non-blocking socket."""

        self._check_socket(sock)
        view = memoryview(data)
        total = 0
        while total < len(view):
            try:
                sent = sock.send(view[total:])
                if sent == 0:
                    self.wait_writable(sock)
                    continue
                total += sent
            except (BlockingIOError, InterruptedError):
                self.wait_writable(sock)

    def sock_sendto(self, sock: socket.socket, data: Any, address: Any) -> int:
        """Send one datagram through a non-blocking socket."""

        self._check_socket(sock)
        while True:
            try:
                return sock.sendto(data, address)
            except (BlockingIOError, InterruptedError):
                self.wait_writable(sock)

    def sock_accept(self, sock: socket.socket) -> tuple[socket.socket, Any]:
        """Accept one connection from a non-blocking listening socket."""

        self._check_socket(sock)
        while True:
            try:
                conn, address = sock.accept()
                conn.setblocking(False)
                return conn, address
            except (BlockingIOError, InterruptedError):
                self.wait_readable(sock)

    def sock_connect(self, sock: socket.socket, address: Any) -> None:
        """Connect a non-blocking socket to `address`."""

        self._check_socket(sock)
        try:
            sock.connect(address)
            return
        except (BlockingIOError, InterruptedError):
            pass
        except OSError as exc:
            if exc.errno not in (errno.EINPROGRESS, errno.EWOULDBLOCK, errno.EALREADY):
                raise

        while True:
            self.wait_writable(sock)
            err = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            if err == 0:
                return
            if err in (errno.EINPROGRESS, errno.EWOULDBLOCK, errno.EALREADY):
                continue
            raise OSError(err, errno.errorcode.get(err, "socket connect failed"))

    def _check_socket(self, sock: socket.socket) -> None:
        if sock.getblocking():
            raise ValueError("socket must be non-blocking")

    # -- File descriptor registration ---------------------------------

    def _fileobj_to_fd(self, fileobj: object) -> int:
        if isinstance(fileobj, int):
            fd = fileobj
        else:
            fileno = getattr(fileobj, "fileno", None)
            if fileno is None:
                raise ValueError("file object must be an fd or have fileno()")
            fd = fileno()
        if not isinstance(fd, int) or fd < 0:
            raise ValueError("invalid file descriptor")
        return fd

    def _selector_mask_for_fd(self, fd: int) -> int:
        mask = 0
        entry = self._fd_callbacks.get(fd)
        if entry is None:
            return mask
        if entry.reader is not None:
            mask |= selectors.EVENT_READ
        if entry.writer is not None:
            mask |= selectors.EVENT_WRITE
        return mask

    def _update_selector_registration(self, fd: int) -> None:
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

    def _schedule_fd_callback(self, fd: int, event: int) -> None:
        callbacks = self._fd_callbacks.get(fd)
        if callbacks is None:
            return
        assert event in (selectors.EVENT_READ, selectors.EVENT_WRITE)
        entry = callbacks.reader if event == selectors.EVENT_READ else callbacks.writer
        if entry is None:
            return
        callback, args, context = entry
        cast(BaseScheduler, self).call_soon(callback, *args, context=context)

    # -- Driver wakeup and polling ------------------------------------

    def _drain_selector_wakeup(self) -> None:
        while True:
            try:
                if not self._selector_wakeup_reader.recv(4096):
                    return
            except BlockingIOError:
                return
            except OSError:
                return

    def _wake_selector(self) -> None:
        try:
            self._selector_wakeup_writer.send(b"\0")
        except BlockingIOError:
            pass
        except OSError:
            pass

    def _break_wait_threadsafe(self) -> None:
        self._wake_selector()

    def _break_wait(self) -> None:
        self._wake_selector()

    def _wait_thread(self) -> None:
        scheduler = cast(BaseScheduler, self)
        deadline = scheduler._next_timer_deadline()
        timeout = None if deadline is None else scheduler._delay_until(deadline)
        events = self._selector.select(timeout=timeout)
        self._process_selector_events(events)

    def _process_selector_events(self, events: list[tuple[selectors.SelectorKey, int]]) -> None:
        wakeup_fd = self._selector_wakeup_reader.fileno()
        for key, mask in events:
            fd = key.fd
            if fd == wakeup_fd:
                self._drain_selector_wakeup()
                continue
            if mask & selectors.EVENT_READ:
                self._schedule_fd_callback(fd, selectors.EVENT_READ)
            if mask & selectors.EVENT_WRITE:
                self._schedule_fd_callback(fd, selectors.EVENT_WRITE)
            self._update_selector_registration(fd)

    def _has_pending_driver_work(self) -> bool:
        return bool(self._fd_callbacks) or BaseScheduler._has_pending_driver_work(cast(BaseScheduler, self))


class SelectorScheduler(SelectorMixin, BaseScheduler, ABC):
    """Shared selector-backed cooperative scheduling mechanics."""

    def __init__(
        self,
        selector: selectors.BaseSelector | None = None,
        *,
        runnable_queue_factory: RunnableQueueFactory | None = None,
    ) -> None:
        super().__init__(selector=selector, runnable_queue_factory=runnable_queue_factory)

    @abstractmethod
    async def _driver_wait(self) -> None:
        raise NotImplementedError


class SyncSelectorScheduler(SyncDrivingMixin, SelectorScheduler, SyncSchedulerDrivingAPI):
    """Synchronous scheduler with selector-backed fd readiness waits."""

    async def _driver_wait(self) -> None:
        self._wait_thread()


class AsyncSelectorScheduler(AsyncDrivingMixin, SelectorScheduler, AsyncSchedulerDrivingAPI):
    """Async-hosted scheduler with selector-backed fd readiness waits."""

    def __init__(
        self,
        selector: selectors.BaseSelector | None = None,
        *,
        runnable_queue_factory: RunnableQueueFactory | None = None,
    ) -> None:
        super().__init__(selector=selector, runnable_queue_factory=runnable_queue_factory)
        self._wakeup_loop: _asyncio.AbstractEventLoop | None = None

    def bind_loop(self, loop: _asyncio.AbstractEventLoop) -> None:
        """Bind this scheduler to an asyncio event loop clock."""

        if self._wakeup_loop is not None and self._wakeup_loop is not loop:
            raise RuntimeError("AsyncSelectorScheduler is already bound to a different event loop")
        self._wakeup_loop = loop
        self._time = loop.time

    def _lazy_bind_running_loop(self) -> None:
        if self._wakeup_loop is None:
            self.bind_loop(_asyncio.get_running_loop())

    def _before_arun(self) -> None:
        self._lazy_bind_running_loop()

    async def _driver_wait(self) -> None:
        loop = self._wakeup_loop
        if loop is None:
            self._lazy_bind_running_loop()
            loop = self._wakeup_loop
        assert loop is not None

        deadline = self._next_timer_deadline()
        if deadline is not None and deadline <= self.time():
            self._process_selector_events(self._selector.select(timeout=0))
            return

        waiter = loop.create_future()

        def wake() -> None:
            if not waiter.done():
                waiter.set_result(None)

        registered: list[tuple[int, int]] = []
        timeout = None if deadline is None else max(0.0, deadline - self.time())
        try:
            for key in self._selector.get_map().values():
                try:
                    if key.events & selectors.EVENT_READ:
                        loop.add_reader(key.fd, wake)
                        registered.append((key.fd, selectors.EVENT_READ))
                    if key.events & selectors.EVENT_WRITE:
                        loop.add_writer(key.fd, wake)
                        registered.append((key.fd, selectors.EVENT_WRITE))
                except (AttributeError, NotImplementedError) as exc:
                    raise RuntimeError(
                        "AsyncSelectorScheduler requires an asyncio event loop with add_reader/add_writer support"
                    ) from exc
            if timeout is None:
                await waiter
            else:
                try:
                    await _asyncio.wait_for(waiter, timeout)
                except _asyncio.TimeoutError:
                    pass
        finally:
            for fd, event in registered:
                if event == selectors.EVENT_READ:
                    loop.remove_reader(fd)
                else:
                    loop.remove_writer(fd)

        self._process_selector_events(self._selector.select(timeout=0))

    async def _driver_yield(self) -> None:
        await _asyncio.sleep(0)

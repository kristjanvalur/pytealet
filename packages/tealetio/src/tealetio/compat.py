from __future__ import annotations

import asyncio
import contextvars
import math
import select
import selectors
import socket
import sys
from typing import Any, Callable, Protocol, cast


class SelectReleasedSelector(Protocol):
    def select_released(
        self, timeout: float | None = None, lock: Any | None = None
    ) -> list[tuple[selectors.SelectorKey, int]]: ...


class _ReleasedSelectSelector(selectors.SelectSelector):
    """Select selector that snapshots fd sets before releasing a caller lock."""

    def select_released(
        self, timeout: float | None = None, lock: Any | None = None
    ) -> list[tuple[selectors.SelectorKey, int]]:
        timeout = None if timeout is None else max(timeout, 0)
        selector = cast(Any, self)
        readers = frozenset(selector._readers)
        writers = frozenset(selector._writers)
        ready: list[tuple[selectors.SelectorKey, int]] = []

        if lock is not None:
            lock.release()
        try:
            try:
                readable, writable, _ = selector._select(readers, writers, [], timeout)
            except InterruptedError:
                return ready
        finally:
            if lock is not None:
                lock.acquire()

        readable = frozenset(readable)
        writable = frozenset(writable)
        fd_to_key_get = selector._fd_to_key.get
        for fd in readable | writable:
            key = fd_to_key_get(fd)
            if key:
                events = (fd in readable and selectors.EVENT_READ) | (fd in writable and selectors.EVENT_WRITE)
                ready.append((key, events & key.events))
        return ready


if hasattr(selectors, "EpollSelector"):

    class _ReleasedEpollSelector(selectors.EpollSelector):
        """Epoll selector that releases a caller lock only during epoll_wait."""

        def select_released(
            self, timeout: float | None = None, lock: Any | None = None
        ) -> list[tuple[selectors.SelectorKey, int]]:
            if timeout is None:
                timeout = -1
            elif timeout <= 0:
                timeout = 0
            else:
                timeout = math.ceil(timeout * 1e3) * 1e-3

            selector = cast(Any, self)
            max_events = len(selector._fd_to_key) or 1
            ready: list[tuple[selectors.SelectorKey, int]] = []

            if lock is not None:
                lock.release()
            try:
                try:
                    fd_event_list = selector._selector.poll(timeout, max_events)
                except InterruptedError:
                    return ready
            finally:
                if lock is not None:
                    lock.acquire()

            fd_to_key = selector._fd_to_key
            for fd, event in fd_event_list:
                key = fd_to_key.get(fd)
                if key:
                    events = (event & ~select.EPOLLIN and selectors.EVENT_WRITE) | (
                        event & ~select.EPOLLOUT and selectors.EVENT_READ
                    )
                    ready.append((key, events & key.events))
            return ready

else:
    _ReleasedEpollSelector = None


def released_default_selector() -> selectors.BaseSelector:
    if _ReleasedEpollSelector is not None:
        return _ReleasedEpollSelector()
    return _ReleasedSelectSelector()


if sys.version_info >= (3, 11):

    def run_asyncio_once(
        awaitable,
        *,
        context: contextvars.Context | None = None,
        loop_factory: Callable[[], asyncio.AbstractEventLoop] | None = None,
        debug: bool | None = None,
    ):
        with asyncio.Runner(loop_factory=loop_factory, debug=debug) as runner:
            if context is None:
                return runner.run(awaitable)
            return runner.run(awaitable, context=context)

else:

    def run_asyncio_once(
        awaitable,
        *,
        context: contextvars.Context | None = None,
        loop_factory: Callable[[], asyncio.AbstractEventLoop] | None = None,
        debug: bool | None = None,
    ):
        if loop_factory is None:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        else:
            loop = loop_factory()
        try:
            if debug is not None:
                loop.set_debug(debug)
            if context is None:
                return loop.run_until_complete(awaitable)
            task = context.run(loop.create_task, awaitable)
            return loop.run_until_complete(task)
        finally:
            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
                loop.run_until_complete(loop.shutdown_default_executor())
            finally:
                asyncio.set_event_loop(None)
                loop.close()


if sys.version_info >= (3, 11):

    async def wait_for_timeout(awaitable, timeout: float) -> None:
        async with asyncio.timeout(timeout):
            await awaitable

    async def wait_until(awaitable, when: float) -> None:
        async with asyncio.timeout_at(when):
            await awaitable

else:

    async def wait_for_timeout(awaitable, timeout: float) -> None:
        await asyncio.wait_for(awaitable, timeout=timeout)

    async def wait_until(awaitable, when: float) -> None:
        timeout = max(0.0, when - asyncio.get_running_loop().time())
        await asyncio.wait_for(awaitable, timeout=timeout)


def _wake_future(future: asyncio.Future[None]) -> None:
    if not future.done():
        future.set_result(None)


if sys.version_info >= (3, 11):

    def sock_recvfrom(loop: asyncio.AbstractEventLoop, sock: socket.socket, bufsize: int):
        return loop.sock_recvfrom(sock, bufsize)

    def sock_recvfrom_into(
        loop: asyncio.AbstractEventLoop,
        sock: socket.socket,
        buf: Any,
        nbytes: int = 0,
    ):
        if nbytes:
            return loop.sock_recvfrom_into(sock, buf, nbytes)
        return loop.sock_recvfrom_into(sock, buf)

    def sock_sendto(loop: asyncio.AbstractEventLoop, sock: socket.socket, data: Any, address: Any):
        return loop.sock_sendto(sock, data, address)

else:

    async def _sock_recvfrom(sock: socket.socket, bufsize: int) -> tuple[bytes, Any]:
        loop = asyncio.get_running_loop()
        while True:
            try:
                return sock.recvfrom(bufsize)
            except (BlockingIOError, InterruptedError):
                waiter = loop.create_future()
                loop.add_reader(sock.fileno(), _wake_future, waiter)
                try:
                    await waiter
                finally:
                    loop.remove_reader(sock.fileno())

    async def _sock_recvfrom_into(sock: socket.socket, buf: Any, nbytes: int = 0) -> tuple[int, Any]:
        loop = asyncio.get_running_loop()
        while True:
            try:
                if nbytes:
                    return sock.recvfrom_into(buf, nbytes)
                return sock.recvfrom_into(buf)
            except (BlockingIOError, InterruptedError):
                waiter = loop.create_future()
                loop.add_reader(sock.fileno(), _wake_future, waiter)
                try:
                    await waiter
                finally:
                    loop.remove_reader(sock.fileno())

    async def _sock_sendto(sock: socket.socket, data: Any, address: Any) -> int:
        loop = asyncio.get_running_loop()
        while True:
            try:
                return sock.sendto(data, address)
            except (BlockingIOError, InterruptedError):
                waiter = loop.create_future()
                loop.add_writer(sock.fileno(), _wake_future, waiter)
                try:
                    await waiter
                finally:
                    loop.remove_writer(sock.fileno())

    def sock_recvfrom(loop: asyncio.AbstractEventLoop, sock: socket.socket, bufsize: int):
        return _sock_recvfrom(sock, bufsize)

    def sock_recvfrom_into(
        loop: asyncio.AbstractEventLoop,
        sock: socket.socket,
        buf: Any,
        nbytes: int = 0,
    ):
        return _sock_recvfrom_into(sock, buf, nbytes)

    def sock_sendto(loop: asyncio.AbstractEventLoop, sock: socket.socket, data: Any, address: Any):
        return _sock_sendto(sock, data, address)

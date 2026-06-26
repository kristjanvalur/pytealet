from __future__ import annotations

import asyncio
import contextvars
import socket
import sys
from typing import Any, Callable

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

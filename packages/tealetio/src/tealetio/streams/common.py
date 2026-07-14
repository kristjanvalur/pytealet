"""Shared stream helpers and scheduler IO resolution."""

from __future__ import annotations

import socket
from collections.abc import Coroutine
from typing import Any, TypeVar

from asynkit import coro_drive

from ..io_manager import IO_UNSUPPORTED_ERROR, SELECTOR_IO_UNSUPPORTED_ERROR, ProactorIOManager, SupportsProactorIO
from ..scheduler import BaseScheduler

T = TypeVar("T")

DEFAULT_LIMIT = 2**16


def run_coro(coro: Coroutine[Any, Any, T]) -> T:
    """Drive an async-stream coroutine without an asyncio event loop.

    ``AsyncStream*`` methods are ``async def`` for handler compatibility, but
    they ultimately block through the scheduler-owned IO manager's
    ``IOWaiter.wait()``
    path rather than yielding asyncio futures. Public stream and factory APIs
    depend on ``SocketIO`` only. Unexpected yields surface as ``RuntimeError``.
    """

    def on_yield(value: object) -> object:
        raise RuntimeError(f"tealetio stream coroutine yielded unexpectedly: {value!r}")

    return coro_drive(coro, on_yield)


def writer_extra_info(sock: socket.socket, name: str, default: Any = None) -> Any:
    if name == "socket":
        return sock
    if name == "peername":
        try:
            return sock.getpeername()
        except OSError:
            return default
    if name == "sockname":
        try:
            return sock.getsockname()
        except OSError:
            return default
    return default


def resolve_scheduler(scheduler: BaseScheduler | None) -> BaseScheduler:
    if scheduler is not None:
        return scheduler
    from ..scheduler import get_running_scheduler

    return get_running_scheduler()


def require_proactor_io(scheduler: BaseScheduler) -> ProactorIOManager:
    """Return ``scheduler.io`` for proactor schedulers or raise with a targeted message."""

    from ..proactor import ProactorScheduler
    from ..selector import SelectorScheduler

    if isinstance(scheduler, ProactorScheduler):
        proactor_scheduler: SupportsProactorIO = scheduler
        return proactor_scheduler.io
    if isinstance(scheduler, SelectorScheduler):
        raise RuntimeError(SELECTOR_IO_UNSUPPORTED_ERROR)
    raise RuntimeError(IO_UNSUPPORTED_ERROR)

"""Coroutine and socket helpers without scheduler imports."""

from __future__ import annotations

import socket
from collections.abc import Coroutine
from typing import Any, TypeVar

from asynkit import coro_drive

T = TypeVar("T")


def run_coro(coro: Coroutine[Any, Any, T]) -> T:
    """Drive an async-stream coroutine without an asyncio event loop."""

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

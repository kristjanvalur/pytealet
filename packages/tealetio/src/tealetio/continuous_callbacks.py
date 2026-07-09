"""Composition helpers for continuous proactor operation callbacks."""

from __future__ import annotations

import socket
from collections.abc import Callable
from typing import TYPE_CHECKING, TypeVar

from .recv_iter import RECV_MANY_BUFFER_PRESSURE, _RecvManyResult

if TYPE_CHECKING:
    from .scheduler import BaseScheduler

T = TypeVar("T")


def before_delivery(
    handler: Callable[[T], T],
    deliver: Callable[[T], object],
) -> Callable[[T], None]:
    """Run ``handler`` on the delivery thread, then pass its result to ``deliver``."""

    def wrapped(result: T) -> None:
        deliver(handler(result))

    return wrapped


def marshal_to_scheduler(
    scheduler: BaseScheduler,
    callback: Callable[[T], object],
) -> Callable[[T], None]:
    """Wrap ``callback`` so each result is delivered on the scheduler thread."""

    def deliver(result: T) -> None:
        scheduler.call_soon_threadsafe(callback, result)

    return deliver


def recv_many_echo_handler(sock: socket.socket) -> Callable[[_RecvManyResult], _RecvManyResult]:
    """Echo each received data chunk back on ``sock`` before downstream delivery."""

    def handler(result: _RecvManyResult) -> _RecvManyResult:
        index, payload = result
        if index == RECV_MANY_BUFFER_PRESSURE:
            return result
        if isinstance(payload, memoryview) and payload:
            sock.send(payload.tobytes())
        return result

    return handler

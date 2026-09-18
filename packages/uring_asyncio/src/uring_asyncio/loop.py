"""Asyncio event loop using ``UringProactor`` as its IOCP-shaped backend."""

from __future__ import annotations

import asyncio
import sys
from asyncio import proactor_events
from collections.abc import Coroutine
from typing import Any, TypeVar

from .proactor import UringProactor

_T = TypeVar("_T")


class UringProactorEventLoop(proactor_events.BaseProactorEventLoop):
    """asyncio proactor loop whose completions come from ``uring_api.Ring``.

    Suitable as ``asyncio.run(..., loop_factory=UringProactorEventLoop)`` on
    Python 3.12+. The stdlib self-pipe is kept for ``call_soon_threadsafe``
    and ``signal.set_wakeup_fd``.
    """

    def __init__(self, proactor: UringProactor | None = None) -> None:
        if proactor is None:
            proactor = UringProactor()
        super().__init__(proactor)

    def run_forever(self) -> None:
        """Arm the self-pipe read, then run until ``stop()``."""

        # BaseProactorEventLoop creates the socketpair but only Windows
        # ProactorEventLoop starts the perpetual recv. Do that here so
        # call_soon_threadsafe can wake select().
        loop_self_reading = getattr(self, "_loop_self_reading", None)
        if loop_self_reading is not None and getattr(self, "_self_reading_future", None) is None:
            loop_self_reading()
        try:
            super().run_forever()
        finally:
            self_reading_future = getattr(self, "_self_reading_future", None)
            if self_reading_future is not None:
                self_reading_future.cancel()
                self._self_reading_future = None


def run(main: Coroutine[Any, Any, _T], *, debug: bool | None = None) -> _T:
    """Run ``main`` on ``UringProactorEventLoop``.

    On Python 3.12+ this is ``asyncio.run(..., loop_factory=UringProactorEventLoop)``.
    Earlier versions construct the loop directly.
    """

    if sys.version_info >= (3, 12):
        if debug is None:
            return asyncio.run(main, loop_factory=UringProactorEventLoop)
        return asyncio.run(main, debug=debug, loop_factory=UringProactorEventLoop)

    loop = UringProactorEventLoop()
    try:
        asyncio.set_event_loop(loop)
        if debug is not None:
            loop.set_debug(debug)
        return loop.run_until_complete(main)
    finally:
        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
            shutdown_default_executor = getattr(loop, "shutdown_default_executor", None)
            if shutdown_default_executor is not None:
                loop.run_until_complete(shutdown_default_executor())
        finally:
            asyncio.set_event_loop(None)
            loop.close()

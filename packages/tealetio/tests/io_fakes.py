"""Shared IO test fakes for direct ``ProactorIOManager`` unit tests."""

from __future__ import annotations

from typing import Any

from tealetio.scheduler import TimerHandle


class StubScheduler:
    """Minimal scheduler stand-in for ``ProactorIOManager`` unit tests."""

    def __init__(self) -> None:
        self._exception_handler: Any = None
        self.timer_handles: list[tuple[TimerHandle, Any, tuple[object, ...]]] = []

    def set_exception_handler(self, handler: Any) -> None:
        self._exception_handler = handler

    def call_exception_handler(self, context: dict[str, Any]) -> None:
        handler = self._exception_handler
        if handler is None:
            raise context["exception"]
        handler(context)

    def call_soon_threadsafe(self, callback, *args: object, **kwargs: object) -> None:
        del kwargs
        try:
            callback(*args)
        except BaseException as exc:
            self.call_exception_handler(
                {
                    "message": "Exception in callback",
                    "exception": exc,
                    "scheduler": self,
                }
            )

    def call_later(
        self,
        delay: float,
        callback: Any,
        *args: object,
        context: Any = None,
    ) -> TimerHandle:
        del delay, context
        handle = TimerHandle(0.0, callback, args)
        self.timer_handles.append((handle, callback, args))
        return handle

    def fire_timers(self) -> None:
        for handle, callback, args in self.timer_handles:
            if not handle.cancelled():
                callback(*args)
from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable
from contextlib import suppress
from typing import Any

from . import scheduler as scheduler_module


class AsyncRunner:
    """Run a scheduler inside an existing asyncio loop as a background task."""

    def __init__(
        self,
        *,
        scheduler: scheduler_module.SimpleScheduler | None = None,
        scheduler_factory: Callable[[], scheduler_module.SimpleScheduler] | None = None,
    ) -> None:
        if scheduler is not None and scheduler_factory is not None:
            raise ValueError("provide either scheduler or scheduler_factory, not both")
        self._scheduler = scheduler
        self._scheduler_factory = scheduler_factory
        self._pump_task: asyncio.Task[None] | None = None
        self._stop_event: asyncio.Event | None = None

    def get_scheduler(self) -> scheduler_module.SimpleScheduler | None:
        return self._scheduler

    @property
    def task(self) -> asyncio.Task[None] | None:
        return self._pump_task

    async def start(self) -> scheduler_module.SimpleScheduler:
        if self._pump_task is not None:
            return self._require_scheduler()

        self._scheduler = self._scheduler or self._create_scheduler()
        self._stop_event = asyncio.Event()
        self._pump_task = asyncio.create_task(self._pump_loop())
        await asyncio.sleep(0)
        return self._scheduler

    async def close(self) -> None:
        if self._pump_task is None:
            return

        if self._stop_event is not None:
            self._stop_event.set()

        with suppress(asyncio.CancelledError):
            await self._pump_task

        self._pump_task = None
        self._stop_event = None

    async def run(self, entry, /, *args: Any, **kwargs: Any):
        await self.start()
        scheduler = self._require_scheduler()
        previous = scheduler_module._current_scheduler()
        scheduler_module.set_scheduler(scheduler)
        try:
            return await _invoke_entry(entry, *args, **kwargs)
        finally:
            scheduler_module.set_scheduler(previous)

    def _create_scheduler(self) -> scheduler_module.SimpleScheduler:
        factory = self._scheduler_factory or scheduler_module.get_default_scheduler_factory()
        created = factory()
        if not isinstance(created, scheduler_module.SimpleScheduler):
            raise TypeError("scheduler factory must return a SimpleScheduler instance")
        return created

    def _require_scheduler(self) -> scheduler_module.SimpleScheduler:
        if self._scheduler is None:
            raise RuntimeError("runner has no scheduler")
        return self._scheduler

    async def _pump_loop(self) -> None:
        scheduler = self._require_scheduler()
        stop_event = self._stop_event
        assert stop_event is not None
        while not stop_event.is_set():
            if scheduler._tasks or scheduler._timers or scheduler._pending_async_waits:
                await scheduler.arun()
                continue
            await asyncio.sleep(0.001)


async def run_async(entry, /, *args: Any, scheduler_factory: Callable[[], scheduler_module.SimpleScheduler] | None = None, **kwargs: Any):
    """Convenience helper that runs one entry under a temporary AsyncRunner."""

    runner = AsyncRunner(scheduler_factory=scheduler_factory)
    try:
        return await runner.run(entry, *args, **kwargs)
    finally:
        await runner.close()


class Runner:
    """Run scheduler-backed entries from synchronous code without asyncio."""

    def __init__(
        self,
        *,
        scheduler: scheduler_module.SimpleScheduler | None = None,
        scheduler_factory: Callable[[], scheduler_module.SimpleScheduler] | None = None,
    ) -> None:
        if scheduler is not None and scheduler_factory is not None:
            raise ValueError("provide either scheduler or scheduler_factory, not both")
        self._scheduler = scheduler
        self._scheduler_factory = scheduler_factory

    def get_scheduler(self) -> scheduler_module.SimpleScheduler | None:
        return self._scheduler

    def run(self, entry, /, *args: Any, **kwargs: Any):
        scheduler = self._scheduler or self._create_scheduler()
        self._scheduler = scheduler
        previous = scheduler_module._current_scheduler()
        scheduler_module.set_scheduler(scheduler)
        try:
            result = _invoke_sync_entry(entry, *args, **kwargs)
            scheduler.run()
            return result
        finally:
            scheduler_module.set_scheduler(previous)

    def close(self) -> None:
        return None

    def __enter__(self) -> "Runner":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def _create_scheduler(self) -> scheduler_module.SimpleScheduler:
        factory = self._scheduler_factory or scheduler_module.get_default_scheduler_factory()
        created = factory()
        if not isinstance(created, scheduler_module.SimpleScheduler):
            raise TypeError("scheduler factory must return a SimpleScheduler instance")
        return created


async def _invoke_entry(entry, /, *args: Any, **kwargs: Any):
    if inspect.isawaitable(entry):
        if args or kwargs:
            if inspect.iscoroutine(entry):
                entry.close()
            raise TypeError("args/kwargs are not allowed when entry is an awaitable")
        return await entry

    if callable(entry):
        result = entry(*args, **kwargs)
        if isinstance(result, Awaitable) or inspect.isawaitable(result):
            return await result
        return result

    raise TypeError("entry must be a callable or awaitable")


def _invoke_sync_entry(entry, /, *args: Any, **kwargs: Any):
    if inspect.isawaitable(entry):
        if inspect.iscoroutine(entry):
            entry.close()
        raise TypeError("sync runner entry must be synchronous")

    if callable(entry):
        result = entry(*args, **kwargs)
        if inspect.isawaitable(result):
            if inspect.iscoroutine(result):
                result.close()
            raise TypeError("sync runner entry must be synchronous")
        return result

    raise TypeError("entry must be a callable")

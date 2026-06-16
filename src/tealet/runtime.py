from __future__ import annotations

import asyncio
import contextvars
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
        debug: bool | None = None,
    ) -> None:
        if scheduler is not None and scheduler_factory is not None:
            raise ValueError("provide either scheduler or scheduler_factory, not both")
        self._scheduler = scheduler
        self._scheduler_factory = scheduler_factory
        self._debug = debug
        self._debug_loop: asyncio.AbstractEventLoop | None = None
        self._previous_loop_debug: bool | None = None
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
        if self._debug is not None:
            self._scheduler.set_debug(self._debug)
            loop = asyncio.get_running_loop()
            self._debug_loop = loop
            self._previous_loop_debug = loop.get_debug()
            loop.set_debug(self._debug)
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

        if self._debug_loop is not None and self._previous_loop_debug is not None:
            self._debug_loop.set_debug(self._previous_loop_debug)
            self._debug_loop = None
            self._previous_loop_debug = None

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


async def run_async(
    entry,
    /,
    *args: Any,
    scheduler_factory: Callable[[], scheduler_module.SimpleScheduler] | None = None,
    debug: bool | None = None,
    **kwargs: Any,
):
    """Convenience helper that runs one entry under a temporary AsyncRunner."""

    runner = AsyncRunner(scheduler_factory=scheduler_factory, debug=debug)
    try:
        return await runner.run(entry, *args, **kwargs)
    finally:
        await runner.close()


def run(
    entry,
    /,
    *,
    context: contextvars.Context | None = None,
    scheduler_factory: Callable[[], scheduler_module.SimpleScheduler] | None = None,
    debug: bool | None = None,
):
    """Convenience helper that runs one entry under a temporary Runner."""

    runner = Runner(scheduler_factory=scheduler_factory, debug=debug)
    try:
        return runner.run(entry, context=context)
    finally:
        runner.close()


class Runner:
    """Run scheduler-backed entries from synchronous code without asyncio."""

    def __init__(
        self,
        *,
        scheduler: scheduler_module.SimpleScheduler | None = None,
        scheduler_factory: Callable[[], scheduler_module.SimpleScheduler] | None = None,
        context: contextvars.Context | None = None,
        debug: bool | None = None,
    ) -> None:
        if scheduler is not None and scheduler_factory is not None:
            raise ValueError("provide either scheduler or scheduler_factory, not both")
        self._scheduler = scheduler
        self._scheduler_factory = scheduler_factory
        self._context = context
        self._debug = debug
        self._closed = False
        self._initialized = False
        self._previous_scheduler: scheduler_module.SimpleScheduler | None = None

    def get_scheduler(self) -> scheduler_module.SimpleScheduler:
        self._lazy_init()
        scheduler = self._scheduler
        assert scheduler is not None
        return scheduler

    def run(self, entry, /, *, context: contextvars.Context | None = None):
        self._lazy_init()
        scheduler = self._scheduler
        assert scheduler is not None
        runner_context = self._context
        assert runner_context is not None
        run_context = context if context is not None else runner_context

        if isinstance(entry, scheduler_module.Future):
            def wait_for_future() -> Any:
                return entry.wait()

            task = scheduler.spawn(wait_for_future, context=run_context)
        elif callable(entry):
            task = scheduler.spawn(entry, context=run_context)
        else:
            raise TypeError("entry must be a callable or Future")
        return scheduler.run_until_complete(task)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._initialized:
            scheduler_module.set_scheduler(self._previous_scheduler)
            self._previous_scheduler = None
            self._initialized = False
        self._scheduler = None

    def __enter__(self) -> "Runner":
        self._lazy_init()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def _create_scheduler(self) -> scheduler_module.SimpleScheduler:
        factory = self._scheduler_factory or scheduler_module.get_default_scheduler_factory()
        created = factory()
        if not isinstance(created, scheduler_module.SimpleScheduler):
            raise TypeError("scheduler factory must return a SimpleScheduler instance")
        return created

    def _lazy_init(self) -> None:
        if self._closed:
            raise RuntimeError("runner is closed")
        if self._initialized:
            return

        current = scheduler_module._current_scheduler()
        if current is not None and current.is_running():
            raise RuntimeError("cannot initialize runner while another scheduler is running")

        if self._scheduler is None:
            self._scheduler = self._create_scheduler()
        if self._debug is not None:
            self._scheduler.set_debug(self._debug)
        if self._context is None:
            self._context = contextvars.copy_context()

        self._previous_scheduler = current
        scheduler_module.set_scheduler(self._scheduler)
        self._initialized = True


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


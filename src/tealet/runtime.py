from __future__ import annotations

import asyncio
import contextvars
from collections.abc import Callable
from typing import Any

from . import scheduler as scheduler_module


class AsyncRunner:
    """Run a scheduler inside an existing asyncio loop as a background task."""

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

    def get_scheduler(self) -> scheduler_module.SimpleScheduler | None:
        return self._scheduler

    @property
    def task(self) -> asyncio.Task[None] | None:
        return None

    async def start(self) -> scheduler_module.SimpleScheduler:
        self._lazy_init()
        return self._require_scheduler()

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._initialized:
            scheduler_module.set_scheduler(self._previous_scheduler)
            self._previous_scheduler = None
            self._initialized = False
        self._scheduler = None

    async def run(self, entry, /, *, context: contextvars.Context | None = None):
        await self.start()
        scheduler = self._require_scheduler()
        runner_context = self._context
        assert runner_context is not None
        run_context = context if context is not None else runner_context
        if isinstance(entry, scheduler_module.Future):
            target = entry
        elif callable(entry):
            target = scheduler.spawn(entry, context=run_context)
        else:
            raise TypeError("entry must be a callable or Future")
        return await scheduler.arun_until_complete(target)

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


async def run_async(
    entry,
    /,
    *,
    context: contextvars.Context | None = None,
    scheduler_factory: Callable[[], scheduler_module.SimpleScheduler] | None = None,
    debug: bool | None = None,
):
    """Convenience helper that runs one entry under a temporary AsyncRunner."""

    runner = AsyncRunner(scheduler_factory=scheduler_factory, context=context, debug=debug)
    try:
        return await runner.run(entry)
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


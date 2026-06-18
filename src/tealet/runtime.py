from __future__ import annotations

import asyncio
import contextvars
from collections.abc import Callable
from typing import Any, ClassVar, Generic, TypeVar

from . import scheduler as scheduler_module


SchedulerT = TypeVar("SchedulerT", bound=scheduler_module.CoreSchedulerDrivingAPI)


class BaseRunner(Generic[SchedulerT]):
    default_factory: ClassVar[Callable[[], SchedulerT]]
    # TODO: Install KeyboardInterrupt handlers comparable to asyncio runners,
    # routing interrupts through the active user task rather than process main.
    # TODO: Decide whether cancellation should propagate across Future
    # boundaries, or whether waiting cancellation should only detach waiters.

    def __init__(
        self,
        *,
        scheduler_factory: Callable[[], SchedulerT] | None = None,
        context: contextvars.Context | None = None,
        debug: bool | None = None,
    ) -> None:
        self._scheduler: SchedulerT | None = None
        self._scheduler_factory = scheduler_factory
        self._context = context
        self._debug = debug
        self._closed = False
        self._initialized = False
        self._previous_scheduler: scheduler_module.BaseScheduler | None = None

    def get_scheduler(self) -> SchedulerT:
        self._lazy_init()
        scheduler = self._scheduler
        assert scheduler is not None
        return scheduler

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._initialized:
            scheduler_module.set_scheduler(self._previous_scheduler)
            self._previous_scheduler = None
            self._initialized = False
        self._scheduler = None

    def _create_scheduler(self) -> SchedulerT:
        factory = self._scheduler_factory or self.default_factory
        return factory()

    def _require_scheduler(self) -> SchedulerT:
        if self._scheduler is None:
            raise RuntimeError("runner has no scheduler")
        return self._scheduler

    def _resolve_context(self, context: contextvars.Context | None) -> contextvars.Context:
        runner_context = self._context
        assert runner_context is not None
        return context if context is not None else runner_context

    def _target_from_entry(self, entry, context: contextvars.Context):
        scheduler = self._require_scheduler()
        if isinstance(entry, scheduler_module.Future):
            return entry
        if callable(entry):
            return scheduler.spawn(entry, context=context)
        raise TypeError("entry must be a callable or Future")

    def _lazy_init(self) -> None:
        if self._closed:
            raise RuntimeError("runner is closed")
        if self._initialized:
            return

        current = scheduler_module._current_scheduler()
        if current is not None and current.is_running():
            raise RuntimeError("cannot initialize runner while another scheduler is running")

        self._scheduler = self._create_scheduler()
        if self._debug is not None:
            self._scheduler.set_debug(self._debug)
        if self._context is None:
            self._context = contextvars.copy_context()

        self._previous_scheduler = current
        scheduler_module.set_scheduler(self._scheduler)
        self._initialized = True


class AsyncRunner(BaseRunner[scheduler_module.AsyncSchedulerDrivingAPI]):
    """Run scheduler-backed entries from within an existing asyncio task."""

    default_factory: ClassVar[Callable[[], scheduler_module.AsyncSchedulerDrivingAPI]] = scheduler_module.AsyncScheduler

    @property
    def task(self) -> asyncio.Task[None] | None:
        return None

    async def close(self) -> None:
        super().close()

    async def run(self, entry, /, *, context: contextvars.Context | None = None):
        self._lazy_init()
        scheduler = self._require_scheduler()
        run_context = self._resolve_context(context)
        target = self._target_from_entry(entry, run_context)
        return await scheduler.arun_until_complete(target)


async def run_async(
    entry,
    /,
    *,
    context: contextvars.Context | None = None,
    scheduler_factory: Callable[[], scheduler_module.AsyncSchedulerDrivingAPI] | None = None,
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
    scheduler_factory: Callable[[], scheduler_module.SyncSchedulerDrivingAPI] | None = None,
    debug: bool | None = None,
):
    """Convenience helper that runs one entry under a temporary Runner."""

    runner = Runner(scheduler_factory=scheduler_factory, debug=debug)
    try:
        return runner.run(entry, context=context)
    finally:
        runner.close()


class Runner(BaseRunner[scheduler_module.SyncSchedulerDrivingAPI]):
    """Run scheduler-backed entries from synchronous code without asyncio."""

    default_factory: ClassVar[Callable[[], scheduler_module.SyncSchedulerDrivingAPI]] = scheduler_module.Scheduler

    def run(self, entry, /, *, context: contextvars.Context | None = None):
        self._lazy_init()
        scheduler = self._scheduler
        assert scheduler is not None
        run_context = self._resolve_context(context)
        target = self._target_from_entry(entry, run_context)
        return scheduler.run_until_complete(target)

    def __enter__(self) -> "Runner":
        self._lazy_init()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()



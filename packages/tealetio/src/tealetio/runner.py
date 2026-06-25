from __future__ import annotations

import contextvars
import functools
import signal
import sys
import threading
from collections.abc import Callable
from types import FrameType
from typing import Any, ClassVar, Generic, TypeAlias, TypeVar, cast

import _tealet

from . import scheduler as scheduler_module
from . import tasks as task_module


SchedulerT = TypeVar("SchedulerT", bound=scheduler_module.CoreSchedulerDrivingAPI)
SignalHandler: TypeAlias = signal.Handlers | int | Callable[[int, FrameType | None], Any] | None

__all__ = [
    "BaseRunner",
    "Runner",
    "run",
]


class BaseRunner(Generic[SchedulerT]):
    default_factory: ClassVar[object]

    def __init__(
        self,
        *,
        scheduler_factory: Callable[[], SchedulerT] | None = None,
        context: contextvars.Context | None = None,
        debug: bool | None = None,
        handle_sigint: bool = True,
    ) -> None:
        self._scheduler: SchedulerT | None = None
        self._scheduler_factory = scheduler_factory
        self._context = context
        self._debug = debug
        self._handle_sigint = handle_sigint
        self._closed = False
        self._initialized = False
        self._previous_scheduler: scheduler_module.BaseScheduler | None = None
        self._interrupt_count = 0

    def get_scheduler(self) -> SchedulerT:
        self._lazy_init()
        scheduler = self._scheduler
        assert scheduler is not None
        return scheduler

    def _shutdown_scheduler_tasks(self, scheduler: SchedulerT) -> list[task_module.Task]:
        if not isinstance(scheduler, scheduler_module.BaseScheduler):
            return []
        tasks = list(scheduler.all_tasks())
        if not tasks:
            return []

        for task in tasks:
            if task.done():
                continue
            try:
                task.cancel()
            except _tealet.StateError:
                if not task.done():
                    raise
        return tasks

    def _finalize_close(self, scheduler: SchedulerT | None) -> None:
        self._closed = True
        if self._initialized:
            scheduler_module.set_scheduler(self._previous_scheduler)
            self._previous_scheduler = None
            self._initialized = False
        self._scheduler = None
        if scheduler is not None:
            scheduler.close()

    def _create_scheduler(self) -> SchedulerT:
        factory = self._scheduler_factory
        if factory is None:
            factory = cast(Callable[[], SchedulerT], type(self).default_factory)
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
        if isinstance(entry, task_module.Future):
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
        scheduler_module.set_scheduler(cast(scheduler_module.BaseScheduler, self._scheduler))
        self._initialized = True

    def _is_asyncio_runner_sigint_handler(self, handler: SignalHandler) -> bool:
        if not isinstance(handler, functools.partial):
            return False
        return getattr(handler.func, "__name__", None) == "_on_sigint"

    def _can_install_sigint_handler(self) -> bool:
        if not self._handle_sigint:
            return False
        if sys.version_info < (3, 11):
            return False
        if threading.current_thread() is not threading.main_thread():
            return False
        handler = signal.getsignal(signal.SIGINT)
        return handler is signal.default_int_handler or self._is_asyncio_runner_sigint_handler(handler)

    def _install_sigint_handler(
        self,
        main_task: task_module.Future[object],
        scheduler: scheduler_module.BaseScheduler,
    ) -> tuple[object, SignalHandler] | None:
        if not self._can_install_sigint_handler():
            return None
        handler = functools.partial(self._on_sigint, main_task=main_task, scheduler=scheduler)
        try:
            previous = signal.signal(signal.SIGINT, handler)
        except ValueError:
            return None
        return handler, previous

    def _restore_sigint_handler(
        self,
        installed: tuple[object, SignalHandler] | None,
    ) -> None:
        if installed is None:
            return
        handler, previous = installed
        if signal.getsignal(signal.SIGINT) is handler:
            signal.signal(signal.SIGINT, previous)

    def _on_sigint(
        self,
        signum: int,
        frame: FrameType | None,
        main_task: task_module.Future[object],
        scheduler: scheduler_module.BaseScheduler,
    ) -> None:
        self._interrupt_count += 1
        if self._interrupt_count == 1 and not main_task.done():
            scheduler.call_soon_threadsafe(main_task.cancel)
            return
        raise KeyboardInterrupt()

    def _raise_keyboard_interrupt_if_requested(self) -> None:
        if self._interrupt_count > 0:
            raise KeyboardInterrupt()


def run(
    entry,
    /,
    *,
    context: contextvars.Context | None = None,
    scheduler_factory: Callable[[], scheduler_module.SyncSchedulerDrivingAPI] | None = None,
    debug: bool | None = None,
    handle_sigint: bool = True,
):
    """Convenience helper that runs one entry under a temporary Runner."""

    runner = Runner(scheduler_factory=scheduler_factory, debug=debug, handle_sigint=handle_sigint)
    try:
        return runner.run(entry, context=context)
    finally:
        runner.close()


class Runner(BaseRunner[scheduler_module.SyncSchedulerDrivingAPI]):
    """Run scheduler-backed entries from synchronous code without asyncio."""

    default_factory = scheduler_module.Scheduler

    def close(self) -> None:
        if self._closed:
            return
        scheduler = self._scheduler
        try:
            if scheduler is not None:
                with scheduler.main_context():
                    tasks = self._shutdown_scheduler_tasks(scheduler)
                    shutdown_group = scheduler_module.gather(*tasks, return_exceptions=True)
                    scheduler.run_until_complete(shutdown_group)
                    executor_shutdown = scheduler.shutdown_default_executor()
                    scheduler.run_until_complete(executor_shutdown)
        finally:
            self._finalize_close(scheduler)

    def run(self, entry, /, *, context: contextvars.Context | None = None):
        self._lazy_init()
        scheduler = self._scheduler
        assert scheduler is not None
        run_context = self._resolve_context(context)
        with scheduler.main_context():
            target = self._target_from_entry(entry, run_context)
            sigint_handler = self._install_sigint_handler(target, cast(scheduler_module.BaseScheduler, scheduler))
            self._interrupt_count = 0
            try:
                try:
                    return scheduler.run_until_complete(target)
                except task_module.CancelledError:
                    self._raise_keyboard_interrupt_if_requested()
                    raise
            finally:
                self._restore_sigint_handler(sigint_handler)

    def __enter__(self) -> "Runner":
        self._lazy_init()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

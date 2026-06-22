from __future__ import annotations

import asyncio
import contextvars
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Callable, Generic, Protocol, TypeVar, cast

import _tealet
import tealet

from .locks import Event, InvalidStateError

if TYPE_CHECKING:
    from .scheduler import BaseScheduler


CancelledError = asyncio.CancelledError

__all__ = [
    "CancelledError",
    "DefaultTaskFactory",
    "Future",
    "Linkable",
    "Shield",
    "StubTaskFactory",
    "TaskFactory",
    "TealetTask",
    "shield",
]


class Linkable(ABC):
    """Base interface for objects that can be linked from a TealetTask."""

    @abstractmethod
    def _unlink(self, t: tealet.tealet) -> None:
        """Detach a tealet from this link target."""

    def _query_waiting(self, t: tealet.tealet) -> bool:
        return False

    def _query_runnable(self, t: tealet.tealet) -> bool:
        return False


T = TypeVar("T")


class Future(Generic[T]):
    """Minimal Future for scheduler tasks."""

    def __init__(self) -> None:
        self._done = False
        self._result: T | None = None
        self._exception: BaseException | None = None
        self._event = Event()
        self._done_callbacks: list[
            tuple[Callable[[Future[T]], object], contextvars.Context | None]
        ] = []

    # -- State ---------------------------------------------------------

    def done(self) -> bool:
        return self._done

    def cancelled(self) -> bool:
        return isinstance(self._exception, CancelledError)

    def cancel(self) -> bool:
        if self._done:
            return False
        self.set_exception(CancelledError())
        return True

    # -- Completion ----------------------------------------------------

    def set_result(self, value: T) -> None:
        if self._done:
            raise InvalidStateError("Future already done")
        self._result = value
        self._done = True
        self._event.set()
        self._run_done_callbacks()

    def set_exception(self, exc: BaseException) -> None:
        if self._done:
            raise InvalidStateError("Future already done")
        if not isinstance(exc, BaseException):
            raise TypeError("exc must be a BaseException instance")
        self._exception = exc
        self._done = True
        self._event.set()
        self._run_done_callbacks()

    # -- Done callbacks -----------------------------------------------

    def add_done_callback(
        self,
        callback: Callable[[Future[T]], object],
        *,
        context: contextvars.Context | None = None,
    ) -> None:
        if self._done:
            loop = asyncio.get_running_loop()
            if context is None:
                loop.call_soon(callback, self)
            else:
                loop.call_soon(callback, self, context=context)
            return
        if context is None:
            context = contextvars.copy_context()
        self._done_callbacks.append((callback, context))

    def remove_done_callback(self, callback: Callable[[Future[T]], object]) -> int:
        removed = 0
        kept: list[tuple[Callable[[Future[T]], object], contextvars.Context | None]] = []
        for stored_callback, context in self._done_callbacks:
            if stored_callback is callback:
                removed += 1
            else:
                kept.append((stored_callback, context))
        self._done_callbacks = kept
        return removed

    def _run_done_callbacks(self) -> None:
        callbacks = self._done_callbacks[:]
        self._done_callbacks.clear()
        for callback, context in callbacks:
            if context is None:
                callback(self)
            else:
                context.run(callback, self)

    # -- Waiting and results ------------------------------------------

    def _wait(self) -> bool:
        if self._done:
            return True

        return self._event.wait()

    def wait(self) -> T:
        try:
            self._wait()
        except CancelledError:
            from .scheduler import get_running_scheduler

            get_running_scheduler().call_soon(self.cancel)
            raise
        return self.result()

    async def _async_wait(self) -> T:
        if not self._done:
            try:
                await self._event.async_wait()
            except CancelledError:
                from .scheduler import get_running_scheduler

                get_running_scheduler().call_soon(self.cancel)
                raise
        return self.result()

    def __await__(self):
        return self._async_wait().__await__()

    def result(self) -> T:
        if not self._done:
            raise InvalidStateError("Result is not ready.")
        if self.cancelled():
            assert self._exception is not None
            raise self._exception
        if self._exception is not None:
            raise self._exception
        return cast(T, self._result)

    def exception(self) -> BaseException | None:
        if not self._done:
            raise InvalidStateError("Exception is not set.")
        if self.cancelled():
            assert self._exception is not None
            raise self._exception
        return self._exception


class Shield(Generic[T]):
    """Wait wrapper that avoids cancelling the wrapped future."""

    def __init__(self, future: Future[T]) -> None:
        self._future = future

    def wait(self) -> T:
        self._future._wait()
        return self._future.result()


def shield(future: Future[T]) -> Shield[T]:
    return Shield(future)


class TealetTask(tealet.tealet, Future[Any]):
    """Tealet task that is also a Future for its completion result."""

    def __init__(self, owning_scheduler: BaseScheduler):
        tealet.tealet.__init__(self)
        Future.__init__(self)
        self.link: Linkable | None = None
        self._scheduler: BaseScheduler = owning_scheduler

    # -- Runtime state -------------------------------------------------

    def is_waiting(self):
        if self.link is None:
            return False
        return self.link._query_waiting(self)

    def is_runnable(self):
        if self.link is None:
            return False
        return self.link._query_runnable(self)

    def is_blocked(self):
        return self._scheduler._is_blocked(self)

    def is_running(self):
        return tealet.current() is self

    def get_scheduler(self) -> BaseScheduler:
        return self._scheduler

    # -- Scheduler transfer -------------------------------------------

    def _unlink(self):
        if self.link is not None:
            self.link._unlink(self)
        self._scheduler._unlink_pending_async_wait(self)

    def run(self):
        self._scheduler._target_run(self)

    def throw(self, exc: BaseException):
        self._scheduler._target_throw(self, exc)

    def _throw_from_scheduler(self, exc: BaseException):
        super().throw(exc)

    def cancel(self) -> bool:
        if self.done():
            return False
        self.throw(CancelledError())
        return True

    # -- Target completion --------------------------------------------

    def resolve_target(self, result, exc, exc_target):
        suppress = False
        if exc is None:
            self.set_result(result)
        elif isinstance(exc, _tealet.TealetExit):
            self.set_result(None)
            suppress = True
        elif isinstance(exc, (SystemExit, KeyboardInterrupt)):
            self.set_exception(exc)
            return super().resolve_target(result, exc, exc_target)
        else:
            self.set_exception(exc)
            suppress = True
            if exc_target is not None:
                try:
                    exc_target._unlink()
                except AttributeError:
                    pass
                return exc_target, None, suppress

        # Scheduler-owned tasks always route via scheduler target selection,
        # even if task startup immediately raises before user code returns.
        return self._scheduler._find_target(task_exit=True), None, suppress


class TaskFactory(Protocol):
    """Callable strategy for creating scheduler-owned tasks."""

    def __call__(
        self,
        scheduler: BaseScheduler,
        func: Callable[[], object],
        *,
        context: contextvars.Context,
        eager_start: bool | None = None,
    ) -> TealetTask:
        """Create and prepare a task without scheduling it."""
        ...


def _prepare_task(task: TealetTask, func: Callable[[], object], context: contextvars.Context) -> None:
    def task_main(current: tealet.tealet, _arg: object):
        return context.run(func)

    task.prepare(task_main)


def _should_start_eager(scheduler: BaseScheduler, default: bool, override: bool | None) -> bool:
    eager_start = default if override is None else override
    return eager_start and scheduler.is_running()


class DefaultTaskFactory:
    """Default task factory using direct tealet preparation."""

    def __init__(self, *, eager_start: bool = False) -> None:
        self.eager_start = bool(eager_start)

    def __call__(
        self,
        scheduler: BaseScheduler,
        func: Callable[[], object],
        *,
        context: contextvars.Context,
        eager_start: bool | None = None,
    ) -> TealetTask:
        task = TealetTask(scheduler)
        _prepare_task(task, func, context)
        if _should_start_eager(scheduler, self.eager_start, eager_start):
            task.run()
        return task


class StubTaskFactory:
    """Task factory that prepares tasks from a reusable tealet stub."""

    def __init__(self, stub: tealet.tealet | None = None, *, eager_start: bool = False) -> None:
        self._stub = stub
        self.eager_start = bool(eager_start)

    @property
    def stub(self) -> tealet.tealet | None:
        return self._stub

    def stub_here(self) -> tealet.tealet:
        stub = tealet.tealet()
        stub.stub()
        self._stub = stub
        return stub

    def __call__(
        self,
        scheduler: BaseScheduler,
        func: Callable[[], object],
        *,
        context: contextvars.Context,
        eager_start: bool | None = None,
    ) -> TealetTask:
        stub = self._stub
        if stub is None:
            stub = self.stub_here()
        task = TealetTask(scheduler)
        task.set_stub(stub)
        _prepare_task(task, func, context)
        if _should_start_eager(scheduler, self.eager_start, eager_start):
            task.run()
        return task
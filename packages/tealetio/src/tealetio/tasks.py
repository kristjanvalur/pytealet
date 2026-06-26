from __future__ import annotations

import asyncio
import contextvars
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Callable, Generic, Protocol, TypeVar, cast

import _tealet
import tealet

from .locks import Event, InvalidStateError

if TYPE_CHECKING:
    from .scheduler import BaseScheduler
    from .locks import PriorityLock


TASK_PRIORITY_CRITICAL = -20.0
TASK_PRIORITY_HIGH = -10.0
TASK_PRIORITY_DEFAULT = 0.0
TASK_PRIORITY_LOW = 10.0
TASK_PRIORITY_IDLE = 20.0
TEALET_PRI_INF = float("inf")

CancelledError = asyncio.CancelledError

__all__ = [
    "CancelledError",
    "DefaultTaskFactory",
    "Future",
    "get_current",
    "Linkable",
    "PriorityTask",
    "TaskLink",
    "Shield",
    "StubTaskFactory",
    "TaskConstructor",
    "TaskFactory",
    "TASK_PRIORITY_CRITICAL",
    "TASK_PRIORITY_DEFAULT",
    "TASK_PRIORITY_HIGH",
    "TASK_PRIORITY_IDLE",
    "TASK_PRIORITY_LOW",
    "Task",
    "shield",
]


class TaskLink(ABC):
    """Base interface for objects that can be linked from a Task."""

    @abstractmethod
    def _unlink(self, t: tealet.tealet) -> None:
        """Detach a tealet from this link target."""

    def _query_waiting(self) -> bool:
        return False

    def _query_runnable(self) -> bool:
        return False

    def on_modified(self, task: tealet.tealet) -> None:
        """Handle a linked tealet changing its scheduling related state."""


Linkable = TaskLink


T = TypeVar("T")
TaskConstructor = Callable[..., "Task"]


class _SchedulerTealetFactory:
    def __init__(self, scheduler: BaseScheduler, task_constructor: Callable[[Any], _tealet.tealet]) -> None:
        self.scheduler = scheduler
        self._task_constructor = task_constructor

    def __call__(self) -> _tealet.tealet:
        return self._task_constructor(self.scheduler)


@contextmanager
def scheduler_tealet_factory(scheduler: BaseScheduler):
    """Use `scheduler` to construct low-level tealets in this context."""

    current_factory = _tealet.get_tealet_factory()
    if isinstance(current_factory, _SchedulerTealetFactory) and current_factory.scheduler is scheduler:
        yield
        return

    task_constructor = cast(Callable[[Any], _tealet.tealet], scheduler.get_task_factory().task_constructor)
    previous_factory = current_factory
    _tealet.set_tealet_factory(_SchedulerTealetFactory(scheduler, task_constructor))
    try:
        yield
    finally:
        _tealet.set_tealet_factory(previous_factory)


@contextmanager
def task_priority(task: tealet.tealet, priority: float):
    """Temporarily assign `priority` to a task that supports priorities."""

    try:
        previous_priority = cast(Any, task).priority
    except AttributeError:
        yield
        return

    cast(Any, task).priority = priority
    try:
        yield
    finally:
        cast(Any, task).priority = previous_priority


class Future(Generic[T]):
    """Minimal Future for scheduler tasks."""

    def __init__(self) -> None:
        self._done = False
        self._result: T | None = None
        self._exception: BaseException | None = None
        self._event = Event()
        self._done_callbacks: list[tuple[Callable[[Future[T]], object], contextvars.Context | None]] = []

    # -- State ---------------------------------------------------------

    def done(self) -> bool:
        """Return True if the Future has a result or exception."""

        return self._done

    def cancelled(self) -> bool:
        """Return True if the Future completed by cancellation."""

        return isinstance(self._exception, CancelledError)

    def cancel(self) -> bool:
        """Cancel the Future if it has not completed yet."""

        if self._done:
            return False
        self.set_exception(CancelledError())
        return True

    # -- Completion ----------------------------------------------------

    def set_result(self, value: T) -> None:
        """Complete the Future successfully with `value`."""

        if self._done:
            raise InvalidStateError("Future already done")
        self._result = value
        self._done = True
        self._event.set()
        self._run_done_callbacks()

    def set_exception(self, exc: BaseException) -> None:
        """Complete the Future by raising `exc` to waiters."""

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
        """Register `callback` to run when the Future completes."""

        if self._done:
            if context is None:
                context = contextvars.copy_context()
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                context.run(callback, self)
            else:
                loop.call_soon(callback, self, context=context)
            return
        if context is None:
            context = contextvars.copy_context()
        self._done_callbacks.append((callback, context))

    def remove_done_callback(self, callback: Callable[[Future[T]], object]) -> int:
        """Remove matching done callbacks and return the number removed."""

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

        return self._event.swait()

    def wait(self) -> T:
        """Block the current scheduler task until the Future completes."""

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
                await self._event.wait()
            except CancelledError:
                from .scheduler import get_running_scheduler

                get_running_scheduler().call_soon(self.cancel)
                raise
        return self.result()

    def __await__(self):
        return self._async_wait().__await__()

    def result(self) -> T:
        """Return the Future result or raise its completion exception."""

        if not self._done:
            raise InvalidStateError("Result is not ready.")
        if self.cancelled():
            assert self._exception is not None
            raise self._exception
        if self._exception is not None:
            raise self._exception
        return cast(T, self._result)

    def exception(self) -> BaseException | None:
        """Return the Future exception, or None for successful completion."""

        if not self._done:
            raise InvalidStateError("Exception is not set.")
        if self.cancelled():
            assert self._exception is not None
            raise self._exception
        return self._exception


class Shield(Future[T]):
    """Future wrapper whose cancellation does not cancel the wrapped future."""

    def __init__(self, future: Future[T]) -> None:
        super().__init__()
        self._future = future
        if future.done():
            self._complete_from_child(future)
        else:
            future.add_done_callback(self._complete_from_child)

    def _complete_from_child(self, future: Future[T]) -> None:
        if self.done():
            return
        try:
            self.set_result(future.result())
        except BaseException as exc:
            self.set_exception(exc)

    def wait(self) -> T:
        """Block until the shielded Future completes."""

        return super().wait()


def shield(future: Future[T]) -> Shield[T]:
    """Return a Future wrapper that protects `future` from cancellation."""

    return Shield(future)


class Task(tealet.tealet, Future[Any]):
    """Tealet task that is also a Future for its completion result."""

    def __init__(self, owning_scheduler: BaseScheduler):
        tealet.tealet.__init__(self)
        Future.__init__(self)
        self.link: TaskLink | None = None
        self._scheduler: BaseScheduler = owning_scheduler

    # -- Runtime state -------------------------------------------------

    def is_waiting(self):
        """Return True if this task is linked to a waiting primitive."""

        if self.link is None:
            return False
        return self.link._query_waiting()

    def is_runnable(self):
        """Return True if this task is currently runnable."""

        if self.link is None:
            return False
        return self.link._query_runnable()

    def is_blocked(self):
        """Return True if this task is blocked on an asyncio awaitable."""

        return self._scheduler._is_blocked(self)

    def is_running(self):
        """Return True if this task is the current low-level tealet."""

        return tealet.current() is self

    def get_scheduler(self) -> BaseScheduler:
        """Return the scheduler that owns this task."""

        return self._scheduler

    def modified(self) -> None:
        """Notify the current link that task state used for scheduling changed."""
        if self.link is not None:
            self.link.on_modified(self)

    # -- Scheduler transfer -------------------------------------------

    def _unlink(self):
        if self.link is not None:
            self.link._unlink(self)
        self._scheduler._unlink_pending_async_wait(self)

    def run(self):
        """Transfer execution to this task from its owning scheduler."""

        self._scheduler._target_run(self)

    def throw(self, exc: BaseException):
        """Throw `exc` into this task from its owning scheduler."""

        self._scheduler._target_throw(self, exc)

    def _throw_from_scheduler(self, exc: BaseException):
        super().throw(exc)

    def cancel(self) -> bool:
        """Request cancellation by throwing `CancelledError` into the task."""

        if self.done():
            return False
        self.throw(CancelledError())
        return True

    # -- Target completion --------------------------------------------

    def resolve_target(self, result, exc, exc_target):
        """Resolve where control goes when this task exits."""

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


class PriorityTask(Task):
    """Tealet task with a scheduler priority value."""

    def __init__(
        self,
        owning_scheduler: BaseScheduler,
        priority: float = TASK_PRIORITY_DEFAULT,
    ):
        super().__init__(owning_scheduler)
        self._priority = TASK_PRIORITY_DEFAULT
        self._owned_priority_locks: set[PriorityLock] = set()
        self._waiting_on_priority: Any | None = None
        self.priority = priority

    @property
    def priority(self) -> float:
        """Return this task's base scheduling priority."""

        return self._priority

    @priority.setter
    def priority(self, value: float) -> None:
        """Set this task's base scheduling priority."""

        self._priority = float(value)
        self.modified()

    def add_owned_priority_lock(self, lock: PriorityLock) -> None:
        """Track a priority lock currently owned by this task."""

        self._owned_priority_locks.add(lock)

    def remove_owned_priority_lock(self, lock: PriorityLock) -> None:
        """Stop tracking a priority lock released by this task."""

        self._owned_priority_locks.remove(lock)

    def set_waiting_on_priority(self, target: Any | None) -> None:
        """Set the priority-inheritance target this task is waiting on."""

        self._waiting_on_priority = target

    def get_effective_priority(self) -> float:
        """Return the priority after applying inherited lock priority."""

        inherited = min(
            (
                priority
                for lock in self._owned_priority_locks
                if (priority := lock.get_effective_priority()) is not None
            ),
            default=None,
        )
        if inherited is None:
            return self.priority
        return min(self.priority, inherited)

    def _propagate_priority(self, source: Any) -> None:
        del source
        if self.is_runnable():
            self.modified()
        elif self._waiting_on_priority is not None:
            self._waiting_on_priority._propagate_priority(self)


# marks scheduler-owned tealet task code while it is on the Python stack. It is
# cleared when control is handed to asyncio, including nested asyncio loop hosts.
_current_task: contextvars.ContextVar[Task | None] = contextvars.ContextVar(
    "tealetio_current_task",
    default=None,
)


def get_current() -> Task | None:
    """Return the current scheduler-owned tealet task, if one is running."""
    task = _current_task.get()
    if task is None or tealet.current() is not task:
        return None
    return task


def _copy_context_without_current_task(context: contextvars.Context | None = None) -> contextvars.Context:
    if context is None:
        context = contextvars.copy_context()
    else:
        context = context.copy()
    context.run(_current_task.set, None)
    return context


@contextmanager
def _without_current_task():
    token = _current_task.set(None)
    try:
        yield
    finally:
        _current_task.reset(token)


class TaskFactory(Protocol):
    """Callable strategy for creating scheduler-owned tasks."""

    @property
    def task_constructor(self) -> TaskConstructor:
        """Return the concrete tealet wrapper constructor used by this factory."""
        ...

    def __call__(
        self,
        scheduler: BaseScheduler,
        func: Callable[[], object],
        *,
        context: contextvars.Context,
        eager_start: bool | None = None,
        **kwargs: Any,
    ) -> Task:
        """Create and prepare a task without scheduling it."""
        ...


def _prepare_task(task: Task, func: Callable[[], object], context: contextvars.Context) -> None:
    def task_main(current: tealet.tealet, _arg: object):
        def run_func():
            token = _current_task.set(task)
            try:
                return func()
            finally:
                _current_task.reset(token)

        return context.run(run_func)

    task.prepare(task_main)


def _should_start_eager(scheduler: BaseScheduler, default: bool, override: bool | None) -> bool:
    eager_start = default if override is None else override
    return eager_start and scheduler.is_running()


class DefaultTaskFactory:
    """Default task factory using direct tealet preparation."""

    def __init__(
        self,
        *,
        task_constructor: TaskConstructor = Task,
        eager_start: bool = False,
    ) -> None:
        self.task_constructor = task_constructor
        self.eager_start = bool(eager_start)

    def __call__(
        self,
        scheduler: BaseScheduler,
        func: Callable[[], object],
        *,
        context: contextvars.Context,
        eager_start: bool | None = None,
        **kwargs: Any,
    ) -> Task:
        task = self.task_constructor(scheduler, **kwargs)
        _prepare_task(task, func, context)
        if _should_start_eager(scheduler, self.eager_start, eager_start):
            task.run()
        return task


class StubTaskFactory:
    """Task factory that prepares tasks from a reusable tealet stub."""

    def __init__(
        self,
        stub: tealet.tealet | None = None,
        *,
        task_constructor: TaskConstructor = Task,
        eager_start: bool = False,
    ) -> None:
        self._stub = stub
        self.task_constructor = task_constructor
        self.eager_start = bool(eager_start)

    @property
    def stub(self) -> tealet.tealet | None:
        """Return the reusable stub tealet, if one has been created."""

        return self._stub

    def stub_here(self) -> tealet.tealet:
        """Create and store a reusable tealet stub in the current place."""

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
        **kwargs: Any,
    ) -> Task:
        stub = self._stub
        if stub is None:
            stub = self.stub_here()
        task = self.task_constructor(scheduler, **kwargs)
        task.set_stub(stub)
        _prepare_task(task, func, context)
        if _should_start_eager(scheduler, self.eager_start, eager_start):
            task.run()
        return task

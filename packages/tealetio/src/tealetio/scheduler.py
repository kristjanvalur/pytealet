from __future__ import annotations

import asyncio
import concurrent.futures
import contextvars
import functools
import heapq
import importlib
import inspect
import itertools
import logging
import queue
import socket
import threading
import time
import warnings
import weakref
from abc import ABC, abstractmethod
from collections import deque
from collections.abc import Iterable, Iterator
from contextlib import nullcontext
from typing import (
    Any,
    Callable,
    ContextManager,
    Coroutine,
    Literal,
    Protocol,
    TypeAlias,
    TypeVar,
    cast,
)

from asynkit import coro_drive as _coro_drive
from asynkit import syncmethod as _syncmethod
import tealet

from .locks import (
    Event,
    Queue,
    QueueShutDown,
    RawTimeoutError,
    TimeoutError,
    set_scheduler_resolver,
    timeout as scheduler_timeout,
)
from .operations import ContinuousOperation
from . import tasks as _tasks


T = TypeVar("T")

logger = logging.getLogger(__name__)

DEFAULT_EXECUTOR_SHUTDOWN_TIMEOUT = 300.0


__all__ = [
    "ALL_COMPLETED",
    "AsyncSchedulerDrivingAPI",
    "AsyncDrivingMixin",
    "BaseScheduler",
    "BaseDrivingMixin",
    "BasicScheduler",
    "Channel",
    "CoreSchedulerDrivingAPI",
    "DEFAULT_EXECUTOR_SHUTDOWN_TIMEOUT",
    "DeadlockError",
    "FIRST_COMPLETED",
    "FIRST_EXCEPTION",
    "FifoRunnableQueue",
    "Scheduler",
    "PrescheduledRunnableQueue",
    "PriorityRunnableQueue",
    "RunnableQueue",
    "RunnableQueueFactory",
    "SyncDrivingMixin",
    "SyncSchedulerDrivingAPI",
    "TimerHandle",
    "as_completed",
    "await_",
    "create_task",
    "ensure_future",
    "gather",
    "getaddrinfo",
    "getnameinfo",
    "ensure_resolved",
    "get_scheduler",
    "get_running_scheduler",
    "set_scheduler",
    "sleep",
    "spawn",
    "to_thread",
    "wait",
    "wait_for",
]

FIRST_COMPLETED = "FIRST_COMPLETED"
FIRST_EXCEPTION = "FIRST_EXCEPTION"
ALL_COMPLETED = "ALL_COMPLETED"
_ReturnWhen: TypeAlias = Literal["FIRST_COMPLETED", "FIRST_EXCEPTION", "ALL_COMPLETED"]
_TimeFunction: TypeAlias = Callable[[], float]


# a thread local scheduler
_scheduler = threading.local()


class FifoRunnableQueue(_tasks.TaskLink):
    """FIFO runnable task storage and TaskLink owner for runnable tealets."""

    def __init__(self) -> None:
        self._items: deque[tealet.tealet] = deque()
        self._set: set[tealet.tealet] = set()

    def __bool__(self) -> bool:
        return bool(self._items)

    def __len__(self) -> int:
        return len(self._items)

    def __contains__(self, task: tealet.tealet) -> bool:
        return task in self._set

    def add(self, task: tealet.tealet) -> bool:
        if task in self._set:
            return False
        self._items.append(task)
        self._set.add(task)
        task.link = self
        return True

    def discard(self, task: tealet.tealet) -> bool:
        if task not in self._set:
            return False
        self._set.remove(task)
        try:
            self._items.remove(task)
        except ValueError:
            pass
        task.link = None
        return True

    def _unlink(self, t: tealet.tealet) -> None:
        self.discard(t)

    def _query_waiting(self) -> bool:
        return False

    def _query_runnable(self) -> bool:
        return True

    def pop_next(self) -> tealet.tealet:
        task = self._items.popleft()
        self._set.discard(task)
        return task

    def tasks(self) -> tuple[_tasks.Task, ...]:
        return cast(tuple[_tasks.Task, ...], tuple(self._items))

    def _normalise_insert_position(self, position: int, length: int) -> int:
        # match list/deque insertion semantics, with -1 meaning append.
        if position < 0:
            return max(length + position + 1, 0)
        return position

    def reschedule(self, task: tealet.tealet, position: int | None) -> None:
        if task not in self._set:
            raise ValueError("task is not runnable")
        self._items.remove(task)
        if position is None:
            self._items.append(task)
        else:
            self._items.insert(self._normalise_insert_position(position, len(self._items)), task)

    def _insert_after_first(self, task: tealet.tealet, position: int) -> None:
        if task not in self._set:
            raise ValueError("task is not runnable")
        self._items.remove(task)
        assert self._items
        index = self._normalise_insert_position(position, len(self._items) - 1) + 1
        self._items.insert(index, task)

    def yield_to(self, target: tealet.tealet, current: tealet.tealet, insert_current_at: int | None) -> None:
        self.reschedule(target, 0)
        self.add(current)
        if insert_current_at is not None:
            self._insert_after_first(current, insert_current_at)


class PrescheduledRunnableQueue(FifoRunnableQueue):
    """Runnable queue with an immediate lane ahead of the normal FIFO policy."""

    def __init__(self) -> None:
        super().__init__()
        self._prescheduled: deque[tealet.tealet] = deque()
        self._prescheduled_set: set[tealet.tealet] = set()

    def __bool__(self) -> bool:
        return bool(self._prescheduled or self._items)

    def __len__(self) -> int:
        return len(self._prescheduled) + len(self._items)

    def __contains__(self, task: tealet.tealet) -> bool:
        return task in self._prescheduled_set or super().__contains__(task)

    def discard(self, task: tealet.tealet) -> bool:
        if task in self._prescheduled_set:
            self._prescheduled_set.remove(task)
            try:
                self._prescheduled.remove(task)
            except ValueError:
                pass
            task.link = None
            return True
        return super().discard(task)

    def pop_next(self) -> tealet.tealet:
        if self._prescheduled:
            task = self._prescheduled.popleft()
            self._prescheduled_set.discard(task)
            return task
        return super().pop_next()

    def tasks(self) -> tuple[_tasks.Task, ...]:
        return cast(tuple[_tasks.Task, ...], (*self._prescheduled, *self._items))

    def _remove_without_unlink(self, task: tealet.tealet) -> None:
        # queue moves keep the task linked to this queue, so do not clear link.
        if task in self._prescheduled_set:
            self._prescheduled_set.remove(task)
            self._prescheduled.remove(task)
        elif task in self._set:
            self._set.remove(task)
            self._items.remove(task)
        else:
            raise ValueError("task is not runnable")

    def _insert_prescheduled(self, task: tealet.tealet, position: int) -> None:
        self._prescheduled.insert(position, task)
        self._prescheduled_set.add(task)
        task.link = self

    def _insert_normal(self, task: tealet.tealet, position: int) -> None:
        self._items.insert(position, task)
        self._set.add(task)
        task.link = self

    def reschedule(self, task: tealet.tealet, position: int | None) -> None:
        self._remove_without_unlink(task)
        if position is None:
            self._insert_normal(task, len(self._items))
            return
        # explicit positions address the immediate lane only; normal policy
        # queues, including future priority queues, may not be indexable.
        self._insert_prescheduled(task, self._normalise_insert_position(position, len(self._prescheduled)))

    def _insert_after_first(self, task: tealet.tealet, position: int) -> None:
        assert task not in self
        assert self._prescheduled
        # explicit yield_to() positions address only the immediate lane after
        # the target at index 0; normal runnable policy is not part of this index.
        index = self._normalise_insert_position(position, len(self._prescheduled) - 1) + 1
        self._insert_prescheduled(task, index)

    def yield_to(self, target: tealet.tealet, current: tealet.tealet, insert_current_at: int | None) -> None:
        # target is forced into the immediate lane; current remains runnable and
        # either follows normal policy or is inserted into the immediate lane.
        self._remove_without_unlink(target)
        self._insert_prescheduled(target, 0)
        if current in self:
            self._remove_without_unlink(current)
        if insert_current_at is None:
            self.add(current)
        else:
            self._insert_after_first(current, insert_current_at)


class PriorityRunnableQueue(PrescheduledRunnableQueue):
    """Runnable queue with an immediate lane ahead of stable priority policy."""

    def __init__(self) -> None:
        super().__init__()
        self._priority_items: list[tuple[Any, int, tealet.tealet]] = []
        self._priority_sequence = itertools.count()

    def __bool__(self) -> bool:
        return bool(self._prescheduled or self._priority_items)

    def __len__(self) -> int:
        return len(self._prescheduled) + len(self._priority_items)

    def add(self, task: tealet.tealet) -> bool:
        if task in self._set or task in self._prescheduled_set:
            return False
        self._insert_normal(task, len(self._priority_items))
        return True

    def discard(self, task: tealet.tealet) -> bool:
        if task in self._prescheduled_set:
            return super().discard(task)
        if task not in self._set:
            return False
        self._remove_normal(task)
        task.link = None
        return True

    def pop_next(self) -> tealet.tealet:
        if self._prescheduled:
            return super().pop_next()
        _, _, task = heapq.heappop(self._priority_items)
        self._set.discard(task)
        return task

    def tasks(self) -> tuple[_tasks.Task, ...]:
        return cast(
            tuple[_tasks.Task, ...],
            (*self._prescheduled, *(entry[2] for entry in sorted(self._priority_items))),
        )

    def _active_priority(self, task: tealet.tealet) -> Any:
        assert isinstance(task, _tasks.Task)
        try:
            return cast(Any, task).get_effective_priority()
        except AttributeError:
            return _tasks.TASK_PRIORITY_DEFAULT

    def _priority_entry(self, task: tealet.tealet) -> tuple[Any, int, tealet.tealet]:
        return (self._active_priority(task), next(self._priority_sequence), task)

    def _remove_normal(self, task: tealet.tealet) -> None:
        self._set.remove(task)
        for index, entry in enumerate(self._priority_items):
            if entry[2] is task:
                del self._priority_items[index]
                heapq.heapify(self._priority_items)
                return
        raise ValueError("task is not runnable")

    def _remove_without_unlink(self, task: tealet.tealet) -> None:
        if task in self._prescheduled_set:
            self._prescheduled_set.remove(task)
            self._prescheduled.remove(task)
        elif task in self._set:
            self._remove_normal(task)
        else:
            raise ValueError("task is not runnable")

    def _insert_normal(self, task: tealet.tealet, position: int) -> None:
        del position
        heapq.heappush(self._priority_items, self._priority_entry(task))
        self._set.add(task)
        task.link = self

    def on_modified(self, task: tealet.tealet) -> None:
        if task in self._prescheduled_set:
            return
        if task not in self._set:
            return
        self._remove_normal(task)
        self._insert_normal(task, len(self._priority_items))


class RunnableQueue(Protocol):
    """Scheduler-facing interface for runnable queue implementations."""

    def __bool__(self) -> bool: ...

    def __len__(self) -> int: ...

    def __contains__(self, task: tealet.tealet) -> bool: ...

    def add(self, task: tealet.tealet) -> bool: ...

    def discard(self, task: tealet.tealet) -> bool: ...

    def pop_next(self) -> tealet.tealet: ...

    def tasks(self) -> tuple[_tasks.Task, ...]: ...

    def reschedule(self, task: tealet.tealet, position: int | None) -> None: ...

    def yield_to(self, target: tealet.tealet, current: tealet.tealet, insert_current_at: int | None) -> None: ...


RunnableQueueFactory: TypeAlias = Callable[[], RunnableQueue]


class CoreSchedulerDrivingAPI(ABC):
    """Common control and driving surface shared by scheduler drivers."""

    @abstractmethod
    def is_running(self) -> bool:
        """Return whether this scheduler is currently driving."""

    @abstractmethod
    def set_debug(self, enabled: bool) -> None:
        """Set scheduler debug mode flag."""

    @abstractmethod
    def get_debug(self) -> bool:
        """Return scheduler debug mode flag."""

    @abstractmethod
    def close(self) -> None:
        """Release scheduler-owned resources."""

    @abstractmethod
    def main_context(self) -> ContextManager[None]:
        """Use this scheduler's task factory for the current main tealet wrapper."""

    @abstractmethod
    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Bind this scheduler to an asyncio event loop clock."""

    @abstractmethod
    def shutdown_default_executor(
        self,
        timeout: float | None = DEFAULT_EXECUTOR_SHUTDOWN_TIMEOUT,
    ) -> "_tasks.Future[Any]":
        """Return a future that completes after the default executor shuts down."""

    @abstractmethod
    def spawn(
        self,
        func: Callable[[], T],
        *,
        context: contextvars.Context | None = None,
        eager_start: bool | None = None,
        **kwargs: Any,
    ) -> "_tasks.Task":
        """Spawn a scheduler-managed task from a zero-arg callable."""

    @abstractmethod
    def stop(self) -> None:
        """Stop a currently running driver."""

    @abstractmethod
    def run(self, *, yield_every: int | None = None) -> None:
        """Run scheduler work synchronously until idle."""

    @abstractmethod
    def run_forever(self, *, yield_every: int | None = None) -> None:
        """Run scheduler work synchronously until stop() is called."""

    @abstractmethod
    def run_until_complete(
        self,
        future: "_tasks.Future[T] | Callable[[], T]",
        *,
        yield_every: int | None = None,
    ) -> T:
        """Run scheduler work synchronously until a target completes."""

    @abstractmethod
    async def arun(self, *, yield_every: int | None = None) -> None:
        """Run scheduler work asynchronously until idle."""

    @abstractmethod
    async def arun_forever(self, *, yield_every: int | None = None) -> None:
        """Run scheduler work asynchronously until stop() is called."""

    @abstractmethod
    async def arun_until_complete(
        self,
        future: "_tasks.Future[T] | Callable[[], T]",
        *,
        yield_every: int | None = None,
    ) -> T:
        """Run scheduler work asynchronously until a target completes."""


class SyncSchedulerDrivingAPI(CoreSchedulerDrivingAPI, ABC):
    """Synchronous scheduler driver API.

    This class is retained as a descriptive compatibility alias for callers
    that specifically require the synchronous convenience methods.
    """


class AsyncSchedulerDrivingAPI(CoreSchedulerDrivingAPI, ABC):
    """Asyncio-hosted scheduler driver API."""


class BaseDrivingMixin:
    """Generic driver loop for cooperative schedulers."""

    async def _driver_wait(self) -> None:
        raise NotImplementedError

    async def _driver_yield(self) -> None:
        return None

    def _before_arun(self) -> None:
        pass

    def _arun_should_terminate(self) -> bool:
        scheduler = cast(Any, self)
        return not (
            scheduler._has_runnable_work()
            or scheduler._has_pending_timers()
            or scheduler._pending_async_waits
            or scheduler._has_pending_driver_work()
        )

    @staticmethod
    def _validate_yield_every(yield_every: int | None) -> None:
        if yield_every is not None and yield_every <= 0:
            raise ValueError("yield_every must be > 0 or None")

    async def arun(self, *, yield_every: int | None = None) -> None:
        """Run scheduler work until idle."""

        scheduler = cast(Any, self)
        scheduler._verify_current_scheduler()
        self._validate_yield_every(yield_every)
        with scheduler.main_context(), _tasks.task_priority(tealet.current(), _tasks.TEALET_PRI_INF):
            self._before_arun()
            scheduler._running = True
            scheduler._owner_thread = threading.get_ident()
            try:
                while not self._arun_should_terminate():
                    scheduler._run_ready_batch(yield_every)
                    if yield_every is not None and scheduler._has_runnable_work():
                        await self._driver_yield()
                        continue
                    if not self._arun_should_terminate():
                        await self._driver_wait()
            finally:
                scheduler._owner_thread = None
                scheduler._running = False

    async def arun_forever(self, *, yield_every: int | None = None) -> None:
        """Run scheduler work until `stop()` is called."""

        scheduler = cast(Any, self)
        scheduler._verify_current_scheduler()
        self._validate_yield_every(yield_every)
        with scheduler.main_context(), _tasks.task_priority(tealet.current(), _tasks.TEALET_PRI_INF):
            self._before_arun()
            scheduler._stopping = False
            scheduler._running = True
            scheduler._owner_thread = threading.get_ident()
            try:
                while not scheduler._stopping:
                    scheduler._run_ready_batch(yield_every)
                    if not scheduler._stopping and scheduler._has_runnable_work():
                        await self._driver_yield()
                        continue
                    if not scheduler._stopping:
                        await self._driver_wait()
            finally:
                scheduler._owner_thread = None
                scheduler._running = False
                scheduler._stopping = False

    async def arun_until_complete(
        self,
        future: _tasks.Future[T] | Callable[[], T],
        *,
        yield_every: int | None = None,
    ) -> T:
        """Run scheduler work until `future` completes and return its result."""

        scheduler = cast(Any, self)
        scheduler._verify_current_scheduler()
        self._validate_yield_every(yield_every)
        with scheduler.main_context(), _tasks.task_priority(tealet.current(), _tasks.TEALET_PRI_INF):
            if isinstance(future, _tasks.Future):
                target: _tasks.Future[T] = future
                if isinstance(target, _tasks.Task) and target.get_scheduler() is not self:
                    raise RuntimeError("Future is bound to a different scheduler")
            elif callable(future):
                target = scheduler.spawn(future)
            else:
                raise TypeError("future must be a Future or callable")

            self._before_arun()
            scheduler._stopping = False
            scheduler._running = True
            scheduler._owner_thread = threading.get_ident()
            try:
                while not target.done() and not scheduler._stopping:
                    scheduler._run_ready_batch(yield_every)
                    if not target.done() and not scheduler._stopping and scheduler._has_runnable_work():
                        await self._driver_yield()
                        continue
                    if not target.done() and not scheduler._stopping:
                        await self._driver_wait()
            finally:
                scheduler._owner_thread = None
                scheduler._running = False
                scheduler._stopping = False

        if not target.done():
            raise RuntimeError("Scheduler stopped before Future completed.")
        return target.result()


class SyncDrivingMixin(BaseDrivingMixin):
    """Blocking driver facade built from the async-shaped driving loop."""

    run = _syncmethod(BaseDrivingMixin.arun)
    run_forever = _syncmethod(BaseDrivingMixin.arun_forever)
    run_until_complete = _syncmethod(BaseDrivingMixin.arun_until_complete)


class AsyncDrivingMixin(BaseDrivingMixin):
    """Async-hosted driver facade that rejects blocking entry points."""

    def run(self, *, yield_every: int | None = None) -> None:
        """Raise because async-hosted schedulers must be driven from an async task."""

        raise NotImplementedError(f"{type(self).__name__} does not support run(); use arun()")

    def run_forever(self, *, yield_every: int | None = None) -> None:
        """Raise because async-hosted schedulers must be driven from an async task."""

        raise NotImplementedError(f"{type(self).__name__} does not support run_forever(); use arun_forever()")

    def run_until_complete(
        self,
        future: _tasks.Future[T] | Callable[[], T],
        *,
        yield_every: int | None = None,
    ) -> T:
        """Raise because async-hosted schedulers must be driven from an async task."""

        raise NotImplementedError(
            f"{type(self).__name__} does not support run_until_complete(); use arun_until_complete()"
        )


def set_scheduler(value: "BaseScheduler | None") -> None:
    """Bind or clear the current scheduler for this thread."""

    if value is None:
        if hasattr(_scheduler, "instance"):
            del _scheduler.instance
        return
    _scheduler.instance = value


def get_running_scheduler() -> "BaseScheduler":
    """Return the current scheduler while it is actively driving work."""

    current = _current_scheduler()
    if current is not None and current.is_running():
        return current
    raise RuntimeError("no running scheduler")


set_scheduler_resolver(get_running_scheduler)


def _current_scheduler() -> "BaseScheduler | None":
    return getattr(_scheduler, "instance", None)


def get_scheduler() -> "BaseScheduler":
    """Return the currently bound scheduler, whether or not it is running."""

    current = _current_scheduler()
    if current is None:
        raise RuntimeError("no current scheduler")
    return current


def to_thread(func: Callable[..., T], /, *args: object, **kwargs: object) -> T:
    """Run a callable in the scheduler default thread pool and wait for its result."""

    context = contextvars.copy_context()
    call = functools.partial(context.run, func, *args, **kwargs)
    return get_running_scheduler().run_in_executor(None, call).wait()


def getaddrinfo(
    host: str | bytes | None,
    port: str | int | bytes | None,
    *,
    family: int = 0,
    type: int = 0,
    proto: int = 0,
    flags: int = 0,
) -> list[tuple[int, int, int, str, tuple[Any, ...]]]:
    """Resolve ``host``/``port`` on a worker thread and wait from the current tealet."""

    return get_running_scheduler().getaddrinfo(
        host,
        port,
        family=family,
        type=type,
        proto=proto,
        flags=flags,
    )


def getnameinfo(sockaddr: tuple[Any, ...], flags: int = 0) -> tuple[str, str]:
    """Reverse-resolve ``sockaddr`` on a worker thread and wait from the current tealet."""

    return get_running_scheduler().getnameinfo(sockaddr, flags=flags)


def ensure_resolved(
    address: tuple[Any, ...],
    *,
    family: int = 0,
    type: int = socket.SOCK_STREAM,
    proto: int = 0,
    flags: int = 0,
) -> list[tuple[int, int, int, str, tuple[Any, ...]]]:
    """Resolve ``address`` like asyncio ``loop._ensure_resolved``."""

    from . import dns

    return dns.ensure_resolved(
        get_running_scheduler(),
        address,
        family=family,
        type=type,
        proto=proto,
        flags=flags,
    )


def sleep(delay: float) -> None:
    """Suspend the current task for `delay` seconds.

    `sleep(0)` is a cooperative yield point, matching the conventional
    `asyncio.sleep(0)` pattern.
    """

    get_running_scheduler().sleep(delay)


def await_(awaitable: Any) -> Any:
    """Await an asyncio awaitable from the current scheduler task."""

    return get_running_scheduler().await_(awaitable)


def spawn(
    func: Callable[[], T],
    *,
    context: contextvars.Context | None = None,
    eager_start: bool | None = None,
    **kwargs: Any,
) -> "_tasks.Task":
    """Spawn a task on the current scheduler from a zero-argument callable."""

    return get_scheduler().spawn(func, context=context, eager_start=eager_start, **kwargs)


def create_task(
    func: Callable[[], T],
    *,
    context: contextvars.Context | None = None,
    eager_start: bool | None = None,
    **kwargs: Any,
) -> "_tasks.Task":
    """Create a task on the current scheduler using asyncio-style naming."""

    return spawn(func, context=context, eager_start=eager_start, **kwargs)


class Channel(_tasks.TaskLink):
    """Rendezvous channel for sync tealet operations and optional async waits."""

    # Operation model:
    # - Unbuffered rendezvous. Each send pairs with one receive.
    # - Single waiter queue, direction inferred from signed balance.
    # - Sync operations may do immediate tealet run() based on preference.
    # - Async operations never do immediate transfer; they only wake.

    def __init__(self, preference: int = -1) -> None:
        self._waiters: deque[tealet.tealet | Event] = deque()
        self._balance = 0
        self._packets: dict[tealet.tealet | Event, tuple[bool, object]] = {}
        self.preference = preference

    # -- Configuration -------------------------------------------------

    @property
    def balance(self) -> int:
        """Return positive senders or negative receivers waiting on the channel."""

        return self._balance

    @property
    def preference(self) -> int:
        """Return the immediate-transfer preference for sync rendezvous."""

        return self._preference

    @preference.setter
    def preference(self, value: int) -> None:
        """Set sync rendezvous preference to sender, neutral, or receiver."""

        if value not in (-1, 0, 1):
            raise ValueError("preference must be -1, 0, or 1")
        self._preference = value

    def _deliver(self, packet: tuple[bool, object]) -> object:
        is_exc, payload = packet
        if is_exc:
            raise cast(BaseException, payload)
        return payload

    # -- Waiter bookkeeping -------------------------------------------

    def _clear_link(self, t: tealet.tealet) -> None:
        try:
            t.link = None
        except AttributeError:
            pass

    def _set_waiter_link(self, waiter: tealet.tealet | Event) -> None:
        if isinstance(waiter, Event):
            return
        self._clear_link(waiter)
        try:
            waiter.link = self
        except AttributeError:
            pass

    def _waiter_scheduler(self, waiter: tealet.tealet) -> BaseScheduler:
        if isinstance(waiter, _tasks.Task):
            return waiter.get_scheduler()
        return get_running_scheduler()

    def _run_on_scheduler(self, owning: BaseScheduler, callback: Callable[..., object], *args: object) -> None:
        if _current_scheduler() is owning:
            callback(*args)
            return
        owning.call_soon_threadsafe(callback, *args)

    def _wake_non_immediate(self, waiter: tealet.tealet | Event) -> None:
        if isinstance(waiter, Event):
            waiter.set()
            return
        owning = self._waiter_scheduler(waiter)
        self._run_on_scheduler(owning, owning._make_runnable, waiter)

    def _wake_sync(self, waiter: tealet.tealet | Event, prefer_immediate: bool) -> None:
        if isinstance(waiter, Event):
            waiter.set()
            return
        current = _current_scheduler()
        owning = self._waiter_scheduler(waiter)
        if prefer_immediate and current is owning:
            waiter.run()
            return
        self._run_on_scheduler(owning, owning._make_runnable, waiter)

    def _link_sender(self, waiter: tealet.tealet | Event, packet: tuple[bool, object]) -> None:
        self._packets[waiter] = packet
        self._waiters.append(waiter)
        self._balance += 1
        self._set_waiter_link(waiter)

    def _link_receiver(self, waiter: tealet.tealet | Event) -> None:
        self._waiters.append(waiter)
        self._balance -= 1
        self._set_waiter_link(waiter)

    def _unlink_waiter(self, waiter: tealet.tealet | Event) -> None:
        removed = False
        self._packets.pop(waiter, None)
        try:
            self._waiters.remove(waiter)
            removed = True
        except ValueError:
            pass
        if removed:
            if self._balance > 0:
                self._balance -= 1
            elif self._balance < 0:
                self._balance += 1
        if isinstance(waiter, tealet.tealet):
            self._clear_link(waiter)

    def _unlink(self, t: tealet.tealet) -> None:
        self._unlink_waiter(t)

    def _query_waiting(self) -> bool:
        return True

    def _query_runnable(self) -> bool:
        return False

    # -- Synchronous operations ---------------------------------------

    def _send_packet(self, packet: tuple[bool, object]) -> None:
        if self._balance < 0:
            receiver = self._waiters.popleft()
            self._balance += 1
            if isinstance(receiver, tealet.tealet):
                self._clear_link(receiver)
            self._packets[receiver] = packet
            self._wake_sync(receiver, prefer_immediate=self._preference < 0)
            return

        current = tealet.current()
        try:
            get_running_scheduler()._schedule(lambda: self._link_sender(current, packet))
        except BaseException as exc:
            missing = object()
            pending = self._packets.pop(current, missing)
            self._unlink_waiter(current)
            if pending is missing and isinstance(exc, RawTimeoutError):
                # Timeout-vs-delivery race: if receiver already consumed packet,
                # treat send as successful and suppress timeout.
                return
            raise

    def send(self, value: object) -> None:
        """Send one value using sync tealet rendezvous semantics.

        If a receiver is already waiting, the value is delivered immediately.
        For a waiting tealet receiver, immediate ``run()`` may occur depending
        on ``preference``. If no receiver is waiting, the current tealet blocks
        by entering the channel wait queue.
        """
        self._send_packet((False, value))

    def send_exception(self, exc: BaseException) -> None:
        """Send an exception instance to the next receiver.

        The receiver raises the delivered exception when it receives the packet.
        """
        if not isinstance(exc, BaseException):
            raise TypeError("exc must be a BaseException instance")
        self._send_packet((True, exc))

    def receive(self) -> object:
        """Receive one packet using sync tealet rendezvous semantics.

        If a sender is already waiting, the packet is consumed immediately.
        For a waiting tealet sender, immediate ``run()`` may occur depending on
        ``preference``. If no sender is waiting, the current tealet blocks by
        entering the channel wait queue.
        """
        if self._balance > 0:
            sender = self._waiters.popleft()
            self._balance -= 1
            packet = self._packets.pop(sender)
            if isinstance(sender, tealet.tealet):
                self._clear_link(sender)
            self._wake_sync(sender, prefer_immediate=self._preference > 0)
            return self._deliver(packet)

        current = tealet.current()
        try:
            get_running_scheduler()._schedule(lambda: self._link_receiver(current))
        except BaseException as exc:
            missing = object()
            packet = self._packets.pop(current, missing)
            self._unlink_waiter(current)
            if packet is not missing and isinstance(exc, RawTimeoutError):
                # Timeout-vs-delivery race: if a packet was already delivered,
                # consume the packet and suppress the timeout.
                return self._deliver(cast(tuple[bool, object], packet))
            raise

        return self._deliver(self._packets.pop(current))

    # -- Async operations ---------------------------------------------

    async def async_send(self, value: object) -> None:
        """Send one value from async code without immediate task transfer.

        Async operations always use non-immediate wake semantics. When matching
        a waiting tealet receiver, it is only made runnable. When matching an
        Event waiter, the Event is set.
        """
        packet = (False, value)
        if self._balance < 0:
            receiver = self._waiters.popleft()
            self._balance += 1
            if isinstance(receiver, tealet.tealet):
                self._clear_link(receiver)
            self._packets[receiver] = packet
            self._wake_non_immediate(receiver)
            return

        waiter = Event()
        self._link_sender(waiter, packet)
        try:
            await waiter.wait()
        except BaseException as exc:
            missing = object()
            pending = self._packets.pop(waiter, missing)
            self._unlink_waiter(waiter)
            if pending is missing and isinstance(exc, _tasks.CancelledError):
                return
            raise

    async def async_receive(self) -> object:
        """Receive one packet from async code without immediate task transfer.

        Async operations always use non-immediate wake semantics. When matching
        a waiting tealet sender, it is only made runnable. When matching an
        Event waiter, the Event is set.
        """
        if self._balance > 0:
            sender = self._waiters.popleft()
            self._balance -= 1
            packet = self._packets.pop(sender)
            if isinstance(sender, tealet.tealet):
                self._clear_link(sender)
            self._wake_non_immediate(sender)
            return self._deliver(packet)

        waiter = Event()
        self._link_receiver(waiter)
        try:
            await waiter.wait()
        except BaseException as exc:
            # Async timeouts surface as cancellation at this await point.
            # We cannot reliably distinguish timeout cancellation from an
            # explicit task cancellation here. If a packet is already queued,
            # prefer delivering data over propagating cancellation.
            missing = object()
            packet = self._packets.pop(waiter, missing)
            self._unlink_waiter(waiter)
            if packet is not missing and isinstance(exc, _tasks.CancelledError):
                return self._deliver(cast(tuple[bool, object], packet))
            raise

        return self._deliver(self._packets.pop(waiter))


class DeadlockError(RuntimeError):
    """Raised when the scheduler has no runnable tasks."""


def _format_callback_source(callback: Callable[..., object], args: tuple[object, ...]) -> str:
    func_repr = getattr(callback, "__qualname__", repr(callback))
    if args:
        arg_repr = ", ".join(repr(arg) for arg in args)
        return f"{func_repr}({arg_repr})"
    return f"{func_repr}()"


class TimerHandle:
    """Cancellable callback scheduled to run in the future."""

    def __init__(
        self,
        when: float,
        callback: Callable[..., object],
        args: tuple[object, ...] = (),
        context: contextvars.Context | None = None,
    ) -> None:
        self._when = when
        self._callback = callback
        self._args = args
        self._context = context
        self._cancelled = False

    # -- Public state --------------------------------------------------

    @property
    def when(self) -> float:
        """Return the monotonic time when this handle is due."""

        return self._when

    def cancel(self) -> None:
        """Prevent this timer callback from running."""

        self._cancelled = True

    def cancelled(self) -> bool:
        """Return True if this timer handle has been cancelled."""

        return self._cancelled

    # -- Execution -----------------------------------------------------

    def _run(self, scheduler: BaseScheduler) -> None:
        if self._cancelled:
            return
        scheduler._run_callback(self._callback, self._args, self._context, handle=self)

    def __enter__(self) -> "TimerHandle":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.cancel()


class _GatheringFuture(_tasks.Future[list[Any]]):
    def __init__(self, children: list[_tasks.Future[Any]]) -> None:
        super().__init__()
        self._children = children
        self._cancel_requested = False

    def cancel(self) -> bool:
        if self.done():
            return False
        self._cancel_requested = True
        cancelled = False
        for child in self._children:
            cancelled = child.cancel() or cancelled
        return cancelled

    def _cancel_requested_exception(self) -> _tasks.CancelledError | None:
        if self._cancel_requested:
            return _tasks.CancelledError()
        return None


def gather(
    *entries: _tasks.Future[Any] | Callable[[], Any],
    return_exceptions: bool = False,
) -> _tasks.Future[list[Any]]:
    """Return a Future collecting results from scheduler futures or callables."""

    scheduler = get_scheduler()
    children = [scheduler.ensure_future(entry) for entry in entries]

    gather_future = _GatheringFuture(children)
    if not children:
        gather_future.set_result([])
        return gather_future

    results: list[Any] = [None] * len(children)
    remaining = len(children)

    def child_done(index: int, child: _tasks.Future[Any]) -> None:
        nonlocal remaining
        if gather_future.done():
            return
        if child._exception is not None:
            if not return_exceptions:
                gather_future.set_exception(child._exception)
                return
            results[index] = child._exception
        else:
            results[index] = child._result
        remaining -= 1
        if remaining == 0:
            cancelled = gather_future._cancel_requested_exception()
            if cancelled is not None:
                gather_future.set_exception(cancelled)
                return
            gather_future.set_result(results)

    for index, child in enumerate(children):
        if child.done():
            child_done(index, child)
        else:
            child.add_done_callback(lambda done_child, index=index: child_done(index, done_child))

    return gather_future


def ensure_future(
    entry: _tasks.Future[Any] | Callable[[], Any],
) -> _tasks.Future[Any]:
    """Return `entry` as a scheduler Future, spawning callables as needed."""

    scheduler = get_scheduler()
    return scheduler.ensure_future(entry)


def wait(
    entries: Iterable[_tasks.Future[Any] | Callable[[], Any]],
    *,
    timeout: float | None = None,
    return_when: _ReturnWhen = ALL_COMPLETED,
) -> _tasks.Future[tuple[set[_tasks.Future[Any]], set[_tasks.Future[Any]]]]:
    """Return a Future that waits for entries according to `return_when`."""

    scheduler = get_scheduler()
    children = {scheduler.ensure_future(entry) for entry in entries}
    if not children:
        raise ValueError("Set of Futures is empty.")
    if return_when not in (FIRST_COMPLETED, FIRST_EXCEPTION, ALL_COMPLETED):
        raise ValueError(f"Invalid return_when value: {return_when!r}")

    def wait_task() -> tuple[set[_tasks.Future[Any]], set[_tasks.Future[Any]]]:
        try:
            for child in _as_completed_futures(children, timeout=timeout):
                if return_when == FIRST_COMPLETED:
                    break
                if return_when == FIRST_EXCEPTION and child._exception is not None and not child.cancelled():
                    break
        except TimeoutError:
            pass
        done = {child for child in children if child.done()}
        return done, children - done

    return scheduler.spawn(wait_task)


def _as_completed_futures(
    children: Iterable[_tasks.Future[Any]],
    *,
    timeout: float | None = None,
) -> Iterator[_tasks.Future[Any]]:
    children = list(dict.fromkeys(children))
    if not children:
        return
    pending_callbacks = set(children)
    completed: Queue[_tasks.Future[Any]] = Queue()

    def complete_next(child: _tasks.Future[Any]) -> None:
        pending_callbacks.discard(child)
        completed.put_nowait(child)
        if not pending_callbacks:
            completed.shutdown()

    try:
        for child in children:
            if child.done():
                complete_next(child)
            else:
                child.add_done_callback(complete_next)
        with scheduler_timeout(timeout) if timeout is not None else nullcontext():
            while True:
                child = completed.sget()
                yield child
    except QueueShutDown:
        return
    finally:
        for child in pending_callbacks:
            child.remove_done_callback(complete_next)


def wait_for(
    entry: _tasks.Future[Any] | Callable[[], Any],
    timeout: float | None,
) -> _tasks.Future[Any]:
    """Return a Future that waits for one entry with an optional timeout."""

    scheduler = get_scheduler()
    child = scheduler.ensure_future(entry)

    def wait_task() -> Any:
        try:
            with scheduler_timeout(timeout) if timeout is not None else nullcontext():
                return child.wait()
        except TimeoutError:
            if timeout is None:
                raise
            child.cancel()
            raise

    return scheduler.spawn(wait_task)


def as_completed(
    entries: Iterable[_tasks.Future[Any] | Callable[[], Any]],
    *,
    timeout: float | None = None,
) -> Iterator[_tasks.Future[Any]]:
    """Yield scheduler futures as they complete."""

    scheduler = get_scheduler()
    children = (scheduler.ensure_future(entry) for entry in entries)
    return _as_completed_futures(children, timeout=timeout)


class BaseScheduler(_tasks.TaskLink, CoreSchedulerDrivingAPI):
    """Shared cooperative scheduling mechanics for concrete drivers."""

    def __init__(self, *, runnable_queue_factory: RunnableQueueFactory | None = None) -> None:
        if runnable_queue_factory is None:
            runnable_queue_factory = PrescheduledRunnableQueue
        self._runnable = runnable_queue_factory()
        self._all_tasks: weakref.WeakSet[_tasks.Task] = weakref.WeakSet()
        self._runner = None
        self._running = False
        self._owner_thread: int | None = None
        self._debug = False
        self._stopping = False
        self._threadsafe_callbacks: queue.SimpleQueue[
            tuple[Callable[..., object], tuple[object, ...], contextvars.Context | None]
        ] = queue.SimpleQueue()
        self._threadsafe_lock = threading.Lock()
        self._pending_executor_calls = 0
        self._pending_async_waits: set[tealet.tealet] = set()
        self._timers: list[tuple[float, int, TimerHandle]] = []
        self._timer_sequence = itertools.count()
        self._time: _TimeFunction = time.monotonic
        self._n_scheduled = 0
        self._target_count = None
        self._default_executor: concurrent.futures.ThreadPoolExecutor | None = None
        self._task_factory: _tasks.TaskFactory = _tasks.DefaultTaskFactory()
        self._exception_handler: Callable[[dict[str, Any]], object] | None = None

    # -- Basic state ---------------------------------------------------

    def time(self) -> float:
        """Return the scheduler's monotonic clock value."""

        return self._time()

    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Raise because this scheduler does not support explicit loop binding."""

        raise NotImplementedError(f"{type(self).__name__} does not support bind_loop()")

    def is_running(self) -> bool:
        """Return True while this scheduler is being driven."""

        return self._running

    def set_debug(self, enabled: bool) -> None:
        """Set the scheduler debug flag."""

        self._debug = bool(enabled)

    def get_debug(self) -> bool:
        """Return the scheduler debug flag."""

        return self._debug

    def all_tasks(self) -> set[_tasks.Task]:
        """Return unfinished scheduler-owned tasks."""

        return {task for task in self._all_tasks if not task.done()}

    def runnable_tasks(self) -> tuple[_tasks.Task, ...]:
        """Return scheduler-owned tasks currently waiting to run."""

        return self._runnable.tasks()

    def get_task_factory(self) -> _tasks.TaskFactory:
        """Return the task factory used by `spawn()`."""

        return self._task_factory

    def set_task_factory(self, factory: _tasks.TaskFactory | None) -> None:
        """Set the task factory, or restore the default when `factory` is None."""

        self._task_factory = _tasks.DefaultTaskFactory() if factory is None else factory

    def set_exception_handler(self, handler: Callable[[dict[str, Any]], object] | None) -> None:
        """Set the callback invoked for exceptions raised by scheduled callbacks."""

        self._exception_handler = handler

    def get_exception_handler(self) -> Callable[[dict[str, Any]], object] | None:
        """Return the current scheduled-callback exception handler."""

        return self._exception_handler

    def default_exception_handler(self, context: dict[str, Any]) -> None:
        """Log an exception raised by a scheduled callback."""

        message = context.get("message") or "Unhandled exception in scheduler callback"
        exception = context.get("exception")
        exc_info: bool | tuple[type[BaseException], BaseException, Any] = False
        if exception is not None:
            exc_info = (type(exception), exception, exception.__traceback__)
        log_lines = [message]
        for key in sorted(context):
            if key in {"message", "exception"}:
                continue
            log_lines.append(f"{key}: {context[key]!r}")
        logger.error("\n".join(log_lines), exc_info=exc_info)

    def call_exception_handler(self, context: dict[str, Any]) -> None:
        """Dispatch a scheduled-callback exception to the configured handler."""

        handler = self._exception_handler
        if handler is None:
            try:
                self.default_exception_handler(context)
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException:
                logger.error("Exception in default exception handler", exc_info=True)
            return
        try:
            handler(context)
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException:
            logger.error("Exception in exception handler", exc_info=True)

    def _run_callback(
        self,
        callback: Callable[..., object],
        args: tuple[object, ...],
        context: contextvars.Context | None = None,
        *,
        handle: TimerHandle | None = None,
    ) -> None:
        try:
            if context is None:
                callback(*args)
            else:
                context.run(callback, *args)
        except (SystemExit, KeyboardInterrupt, _tasks.CancelledError):
            raise
        except BaseException as exc:
            self.call_exception_handler(
                {
                    "message": f"Exception in callback {_format_callback_source(callback, args)}",
                    "exception": exc,
                    "scheduler": self,
                    "handle": handle,
                }
            )

    def main_context(self) -> ContextManager[None]:
        """Use this scheduler's task factory for the current main tealet wrapper."""
        return _tasks.scheduler_tealet_factory(self)

    def close(self) -> None:
        """Release scheduler-owned external resources."""

        executor = self._default_executor
        if executor is not None:
            self._default_executor = None
            executor.shutdown(wait=False)

    def shutdown_default_executor(
        self,
        timeout: float | None = DEFAULT_EXECUTOR_SHUTDOWN_TIMEOUT,
    ) -> _tasks.Future[Any]:
        """Return a Future that completes after the default executor shuts down."""

        future: _tasks.Future[Any] = _tasks.Future()
        executor = self._default_executor
        if executor is None:
            future.set_result(None)
            return future

        self._default_executor = None

        def complete_shutdown(exc: BaseException | None = None) -> None:
            if not future.done():
                if exc is None:
                    future.set_result(None)
                else:
                    future.set_exception(exc)

        def shutdown_worker() -> None:
            try:
                executor.shutdown(wait=True)
            except BaseException as exc:
                self.call_soon_threadsafe(complete_shutdown, exc)
            else:
                self.call_soon_threadsafe(complete_shutdown)

        thread = threading.Thread(
            target=shutdown_worker,
            name="tealet-default-executor-shutdown",
            daemon=True,
        )
        thread.start()

        def wait_for_shutdown(_timeout: float | None = timeout) -> None:
            try:
                with scheduler_timeout(_timeout) if _timeout is not None else nullcontext():
                    future.wait()
            except TimeoutError:
                warnings.warn(
                    f"The executor did not finish joining its threads within {_timeout} seconds.",
                    RuntimeWarning,
                    stacklevel=2,
                )
                executor.shutdown(wait=False)

        return self.spawn(wait_for_shutdown)

    # -- External integration APIs ------------------------------------

    def getaddrinfo(
        self,
        host: str | bytes | None,
        port: str | int | bytes | None,
        *,
        family: int = 0,
        type: int = 0,
        proto: int = 0,
        flags: int = 0,
    ) -> list[tuple[int, int, int, str, tuple[Any, ...]]]:
        """Resolve ``host``/``port`` on a worker thread without blocking the scheduler thread."""

        return cast(
            list[tuple[int, int, int, str, tuple[Any, ...]]],
            self.run_in_executor(
                None,
                socket.getaddrinfo,
                host,
                port,
                family,
                type,
                proto,
                flags,
            ).wait(),
        )

    def getnameinfo(self, sockaddr: tuple[Any, ...], flags: int = 0) -> tuple[str, str]:
        """Reverse-resolve ``sockaddr`` on a worker thread without blocking the scheduler thread."""

        return self.run_in_executor(None, socket.getnameinfo, sockaddr, flags).wait()

    def ensure_resolved(
        self,
        address: tuple[Any, ...],
        *,
        family: int = 0,
        type: int = socket.SOCK_STREAM,
        proto: int = 0,
        flags: int = 0,
    ) -> list[tuple[int, int, int, str, tuple[Any, ...]]]:
        """Resolve ``address``, skipping executor lookup for literal IP hosts."""

        from . import dns

        return dns.ensure_resolved(
            self,
            address,
            family=family,
            type=type,
            proto=proto,
            flags=flags,
        )

    def run_in_executor(
        self,
        executor: concurrent.futures.Executor | None,
        func: Callable[..., T],
        *args: object,
    ) -> _tasks.Future[T]:
        """Run `func(*args)` in an executor and return a scheduler Future."""

        if executor is None:
            if self._default_executor is None:
                self._default_executor = concurrent.futures.ThreadPoolExecutor(thread_name_prefix="tealet")
            executor = self._default_executor

        future: _tasks.Future[T] = _tasks.Future()

        with self._threadsafe_lock:
            self._pending_executor_calls += 1

        def complete_result(value: T) -> None:
            try:
                if not future.done():
                    future.set_result(value)
            finally:
                self._executor_call_done()

        def complete_exception(exc: BaseException) -> None:
            try:
                if not future.done():
                    future.set_exception(exc)
            finally:
                self._executor_call_done()

        def worker() -> None:
            try:
                result = func(*args)
            except BaseException as exc:
                self.call_soon_threadsafe(complete_exception, exc)
            else:
                self.call_soon_threadsafe(complete_result, result)

        try:
            executor.submit(worker)
        except BaseException:
            self._executor_call_done()
            raise
        return future

    def _executor_call_done(self) -> None:
        with self._threadsafe_lock:
            self._pending_executor_calls -= 1
        self._break_wait()

    def add_reader(self, fd: int, callback: Callable[..., object], *args: object) -> None:
        """Register a callback for readability on `fd`."""

        raise NotImplementedError("reader callbacks require an IO-capable scheduler")

    def remove_reader(self, fd: int) -> bool:
        """Remove the readability callback for `fd`."""

        raise NotImplementedError("reader callbacks require an IO-capable scheduler")

    def add_writer(self, fd: int, callback: Callable[..., object], *args: object) -> None:
        """Register a callback for writability on `fd`."""

        raise NotImplementedError("writer callbacks require an IO-capable scheduler")

    def remove_writer(self, fd: int) -> bool:
        """Remove the writability callback for `fd`."""

        raise NotImplementedError("writer callbacks require an IO-capable scheduler")

    def sock_recv(self, sock: socket.socket, n: int) -> bytes:
        """Receive up to `n` bytes from a non-blocking socket."""

        raise NotImplementedError("socket helpers require an IO-capable scheduler")

    def sock_recv_into(self, sock: socket.socket, buf: Any) -> int:
        """Receive bytes from a non-blocking socket into `buf`."""

        raise NotImplementedError("socket helpers require an IO-capable scheduler")

    def sock_recvfrom(self, sock: socket.socket, bufsize: int) -> tuple[bytes, Any]:
        """Receive datagram bytes and address from a non-blocking socket."""

        raise NotImplementedError("socket helpers require an IO-capable scheduler")

    def sock_recvfrom_into(self, sock: socket.socket, buf: Any, nbytes: int = 0) -> tuple[int, Any]:
        """Receive datagram bytes into `buf` from a non-blocking socket."""

        raise NotImplementedError("socket helpers require an IO-capable scheduler")

    def sock_sendall(self, sock: socket.socket, data: Any) -> None:
        """Send all `data` through a non-blocking socket."""

        raise NotImplementedError("socket helpers require an IO-capable scheduler")

    def sock_sendto(self, sock: socket.socket, data: Any, address: Any) -> int:
        """Send one datagram through a non-blocking socket."""

        raise NotImplementedError("socket helpers require an IO-capable scheduler")

    def sock_accept(self, sock: socket.socket) -> tuple[socket.socket, Any]:
        """Accept one connection from a non-blocking listening socket."""

        raise NotImplementedError("socket helpers require an IO-capable scheduler")

    def sock_connect(self, sock: socket.socket, address: Any) -> None:
        """Connect a non-blocking socket to `address`."""

        raise NotImplementedError("socket helpers require an IO-capable scheduler")

    def sock_create(
        self,
        family: int,
        type: int,
        proto: int = 0,
        *,
        flags: int = 0,
    ) -> socket.socket:
        """Create a non-blocking, close-on-exec socket through the scheduler IO backend."""

        raise NotImplementedError("socket helpers require an IO-capable scheduler")

    def open_streams(
        self,
        sock: socket.socket,
        *,
        limit: int = 2**16,
        stream_factory: Any = None,
        async_: bool = False,
    ) -> tuple[Any, Any]:
        """Wrap a connected non-blocking socket as stream endpoints."""

        raise NotImplementedError("stream helpers require an IO-capable scheduler")

    def open_connection(
        self,
        *,
        addr: tuple[str, int] | None = None,
        path: str | None = None,
        family: int = socket.AF_UNSPEC,
        proto: int = 0,
        limit: int = 2**16,
        stream_factory: Any = None,
        async_: bool = False,
    ) -> tuple[Any, Any]:
        """Connect and return stream endpoints (TCP via ``addr``, Unix via ``path``)."""

        raise NotImplementedError("stream helpers require an IO-capable scheduler")

    def start_server(
        self,
        client_handler: Callable[..., Any],
        *,
        addr: tuple[str | None, int] | None = None,
        path: str | None = None,
        family: int = socket.AF_INET,
        backlog: int = 100,
        limit: int = 2**16,
        stream_factory: Any = None,
        async_: bool = False,
    ) -> Any:
        """Start a stream server that dispatches each accept to ``client_handler``."""

        raise NotImplementedError("stream helpers require an IO-capable scheduler")

    def poll(self, fd: int, mask: int) -> int:
        """Wait until an fd reports events in `mask` and return the readiness bitmask."""

        raise NotImplementedError("poll requires an IO-capable scheduler")

    def poll_many(
        self,
        fd: int,
        mask: int,
        callback: Callable[[int], object],
    ) -> ContinuousOperation[int]:
        """Emit readiness bitmasks until cancelled or the backend reports a terminal error."""

        raise NotImplementedError("poll requires an IO-capable scheduler")

    def open(self, path: str, mode: str = "rb") -> Any:
        """Open a positioned binary file through the scheduler."""

        raise NotImplementedError("file I/O requires a proactor scheduler with openat support")

    # -- Driver state --------------------------------------------------

    def _verify_current_scheduler(self) -> None:
        if _current_scheduler() is not self:
            raise RuntimeError("operation requires this scheduler to be the current scheduler")
        if self._running:
            raise RuntimeError("Scheduler already running")

    def stop(self) -> None:
        """Ask the currently running driver loop to stop."""

        self._stopping = True
        self._break_wait()

    # -- Callback and timer scheduling --------------------------------

    def call_soon(
        self,
        callback: Callable[..., object],
        *args: object,
        context: contextvars.Context | None = None,
    ) -> TimerHandle:
        """Schedule `callback(*args)` to run on the next scheduler turn."""

        return self.call_at(self.time(), callback, *args, context=context)

    def call_soon_threadsafe(
        self,
        callback: Callable[..., object],
        *args: object,
        context: contextvars.Context | None = None,
        immediate: bool = False,
    ) -> None:
        """Schedule `callback(*args)` from another thread or driver context."""

        if context is None:
            context = contextvars.copy_context()
        if immediate and self._owner_thread == threading.get_ident():
            self._run_callback(callback, args, context)
            return
        self._threadsafe_callbacks.put((callback, args, context))
        self._break_wait_threadsafe()

    def call_later(
        self,
        delay: float,
        callback: Callable[..., object],
        *args: object,
        context: contextvars.Context | None = None,
    ) -> TimerHandle:
        """Schedule `callback(*args)` to run after `delay` seconds."""

        if delay < 0:
            delay = 0
        return self.call_at(self.time() + delay, callback, *args, context=context)

    def call_at(
        self,
        when: float,
        callback: Callable[..., object],
        *args: object,
        context: contextvars.Context | None = None,
    ) -> TimerHandle:
        """Schedule `callback(*args)` at monotonic time `when`."""

        if context is None:
            context = contextvars.copy_context()
        handle = TimerHandle(when, callback, args, context=context)
        self._enqueue_timer(when, handle)
        return handle

    def _enqueue_timer(self, when: float, handle: TimerHandle) -> None:
        heapq.heappush(self._timers, (when, next(self._timer_sequence), handle))
        self._break_wait()

    def _drain_threadsafe_callbacks(self) -> None:
        # Drain one queued entry at a time. If a callback raises (e.g.
        # CancelledError from task.cancel() targeting the current tealet),
        # leave any remaining entries for a later drain. Bound work to the
        # queue depth observed at entry so callbacks enqueued during this
        # drain are not serviced until the next scheduler turn.
        pending = self._threadsafe_callbacks.qsize()
        while pending > 0:
            pending -= 1
            try:
                callback, args, context = self._threadsafe_callbacks.get_nowait()
            except queue.Empty:
                break
            self._run_callback(callback, args, context)

    def _run_ready_timers(self) -> None:
        self._drain_threadsafe_callbacks()
        now = self.time()
        while self._timers and self._timers[0][0] <= now:
            _, _, handle = heapq.heappop(self._timers)
            handle._run(self)

    def _next_timer_deadline(self) -> float | None:
        while self._timers and self._timers[0][2].cancelled():
            heapq.heappop(self._timers)
        if not self._timers:
            return None
        return self._timers[0][0]

    def _delay_until(self, when: float) -> float:
        return max(0.0, when - self.time())

    def _has_pending_timers(self) -> bool:
        return self._next_timer_deadline() is not None

    def _has_pending_driver_work(self) -> bool:
        if not self._threadsafe_callbacks.empty():
            return True
        with self._threadsafe_lock:
            return bool(self._pending_executor_calls)

    def _has_runnable_work(self) -> bool:
        return bool(self._runnable)

    def _pop_next_runnable(self) -> tealet.tealet:
        return self._runnable.pop_next()

    # -- Link and runnable state --------------------------------------

    def _is_runnable(self, t: tealet.tealet) -> bool:
        return t in self._runnable

    def _is_blocked(self, t: tealet.tealet) -> bool:
        return t in self._pending_async_waits

    def _unlink_pending_async_wait(self, t: tealet.tealet) -> None:
        self._pending_async_waits.discard(t)

    def _unlink(self, t: tealet.tealet) -> None:
        removed = False
        if self._runnable.discard(t):
            removed = True
        if t in self._pending_async_waits:
            self._pending_async_waits.remove(t)
            removed = True
        if removed:
            try:
                t.link = None
            except AttributeError:
                pass

    # -- Task creation and cooperative operations ---------------------

    def ensure_future(
        self,
        entry: _tasks.Future[Any] | Callable[[], Any],
    ) -> _tasks.Future[Any]:
        """Return `entry` as a Future owned by this scheduler."""

        if isinstance(entry, _tasks.Future):
            if isinstance(entry, _tasks.Task) and entry.get_scheduler() is not self:
                raise RuntimeError("Future is bound to a different scheduler")
            return entry
        if callable(entry):
            return self.spawn(entry)
        raise TypeError("entry must be a Future or callable")

    def spawn(
        self,
        func: Callable[[], T],
        *,
        context: contextvars.Context | None = None,
        eager_start: bool | None = None,
        **kwargs: Any,
    ) -> _tasks.Task:
        """Create and schedule a task from a zero-argument callable."""

        if context is None:
            context = contextvars.copy_context()

        t = self._task_factory(self, func, context=context, eager_start=eager_start, **kwargs)
        self._all_tasks.add(t)
        if not t.done():
            self._make_runnable(t)
        return t

    def _schedule(self, enqueue=None) -> None:
        if enqueue is not None:
            enqueue()
        self._run_ready_timers()
        target = self._find_target()
        target.switch()

    def yield_(self) -> None:
        """Yield the current task and make it runnable again."""

        self._schedule(lambda: self._make_runnable(tealet.current()))

    def _sleep_until(self, when: float) -> None:
        if when <= self.time():
            self.yield_()
            return

        current = tealet.current()
        awakened = False

        def wake() -> None:
            nonlocal awakened
            awakened = True
            self._make_runnable(current)

        with self.call_at(when, wake):
            if not awakened:
                self._schedule()

    def sleep(self, delay: float) -> None:
        """Suspend the current task for `delay` seconds."""

        self._sleep_until(self.time() + delay)

    def await_(self, awaitable):
        """Await an asyncio awaitable from a tealet task and return its result."""
        loop = asyncio.get_running_loop()

        if inspect.iscoroutine(awaitable):
            return self._await_coro(cast(Coroutine[Any, Any, Any], awaitable), loop)
        if asyncio.isfuture(awaitable):
            return self._await_future(awaitable, loop)
        if inspect.isawaitable(awaitable):
            # run in a copy of the current context if possible, outside tealetio task scope
            context = _tasks._copy_context_without_current_task()
            try:
                fut = cast(Any, loop).create_task(cast(Coroutine[Any, Any, Any], awaitable), context=context)
            except TypeError:
                fut = loop.create_task(cast(Coroutine[Any, Any, Any], awaitable))
            return self._await_future(fut, loop)

        raise TypeError("awaitable must be an awaitable")

    def _await_coro(
        self,
        coro: Coroutine[Any, Any, Any],
        loop: asyncio.AbstractEventLoop,
    ) -> Any:
        with _tasks._without_current_task():
            return _coro_drive(coro, lambda yielded: self._await_future(yielded, loop))

    def _await_future(
        self,
        fut: Any,
        loop: asyncio.AbstractEventLoop,
    ) -> Any:
        if fut is None:
            self.yield_()
            return None

        if not asyncio.isfuture(fut):
            raise RuntimeError(f"await_ coroutine yielded unsupported object: {fut!r}")

        fut = cast(asyncio.Future[Any], fut)

        current = tealet.current()

        if fut.get_loop() is not loop:
            raise RuntimeError("await_ future is bound to a different event loop")

        if fut.done():
            return fut.result()

        done_evt = Event()
        self._pending_async_waits.add(current)
        state = {"active": True}

        def _resume_waiter(_fut) -> None:
            if not state["active"]:
                return
            state["active"] = False
            self._pending_async_waits.discard(current)
            self._make_runnable(current)
            done_evt.set()

        fut.add_done_callback(_resume_waiter)
        try:
            try:
                done_evt.swait()
            except _tasks.CancelledError:
                loop.call_soon(fut.cancel)
                raise
        finally:
            if state["active"]:
                state["active"] = False
                self._pending_async_waits.discard(current)
            fut.remove_done_callback(_resume_waiter)

        return fut.result()

    # -- Scheduler-owned transfer -------------------------------------

    def _make_runnable(self, t: tealet.tealet) -> None:
        if t in self._runnable:
            return
        assert isinstance(t, _tasks.Task)
        t._scheduler = self
        self._runnable.add(t)
        self._break_wait()

    def reschedule(self, task: _tasks.Task, *, position: int | None = None) -> None:
        """Move a runnable scheduler task to a new runnable queue position."""
        if task.get_scheduler() is not self:
            raise RuntimeError("task is bound to a different scheduler")
        self._runnable.reschedule(task, position)
        self._break_wait()

    def yield_to(self, task: _tasks.Task, *, insert_current_at: int | None = None) -> None:
        """Yield to a runnable scheduler task and keep current runnable."""
        current = tealet.current()
        if task is current:
            return
        if task.get_scheduler() is not self:
            raise RuntimeError("task is bound to a different scheduler")

        def enqueue() -> None:
            self._runnable.yield_to(task, current, insert_current_at)

        self._schedule(enqueue)

    def _target_run(self, target: tealet.tealet) -> None:
        if target is tealet.current():
            return
        assert isinstance(target, _tasks.Task)
        target._unlink()
        self._make_runnable(tealet.current())
        target.switch()

    def _target_throw(self, target: tealet.tealet, exc: BaseException) -> None:
        if target is tealet.current():
            raise exc
        assert isinstance(target, _tasks.Task)
        target._unlink()
        self._make_runnable(tealet.current())
        target._throw_from_scheduler(exc)

    def _find_target(self, task_exit=False) -> tealet.tealet:
        count_transfer = True
        if self._runner is not None and self._target_count is not None and self._n_scheduled >= self._target_count:
            result = self._runner
            assert isinstance(result, _tasks.Task)
            result._unlink()
            count_transfer = False
        elif self._has_runnable_work():
            result = self._pop_next_runnable()
        elif not task_exit:
            raise DeadlockError("No tasks to switch to")
        else:
            result = tealet.main()
        if count_transfer:
            self._n_scheduled += 1
        assert isinstance(result, _tasks.Task)
        result.link = None
        return result

    def _run_ready_batch(self, limit: int | None = None) -> int:
        if self._runner is not None:
            raise RuntimeError("Scheduler already running")
        start_count = self._n_scheduled
        if limit is not None and limit > 0:
            self._target_count = start_count + limit
        self._runner = tealet.current()
        try:
            self._run_ready_timers()
            if not self._has_runnable_work():
                return 0
            self.yield_()
            return self._n_scheduled - start_count - 1
        finally:
            self._runner = None
            self._target_count = None

    def pump(self, n: int | None = None) -> int:
        """Run at most `n` ready scheduler transfers and return the count."""

        self._verify_current_scheduler()
        with self.main_context(), _tasks.task_priority(tealet.current(), _tasks.TEALET_PRI_INF):
            self._running = True
            self._owner_thread = threading.get_ident()
            try:
                return self._run_ready_batch(n)
            finally:
                self._owner_thread = None
                self._running = False

    @abstractmethod
    def _break_wait_threadsafe(self) -> None:
        """Wake a concrete driver from another thread or scheduler context."""

    @abstractmethod
    def _break_wait(self) -> None:
        """Wake a concrete driver from its owning context."""


class BasicScheduler(SyncDrivingMixin, BaseScheduler, SyncSchedulerDrivingAPI):
    """Cooperative scheduling core with timers and explicit wakeups, but no IO driver."""

    def __init__(self, *, runnable_queue_factory: RunnableQueueFactory | None = None) -> None:
        super().__init__(runnable_queue_factory=runnable_queue_factory)
        self._wakeup = threading.Event()

    # -- Driver wakeup -------------------------------------------------

    def _break_wait_threadsafe(self) -> None:
        self._wakeup.set()

    def _break_wait(self) -> None:
        self._wakeup.set()

    def _wait_thread(self) -> None:
        deadline = self._next_timer_deadline()
        timeout = None if deadline is None else self._delay_until(deadline)
        self._wakeup.wait(timeout=timeout)
        self._wakeup.clear()

    async def _driver_wait(self) -> None:
        self._wait_thread()


Scheduler = importlib.import_module(".proactor", __package__).SyncProactorScheduler

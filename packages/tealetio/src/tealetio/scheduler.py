from __future__ import annotations

import asyncio
import concurrent.futures
import contextvars
import functools
import heapq
import inspect
import itertools
import socket
import threading
import time
import warnings
import weakref
from abc import ABC, abstractmethod
from collections import deque
from collections.abc import Iterable, Iterator
from contextlib import nullcontext
from typing import Any, Callable, ContextManager, Coroutine, Literal, Protocol, TypeAlias, TypeVar, cast

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
from . import tasks as _tasks

try:
    from asynkit import coro_start as _coro_start
    from asynkit import CoroStart as _CoroStart
except ImportError:
    _coro_start = None
    _CoroStart = None


T = TypeVar("T")


DEFAULT_EXECUTOR_SHUTDOWN_TIMEOUT = 300.0

__all__ = [
    "ALL_COMPLETED",
    "BaseScheduler",
    "Channel",
    "CoreSchedulerDrivingAPI",
    "DEFAULT_EXECUTOR_SHUTDOWN_TIMEOUT",
    "DeadlockError",
    "FIRST_COMPLETED",
    "FIRST_EXCEPTION",
    "Scheduler",
    "SyncSchedulerDrivingAPI",
    "TimerHandle",
    "as_completed",
    "ensure_future",
    "gather",
    "get_scheduler",
    "get_running_scheduler",
    "set_scheduler",
    "to_thread",
    "wait",
    "wait_for",
]

FIRST_COMPLETED = "FIRST_COMPLETED"
FIRST_EXCEPTION = "FIRST_EXCEPTION"
ALL_COMPLETED = "ALL_COMPLETED"
_ReturnWhen: TypeAlias = Literal["FIRST_COMPLETED", "FIRST_EXCEPTION", "ALL_COMPLETED"]


# a thread local scheduler
_scheduler = threading.local()


class _FifoRunnableQueue(_tasks.TaskLink):
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
        try:
            task.link = self
        except AttributeError:
            pass
        return True

    def discard(self, task: tealet.tealet) -> bool:
        if task not in self._set:
            return False
        self._set.remove(task)
        try:
            self._items.remove(task)
        except ValueError:
            pass
        try:
            task.link = None
        except AttributeError:
            pass
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

    def tasks(self) -> tuple[_tasks.TealetTask, ...]:
        return tuple(task for task in self._items if isinstance(task, _tasks.TealetTask))

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


class _PrescheduledRunnableQueue(_FifoRunnableQueue):
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
            try:
                task.link = None
            except AttributeError:
                pass
            return True
        return super().discard(task)

    def pop_next(self) -> tealet.tealet:
        if self._prescheduled:
            task = self._prescheduled.popleft()
            self._prescheduled_set.discard(task)
            return task
        return super().pop_next()

    def tasks(self) -> tuple[_tasks.TealetTask, ...]:
        return tuple(
            task
            for task in (*self._prescheduled, *self._items)
            if isinstance(task, _tasks.TealetTask)
        )

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
        try:
            task.link = self
        except AttributeError:
            pass

    def _insert_normal(self, task: tealet.tealet, position: int) -> None:
        self._items.insert(position, task)
        self._set.add(task)
        try:
            task.link = self
        except AttributeError:
            pass

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


class _PriorityRunnableQueue(_PrescheduledRunnableQueue):
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
        try:
            task.link = None
        except AttributeError:
            pass
        return True

    def pop_next(self) -> tealet.tealet:
        if self._prescheduled:
            return super().pop_next()
        _, _, task = heapq.heappop(self._priority_items)
        self._set.discard(task)
        return task

    def tasks(self) -> tuple[_tasks.TealetTask, ...]:
        return tuple(
            task
            for task in (*self._prescheduled, *(entry[2] for entry in sorted(self._priority_items)))
            if isinstance(task, _tasks.TealetTask)
        )

    def _active_priority(self, task: tealet.tealet) -> Any:
        try:
            return cast(Any, task).get_active_priority()
        except AttributeError:
            if isinstance(task, _tasks.TealetTask):
                return _tasks.TASK_PRIORITY_DEFAULT
            # external tealets such as the main runner must sort after tasks so
            # every scheduler-owned task can run before control returns to caller.
            return float("inf")

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
        try:
            task.link = self
        except AttributeError:
            pass

    def on_modified(self, task: tealet.tealet) -> None:
        if task in self._prescheduled_set:
            return
        if task not in self._set:
            return
        self._remove_normal(task)
        self._insert_normal(task, len(self._priority_items))


class _RunnableQueue(Protocol):
    """Scheduler-facing interface for runnable queue implementations."""

    def __bool__(self) -> bool: ...

    def __contains__(self, task: tealet.tealet) -> bool: ...

    def add(self, task: tealet.tealet) -> bool: ...

    def discard(self, task: tealet.tealet) -> bool: ...

    def pop_next(self) -> tealet.tealet: ...

    def tasks(self) -> tuple[_tasks.TealetTask, ...]: ...

    def reschedule(self, task: tealet.tealet, position: int | None) -> None: ...

    def yield_to(self, target: tealet.tealet, current: tealet.tealet, insert_current_at: int | None) -> None: ...


_RunnableQueueFactory: TypeAlias = Callable[[], _RunnableQueue]


class CoreSchedulerDrivingAPI(ABC):
    """Common control surface shared by sync and async scheduler drivers."""

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
    ) -> "_tasks.TealetTask":
        """Spawn a scheduler-managed task from a zero-arg callable."""


class SyncSchedulerDrivingAPI(CoreSchedulerDrivingAPI, ABC):
    """Synchronous scheduler driver API."""

    @abstractmethod
    def stop(self) -> None:
        """Stop a currently running sync driver."""

    @abstractmethod
    def run(self) -> None:
        """Run until no local runnable tasks or timers remain."""

    @abstractmethod
    def run_forever(self) -> None:
        """Run until stop() is called."""

    @abstractmethod
    def run_until_complete(self, future: "_tasks.Future[T] | Callable[[], T]") -> T:
        """Run until a target future/callable completes."""


def set_scheduler(value: "BaseScheduler | None") -> None:
    if value is None:
        if hasattr(_scheduler, "instance"):
            del _scheduler.instance
        return
    _scheduler.instance = value


def get_running_scheduler() -> "BaseScheduler":
    current = _current_scheduler()
    if current is not None and current.is_running():
        return current
    raise RuntimeError("no running scheduler")


set_scheduler_resolver(get_running_scheduler)


def _current_scheduler() -> "BaseScheduler | None":
    return getattr(_scheduler, "instance", None)


def get_scheduler() -> "BaseScheduler":
    current = _current_scheduler()
    if current is None:
        raise RuntimeError("no current scheduler")
    return current


def to_thread(func: Callable[..., T], /, *args: object, **kwargs: object) -> T:
    """Run a callable in the scheduler default thread pool and wait for its result."""

    context = contextvars.copy_context()
    call = functools.partial(context.run, func, *args, **kwargs)
    return get_running_scheduler().run_in_executor(None, call).wait()


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
        return self._balance

    @property
    def preference(self) -> int:
        return self._preference

    @preference.setter
    def preference(self, value: int) -> None:
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
        if isinstance(waiter, _tasks.TealetTask):
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
        return self._when

    def cancel(self) -> None:
        self._cancelled = True

    def cancelled(self) -> bool:
        return self._cancelled

    # -- Execution -----------------------------------------------------

    def _run(self) -> None:
        if self._cancelled:
            return
        if self._context is None:
            self._callback(*self._args)
            return
        self._context.run(self._callback, *self._args)

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
    scheduler = get_scheduler()
    return scheduler.ensure_future(entry)


def wait(
    entries: Iterable[_tasks.Future[Any] | Callable[[], Any]],
    *,
    timeout: float | None = None,
    return_when: _ReturnWhen = ALL_COMPLETED,
) -> _tasks.Future[tuple[set[_tasks.Future[Any]], set[_tasks.Future[Any]]]]:
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
        with (scheduler_timeout(timeout) if timeout is not None else nullcontext()):
            while True:
                child = completed.sget()
                yield cast(_tasks.Future[Any], child)
    except QueueShutDown:
        return
    finally:
        for child in pending_callbacks:
            child.remove_done_callback(complete_next)


def wait_for(
    entry: _tasks.Future[Any] | Callable[[], Any],
    timeout: float | None,
) -> _tasks.Future[Any]:
    scheduler = get_scheduler()
    child = scheduler.ensure_future(entry)

    def wait_task() -> Any:
        try:
            with (scheduler_timeout(timeout) if timeout is not None else nullcontext()):
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
    scheduler = get_scheduler()
    children = (scheduler.ensure_future(entry) for entry in entries)
    return _as_completed_futures(children, timeout=timeout)


class BaseScheduler(_tasks.TaskLink, CoreSchedulerDrivingAPI):
    """Shared cooperative scheduler mechanics for concrete drivers."""

    def __init__(self, *, runnable_queue_factory: _RunnableQueueFactory | None = None) -> None:
        if runnable_queue_factory is None:
            runnable_queue_factory = _PrescheduledRunnableQueue
        self._runnable = runnable_queue_factory()
        self._all_tasks: weakref.WeakSet[_tasks.TealetTask] = weakref.WeakSet()
        self._runner = None
        self._running = False
        self._debug = False
        self._stopping = False
        self._threadsafe_callbacks: deque[
            tuple[Callable[..., object], tuple[object, ...], contextvars.Context | None]
        ] = deque()
        self._threadsafe_lock = threading.Lock()
        self._pending_executor_calls = 0
        self._pending_async_waits: set[tealet.tealet] = set()
        self._timers: list[tuple[float, int, TimerHandle]] = []
        self._timer_sequence = itertools.count()
        self._n_scheduled = 0
        self._target_count = None
        self._default_executor: concurrent.futures.ThreadPoolExecutor | None = None
        self._task_factory: _tasks.TaskFactory = _tasks.DefaultTaskFactory()

    # -- Basic state ---------------------------------------------------

    def time(self) -> float:
        return time.monotonic()

    def is_running(self) -> bool:
        return self._running

    def set_debug(self, enabled: bool) -> None:
        self._debug = bool(enabled)

    def get_debug(self) -> bool:
        return self._debug

    def all_tasks(self) -> set[_tasks.TealetTask]:
        """Return unfinished scheduler-owned tasks."""

        return {task for task in self._all_tasks if not task.done()}

    def runnable_tasks(self) -> tuple[_tasks.TealetTask, ...]:
        """Return scheduler-owned tasks currently waiting to run."""

        return self._runnable.tasks()

    def get_task_factory(self) -> _tasks.TaskFactory:
        return self._task_factory

    def set_task_factory(self, factory: _tasks.TaskFactory | None) -> None:
        self._task_factory = _tasks.DefaultTaskFactory() if factory is None else factory

    def main_context(self) -> ContextManager[None]:
        """Use this scheduler's task factory for the current main tealet wrapper."""
        return _tasks.scheduler_tealet_factory(self)

    def close(self) -> None:
        executor = self._default_executor
        if executor is not None:
            self._default_executor = None
            executor.shutdown(wait=False)

    def shutdown_default_executor(
        self,
        timeout: float | None = DEFAULT_EXECUTOR_SHUTDOWN_TIMEOUT,
    ) -> _tasks.Future[Any]:
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
                with (scheduler_timeout(_timeout) if _timeout is not None else nullcontext()):
                    future.wait()
            except TimeoutError:
                warnings.warn(
                    "The executor did not finish joining its threads "
                    f"within {_timeout} seconds.",
                    RuntimeWarning,
                    stacklevel=2,
                )
                executor.shutdown(wait=False)

        return self.spawn(wait_for_shutdown)

    # -- External integration APIs ------------------------------------

    def run_in_executor(
        self,
        executor: concurrent.futures.Executor | None,
        func: Callable[..., T],
        *args: object,
    ) -> _tasks.Future[T]:
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
        raise NotImplementedError("reader callbacks require an IO-capable scheduler")

    def remove_reader(self, fd: int) -> bool:
        raise NotImplementedError("reader callbacks require an IO-capable scheduler")

    def add_writer(self, fd: int, callback: Callable[..., object], *args: object) -> None:
        raise NotImplementedError("writer callbacks require an IO-capable scheduler")

    def remove_writer(self, fd: int) -> bool:
        raise NotImplementedError("writer callbacks require an IO-capable scheduler")

    def sock_recv(self, sock: socket.socket, n: int) -> bytes:
        raise NotImplementedError("socket helpers require an IO-capable scheduler")

    def sock_recv_into(self, sock: socket.socket, buf: Any) -> int:
        raise NotImplementedError("socket helpers require an IO-capable scheduler")

    def sock_recvfrom(self, sock: socket.socket, bufsize: int) -> tuple[bytes, Any]:
        raise NotImplementedError("socket helpers require an IO-capable scheduler")

    def sock_recvfrom_into(self, sock: socket.socket, buf: Any, nbytes: int = 0) -> tuple[int, Any]:
        raise NotImplementedError("socket helpers require an IO-capable scheduler")

    def sock_sendall(self, sock: socket.socket, data: Any) -> None:
        raise NotImplementedError("socket helpers require an IO-capable scheduler")

    def sock_sendto(self, sock: socket.socket, data: Any, address: Any) -> int:
        raise NotImplementedError("socket helpers require an IO-capable scheduler")

    def sock_accept(self, sock: socket.socket) -> tuple[socket.socket, Any]:
        raise NotImplementedError("socket helpers require an IO-capable scheduler")

    def sock_connect(self, sock: socket.socket, address: Any) -> None:
        raise NotImplementedError("socket helpers require an IO-capable scheduler")

    # -- Driver state --------------------------------------------------

    def _verify_current_scheduler(self) -> None:
        if _current_scheduler() is not self:
            raise RuntimeError("operation requires this scheduler to be the current scheduler")
        if self._running:
            raise RuntimeError("Scheduler already running")

    def stop(self) -> None:
        self._stopping = True
        self._break_wait()

    # -- Callback and timer scheduling --------------------------------

    def call_soon(
        self,
        callback: Callable[..., object],
        *args: object,
        context: contextvars.Context | None = None,
    ) -> TimerHandle:
        return self.call_at(self.time(), callback, *args, context=context)

    def call_soon_threadsafe(
        self,
        callback: Callable[..., object],
        *args: object,
        context: contextvars.Context | None = None,
    ) -> None:
        if context is None:
            context = contextvars.copy_context()
        with self._threadsafe_lock:
            self._threadsafe_callbacks.append((callback, args, context))
        self._break_wait_threadsafe()

    def call_later(
        self,
        delay: float,
        callback: Callable[..., object],
        *args: object,
        context: contextvars.Context | None = None,
    ) -> TimerHandle:
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
        if context is None:
            context = contextvars.copy_context()
        handle = TimerHandle(when, callback, args, context=context)
        self._enqueue_timer(when, handle)
        return handle

    def _enqueue_timer(self, when: float, handle: TimerHandle) -> None:
        heapq.heappush(self._timers, (when, next(self._timer_sequence), handle))
        self._break_wait()

    def _drain_threadsafe_callbacks(self) -> None:
        while True:
            with self._threadsafe_lock:
                if not self._threadsafe_callbacks:
                    return
                callback, args, context = self._threadsafe_callbacks.popleft()
            if context is None:
                callback(*args)
            else:
                context.run(callback, *args)

    def _run_ready_timers(self) -> None:
        self._drain_threadsafe_callbacks()
        now = self.time()
        while self._timers and self._timers[0][0] <= now:
            _, _, handle = heapq.heappop(self._timers)
            handle._run()

    def _time_to_next_timer(self) -> float | None:
        while self._timers and self._timers[0][2].cancelled():
            heapq.heappop(self._timers)
        if not self._timers:
            return None
        return max(0.0, self._timers[0][0] - self.time())

    def _has_pending_driver_work(self) -> bool:
        with self._threadsafe_lock:
            return bool(self._threadsafe_callbacks or self._pending_executor_calls)

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
        if isinstance(entry, _tasks.Future):
            if isinstance(entry, _tasks.TealetTask) and entry.get_scheduler() is not self:
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
    ) -> _tasks.TealetTask:
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
        self._n_scheduled += 1
        target.switch()

    def yield_(self) -> None:
        self._schedule(lambda: self._make_runnable(tealet.current()))

    def sleep(self, delay: float) -> None:
        current = tealet.current()
        awakened = False

        def wake() -> None:
            nonlocal awakened
            awakened = True
            self._make_runnable(current)

        with self.call_later(delay, wake):
            if not awakened:
                self._schedule()

    def await_(self, awaitable):
        """Await an asyncio awaitable from a tealet task and return its result."""
        current = tealet.current()
        loop = asyncio.get_running_loop()

        if asyncio.isfuture(awaitable):
            fut = awaitable
            if fut.get_loop() is not loop:
                raise RuntimeError("await_ future is bound to a different event loop")
        elif inspect.iscoroutine(awaitable) and _coro_start is not None:
            # run in a copy of the current context, outside tealetio task scope
            context = _tasks._copy_context_without_current_task()
            coro_start = _coro_start(_CoroStart, cast(Coroutine[Any, Any, Any], awaitable), context)
            if coro_start.done():
                return coro_start.result()
            fut = loop.create_task(coro_start.as_coroutine())
        elif inspect.isawaitable(awaitable):
            # run in a copy of the current context if possible, outside tealetio task scope
            context = _tasks._copy_context_without_current_task()
            try:
                fut = loop.create_task(cast(Coroutine[Any, Any, Any], awaitable), context=context)
            except TypeError:
                fut = loop.create_task(cast(Coroutine[Any, Any, Any], awaitable))
        else:
            raise TypeError("awaitable must be an awaitable")

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
        if isinstance(t, _tasks.TealetTask):
            t._scheduler = self
        self._runnable.add(t)
        self._break_wait()

    def reschedule(self, task: _tasks.TealetTask, *, position: int | None = None) -> None:
        """Move a runnable scheduler task to a new runnable queue position."""
        if task.get_scheduler() is not self:
            raise RuntimeError("task is bound to a different scheduler")
        self._runnable.reschedule(task, position)
        self._break_wait()

    def yield_to(self, task: _tasks.TealetTask, *, insert_current_at: int | None = None) -> None:
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
        try:
            target._unlink()
        except AttributeError:
            raise RuntimeError(f"Cannot throw to this target: {target}") from None
        self._make_runnable(tealet.current())
        target.switch()

    def _target_throw(self, target: tealet.tealet, exc: BaseException) -> None:
        if target is tealet.current():
            raise exc
        try:
            target._unlink()
        except AttributeError:
            raise RuntimeError(f"Cannot throw to this target: {target}") from None
        self._make_runnable(tealet.current())
        if isinstance(target, _tasks.TealetTask):
            target._throw_from_scheduler(exc)
        else:
            target.throw(exc)

    def _find_target(self, task_exit=False) -> tealet.tealet:
        if self._runner is not None and self._target_count is not None and self._n_scheduled >= self._target_count:
            result = self._runner
            try:
                result._unlink()
            except AttributeError:
                self._unlink(result)
        elif self._has_runnable_work():
            result = self._pop_next_runnable()
        elif not task_exit:
            raise DeadlockError("No tasks to switch to")
        else:
            result = tealet.main()
        try:
            result.link = None
        except AttributeError:
            pass
        return result

    def _pump(self, n=0) -> int:
        if self._runner is not None:
            raise RuntimeError("Scheduler already running")
        start_count = self._n_scheduled
        if n > 0:
            self._target_count = start_count + n
        self._runner = tealet.current()
        try:
            self.yield_()
            return self._n_scheduled - start_count - 1
        finally:
            self._runner = None
            self._target_count = None

    def pump(self, n=0) -> int:
        self._verify_current_scheduler()
        self._running = True
        try:
            return self._pump(n)
        finally:
            self._running = False

    # -- Concrete driver hooks ----------------------------------------

    @abstractmethod
    def _break_wait_threadsafe(self) -> None:
        """Wake a concrete driver from another thread or scheduler context."""

    @abstractmethod
    def _break_wait(self) -> None:
        """Wake a concrete driver from its owning context."""


class Scheduler(BaseScheduler, SyncSchedulerDrivingAPI):
    """Cooperative scheduler for synchronous driving."""

    def __init__(self, *, runnable_queue_factory: _RunnableQueueFactory | None = None) -> None:
        super().__init__(runnable_queue_factory=runnable_queue_factory)
        self._wakeup = threading.Event()

    # -- Driver wakeup -------------------------------------------------

    def _break_wait_threadsafe(self) -> None:
        self._wakeup.set()

    def _break_wait(self) -> None:
        self._wakeup.set()

    def _wait_thread(self) -> None:
        self._wakeup.wait(timeout=self._time_to_next_timer())
        self._wakeup.clear()

    # -- Sync run entry points ----------------------------------------

    def run(self) -> None:
        """Run scheduler synchronously until no runnable tasks or timers remain.
        This method is intended for single threaded context with no
        asyncio loop interaction.

        This sync runner only considers local runnable state and
        scheduled timer callbacks (`_timers`). Tealets blocked in
        `await_()` are not progressed here; use `arun()` for that mode.
        """
        self._verify_current_scheduler()
        with self.main_context(), _tasks.task_priority(tealet.current(), _tasks.TEALET_PRI_INF):
            self._running = True
            try:
                while self._has_runnable_work() or self._timers or self._has_pending_driver_work():
                    self._run_ready_timers()
                    if self._has_runnable_work():
                        self._pump()
                    if self._has_runnable_work() or self._timers or self._has_pending_driver_work():
                        self._wait_thread()
            finally:
                self._running = False

    def run_forever(self) -> None:
        self._verify_current_scheduler()
        with self.main_context(), _tasks.task_priority(tealet.current(), _tasks.TEALET_PRI_INF):
            self._stopping = False
            self._running = True
            try:
                while not self._stopping:
                    self._run_ready_timers()
                    if self._has_runnable_work():
                        self._pump()
                        continue
                    self._wait_thread()
            finally:
                self._running = False
                self._stopping = False

    def run_until_complete(
        self,
        future: _tasks.Future[T] | Callable[[], T],
    ) -> T:
        self._verify_current_scheduler()
        with self.main_context():
            if isinstance(future, _tasks.Future):
                target: _tasks.Future[T] = future
                if isinstance(target, _tasks.TealetTask) and target.get_scheduler() is not self:
                    raise RuntimeError("Future is bound to a different scheduler")
            elif callable(future):
                target = self.spawn(future)
            else:
                raise TypeError("future must be a Future or callable")

            return self._run_until_complete_target(target)

    def _run_until_complete_target(self, target: _tasks.Future[T]) -> T:
        with _tasks.task_priority(tealet.current(), _tasks.TEALET_PRI_INF):
            self._stopping = False
            self._running = True
            try:
                while not target.done() and not self._stopping:
                    self._run_ready_timers()
                    if self._has_runnable_work():
                        self._pump()
                    if not target.done() and not self._stopping:
                        self._wait_thread()
            finally:
                self._running = False
                self._stopping = False

        if not target.done():
            raise RuntimeError("Scheduler stopped before Future completed.")
        return target.result()

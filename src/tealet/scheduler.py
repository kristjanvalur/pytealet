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
from typing import Any, Callable, TypeVar

import tealet

from .locks import (
    Event,
    RawTimeoutError,
    TimeoutError,
    set_scheduler_resolver,
    timeout as scheduler_timeout,
)
from . import tasks as _tasks

T = TypeVar("T")


DEFAULT_EXECUTOR_SHUTDOWN_TIMEOUT = 300.0

__all__ = [
    "Channel",
    "DeadlockError",
    "Scheduler",
    "TimerHandle",
    "gather",
    "get_running_scheduler",
    "set_scheduler",
    "to_thread",
]


# a thread local scheduler
_scheduler = threading.local()


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
    def shutdown_default_executor(
        self,
        timeout: float | None = DEFAULT_EXECUTOR_SHUTDOWN_TIMEOUT,
    ) -> "_tasks.Future[Any]":
        """Return a future that completes after the default executor shuts down."""

    @abstractmethod
    def spawn(
        self,
        func: Callable[[], T],
        kwargs: dict[str, object] | None = None,
        context: contextvars.Context | None = None,
        eager: bool | None = None,
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


def to_thread(func: Callable[..., T], /, *args: object, **kwargs: object) -> T:
    """Run a callable in the scheduler default thread pool and wait for its result."""

    context = contextvars.copy_context()
    call = functools.partial(context.run, func, *args, **kwargs)
    return get_running_scheduler().run_in_executor(None, call).wait()


class Channel(_tasks.Linkable):
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
            raise payload
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

    def _query_waiting(self, t: tealet.tealet) -> bool:
        return t in self._waiters

    def _query_runnable(self, t: tealet.tealet) -> bool:
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
                return self._deliver(packet)
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
            await waiter.async_wait()
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
            await waiter.async_wait()
        except BaseException as exc:
            # Async timeouts surface as cancellation at this await point.
            # We cannot reliably distinguish timeout cancellation from an
            # explicit task cancellation here. If a packet is already queued,
            # prefer delivering data over propagating cancellation.
            missing = object()
            packet = self._packets.pop(waiter, missing)
            self._unlink_waiter(waiter)
            if packet is not missing and isinstance(exc, _tasks.CancelledError):
                return self._deliver(packet)
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


class _GatheringFuture(_tasks.Future[list[object]]):
    def __init__(self, children: list[_tasks.Future[object]]) -> None:
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
    *entries: _tasks.Future[object] | Callable[[], object],
    return_exceptions: bool = False,
) -> _tasks.Future[list[object]]:
    scheduler = _current_scheduler()
    if scheduler is None:
        raise RuntimeError("no current scheduler")

    children: list[_tasks.Future[object]] = []
    for entry in entries:
        if isinstance(entry, _tasks.Future):
            if isinstance(entry, _tasks.TealetTask) and entry.get_scheduler() is not scheduler:
                raise RuntimeError("Future is bound to a different scheduler")
            children.append(entry)
        elif callable(entry):
            children.append(scheduler.spawn(entry))
        else:
            raise TypeError("gather arguments must be Futures or callables")

    gather_future = _GatheringFuture(children)
    if not children:
        gather_future.set_result([])
        return gather_future

    results: list[object] = [None] * len(children)
    remaining = len(children)

    def child_done(index: int, child: _tasks.Future[object]) -> None:
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


class BaseScheduler(_tasks.Linkable, CoreSchedulerDrivingAPI):
    """Shared cooperative scheduler mechanics for concrete drivers."""

    def __init__(self) -> None:
        self._tasks: deque[tealet.tealet] = deque()
        self._task_set: set[tealet.tealet] = set()
        self._all_tasks: weakref.WeakSet[_tasks.TealetTask] = weakref.WeakSet()
        self._runner = None
        self._running = False
        self._debug = False
        self._stopping = False
        self._threadsafe_callbacks: deque[
            tuple[Callable[..., object], tuple[object, ...], contextvars.Context | None]
        ] = deque()
        self._threadsafe_lock = threading.Lock()
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

    def get_task_factory(self) -> _tasks.TaskFactory:
        return self._task_factory

    def set_task_factory(self, factory: _tasks.TaskFactory | None) -> None:
        self._task_factory = _tasks.DefaultTaskFactory() if factory is None else factory

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
                if _timeout is None:
                    future.wait()
                else:
                    with scheduler_timeout(_timeout):
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

        def complete_result(value: T) -> None:
            if not future.done():
                future.set_result(value)

        def complete_exception(exc: BaseException) -> None:
            if not future.done():
                future.set_exception(exc)

        def worker() -> None:
            try:
                result = func(*args)
            except BaseException as exc:
                self.call_soon_threadsafe(complete_exception, exc)
            else:
                self.call_soon_threadsafe(complete_result, result)

        executor.submit(worker)
        return future

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
        return False

    # -- Link and runnable state --------------------------------------

    def _is_runnable(self, t: tealet.tealet) -> bool:
        return t in self._task_set

    def _query_runnable(self, t: tealet.tealet) -> bool:
        return self._is_runnable(t)

    def _query_waiting(self, t: tealet.tealet) -> bool:
        return False

    def _is_blocked(self, t: tealet.tealet) -> bool:
        return t in self._pending_async_waits

    def _unlink_pending_async_wait(self, t: tealet.tealet) -> None:
        self._pending_async_waits.discard(t)

    def _unlink(self, t: tealet.tealet) -> None:
        removed = False
        if t in self._task_set:
            self._task_set.remove(t)
            try:
                self._tasks.remove(t)
            except ValueError:
                pass
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

    def spawn(
        self,
        func: Callable[[], T],
        kwargs: dict[str, object] | None = None,
        context: contextvars.Context | None = None,
        eager: bool | None = None,
    ) -> _tasks.TealetTask:
        if context is None:
            context = contextvars.copy_context()
        if kwargs is not None:
            raise TypeError("spawn() does not accept callable kwargs")

        t = self._task_factory(self, func, context=context, eager=eager)
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

    def wait_async(self, awaitable):
        """Wait for an asyncio awaitable from a tealet task and return its result."""
        current = tealet.current()
        loop = asyncio.get_running_loop()

        if asyncio.isfuture(awaitable):
            fut = awaitable
            if fut.get_loop() is not loop:
                raise RuntimeError("wait_async future is bound to a different event loop")
        elif inspect.isawaitable(awaitable):
            fut = loop.create_task(awaitable)
        else:
            raise TypeError("awaitable must be an awaitable, Future, or Task")

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
                done_evt.wait()
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
        if t in self._task_set:
            return
        try:
            t.link = self
        except AttributeError:
            pass
        if isinstance(t, _tasks.TealetTask):
            t._scheduler = self
        self._tasks.append(t)
        self._task_set.add(t)
        self._break_wait()

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
            self._unlink(result)
        elif self._tasks:
            result = self._tasks.popleft()
            self._task_set.discard(result)
        elif not task_exit:
            raise DeadlockError("No tasks to switch to")
        else:
            result = tealet.main()
        try:
            result.link = None
        except AttributeError:
            pass
        return result

    def _pump(self, n=0) -> None:
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

    def pump(self, n=0) -> None:
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

    def __init__(self) -> None:
        super().__init__()
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

        This sync runner only considers local runnable state (`_tasks`) and
        scheduled timer callbacks (`_timers`). Tealets blocked in
        `wait_async()` are not progressed here; use `arun()` for that mode.
        """
        self._verify_current_scheduler()
        self._running = True
        try:
            while self._tasks or self._timers or self._has_pending_driver_work():
                self._run_ready_timers()
                if self._tasks:
                    self._pump()
                if self._tasks or self._timers or self._has_pending_driver_work():
                    self._wait_thread()
        finally:
            self._running = False

    def run_forever(self) -> None:
        self._verify_current_scheduler()
        self._stopping = False
        self._running = True
        try:
            while not self._stopping:
                self._run_ready_timers()
                if self._tasks:
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
        if isinstance(future, _tasks.Future):
            target: _tasks.Future[T] = future
            if isinstance(target, _tasks.TealetTask) and target.get_scheduler() is not self:
                raise RuntimeError("Future is bound to a different scheduler")
        elif callable(future):
            target = self.spawn(future)
        else:
            raise TypeError("future must be a Future or callable")

        self._stopping = False
        self._running = True
        try:
            while not target.done() and not self._stopping:
                self._run_ready_timers()
                if self._tasks:
                    self._pump()
                if not target.done() and not self._stopping:
                    self._wait_thread()
        finally:
            self._running = False
            self._stopping = False

        if not target.done():
            raise RuntimeError("Scheduler stopped before Future completed.")
        return target.result()

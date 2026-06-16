from __future__ import annotations

import asyncio
import heapq
import inspect
import itertools
import threading
import time
from abc import ABC, abstractmethod
from collections import deque
from typing import Callable, Generic, TypeVar

import tealet

from .locks import Event, InvalidStateError, set_scheduler_resolver

T = TypeVar("T")


# a thread local scheduler
_scheduler = threading.local()
_default_scheduler_factory: Callable[[], SimpleScheduler] = lambda: SimpleScheduler()


class Linkable(ABC):
    """Base interface for objects that can be linked from a TealetTask."""

    @abstractmethod
    def _unlink(self, t: tealet.tealet) -> None:
        """Detach a tealet from this link target."""

    def _query_waiting(self, t: tealet.tealet) -> bool:
        return False

    def _query_runnable(self, t: tealet.tealet) -> bool:
        return False


class BaseScheduler(Linkable, ABC):
    """Base scheduler surface used for scheduler-aware type annotations."""

    @abstractmethod
    def _is_runnable(self, t: tealet.tealet) -> bool:
        """Return whether the target tealet is currently runnable."""

    @abstractmethod
    def _is_blocked(self, t: tealet.tealet) -> bool:
        """Return whether the target tealet is blocked on async wait."""

    @abstractmethod
    def _unlink_pending_async_wait(self, t: tealet.tealet) -> None:
        """Remove pending async wait bookkeeping for a tealet."""

    @abstractmethod
    def _make_runnable(self, t: tealet.tealet) -> None:
        """Queue a tealet to run."""

    @abstractmethod
    def _target_run(self, target: tealet.tealet) -> None:
        """Run the target tealet from the scheduler context."""

    @abstractmethod
    def _target_throw(self, target: tealet.tealet, exc: BaseException) -> None:
        """Throw into the target tealet from the scheduler context."""

    @abstractmethod
    def _find_target(self, task_exit: bool = False) -> tealet.tealet:
        """Find the next scheduling target."""

    @abstractmethod
    def call_soon_threadsafe(self, callback: Callable[..., object], *args: object) -> None:
        """Schedule a callback from another scheduler/thread context."""

def scheduler() -> SimpleScheduler:
    return get_scheduler()


def new_scheduler() -> SimpleScheduler:
    created = _default_scheduler_factory()
    if not isinstance(created, SimpleScheduler):
        raise TypeError("scheduler factory must return a SimpleScheduler instance")
    return created


def set_default_scheduler_factory(factory: Callable[[], SimpleScheduler] | None) -> None:
    global _default_scheduler_factory
    if factory is None:
        _default_scheduler_factory = lambda: SimpleScheduler()
        return
    _default_scheduler_factory = factory


def get_default_scheduler_factory() -> Callable[[], SimpleScheduler]:
    return _default_scheduler_factory


def set_scheduler(value: SimpleScheduler | None) -> None:
    if value is None:
        if hasattr(_scheduler, "instance"):
            del _scheduler.instance
        return
    _scheduler.instance = value


def get_scheduler() -> SimpleScheduler:
    current = getattr(_scheduler, "instance", None)
    if current is None:
        current = new_scheduler()
        _scheduler.instance = current
    return current


def get_running_scheduler() -> SimpleScheduler:
    current = _current_scheduler()
    if current is not None and current.is_running():
        return current
    raise RuntimeError("no running scheduler")


set_scheduler_resolver(get_scheduler)


def _current_scheduler() -> SimpleScheduler | None:
    return getattr(_scheduler, "instance", None)


class Channel(Linkable):
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
        if isinstance(waiter, TealetTask):
            return waiter.get_scheduler()
        return scheduler()

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
            scheduler()._schedule(lambda: self._link_sender(current, packet))
        except BaseException as exc:
            missing = object()
            pending = self._packets.pop(current, missing)
            self._unlink_waiter(current)
            if pending is missing and isinstance(exc, RawTimeoutError):
                # Timeout-vs-delivery race: if receiver already consumed packet,
                # treat send as successful and suppress timeout.
                return
            raise

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
            if pending is missing and isinstance(exc, CancelledError):
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
            scheduler()._schedule(lambda: self._link_receiver(current))
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
            if packet is not missing and isinstance(exc, CancelledError):
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
    ) -> None:
        self._when = when
        self._callback = callback
        self._args = args
        self._cancelled = False

    @property
    def when(self) -> float:
        return self._when

    def cancel(self) -> None:
        self._cancelled = True

    def cancelled(self) -> bool:
        return self._cancelled

    def _run(self) -> None:
        if self._cancelled:
            return
        self._callback(*self._args)

    def __enter__(self) -> "TimerHandle":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.cancel()


class Future(Generic[T]):
    """Minimal Future for scheduler tasks."""

    def __init__(self) -> None:
        self._done = False
        self._cancelled = False
        self._result: T | None = None
        self._exception: BaseException | None = None
        self._event = Event()
        self._done_callbacks: list[Callable[[Future[T]], object]] = []

    def done(self) -> bool:
        return self._done

    def cancelled(self) -> bool:
        return self._cancelled

    def cancel(self) -> bool:
        if self._done:
            return False
        self._cancelled = True
        self._done = True
        self._event.set()
        self._run_done_callbacks()
        return True

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

    def add_done_callback(self, callback: Callable[[Future[T]], object]) -> None:
        if self._done:
            callback(self)
            return
        self._done_callbacks.append(callback)

    def remove_done_callback(self, callback: Callable[[Future[T]], object]) -> int:
        removed = 0
        while True:
            try:
                self._done_callbacks.remove(callback)
            except ValueError:
                break
            removed += 1
        return removed

    def _run_done_callbacks(self) -> None:
        callbacks = self._done_callbacks[:]
        self._done_callbacks.clear()
        for callback in callbacks:
            callback(self)

    def _wait(self) -> bool:
        if self._done:
            return True

        return self._event.wait()

    def wait(self) -> None:
        self._wait()
        if self._cancelled:
            raise CancelledError()

    async def async_wait(self) -> None:
        if not self._done:
            await self._event.async_wait()
        if self._cancelled:
            raise CancelledError()

    def __await__(self):
        return self.async_wait().__await__()

    def result(self) -> T:
        if not self._done:
            raise InvalidStateError("Result is not ready.")
        if self._cancelled:
            raise CancelledError()
        if self._exception is not None:
            raise self._exception
        return self._result

    def exception(self) -> BaseException | None:
        if not self._done:
            raise InvalidStateError("Exception is not set.")
        if self._cancelled:
            raise CancelledError()
        return self._exception


class TealetTask(tealet.tealet, Future[object]):
    """Tealet task that is also a Future for its completion result."""

    def __init__(self, owning_scheduler: BaseScheduler):
        tealet.tealet.__init__(self)
        Future.__init__(self)
        self.link: Linkable | None = None
        self._scheduler: BaseScheduler = owning_scheduler

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

    def resolve_target(self, result, exc, exc_target):
        clear = False
        if exc is None:
            self.set_result(result)
        else:
            self.set_exception(exc)
            clear = True
            if exc_target is not None:
                try:
                    exc_target._unlink()
                except AttributeError:
                    pass
                return exc_target, None, clear

        # Scheduler-owned tasks always route via scheduler target selection,
        # even if task startup immediately raises before user code returns.
        return self._scheduler._find_target(task_exit=True), None, clear


class RawTimeoutError(BaseException):
    pass


TimeoutError = asyncio.TimeoutError
CancelledError = asyncio.CancelledError


class Timeout:
    def __init__(self, when: float):
        self._when = when
        self._handle: TimerHandle | None = None
        self._exc = RawTimeoutError()
        self._expired = False

    def reschedule(self, when: float):
        if not self._expired and self._handle is not None:
            self._handle.cancel()
            self._when = when
            self._handle = scheduler().call_at(self._when, self._timeout, tealet.current())

    def expired(self) -> bool:
        return self._expired

    def __enter__(self) -> "Timeout":
        self._handle = scheduler().call_at(self._when, self._timeout, tealet.current())
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._handle is not None:
            self._handle.cancel()
        if exc_val is self._exc:
            assert self._expired is True
            raise asyncio.TimeoutError("Operation timed out") from exc_val

    def _timeout(self, target) -> None:
        self._expired = True
        target.throw(self._exc)


def timeout(delay: float) -> Timeout:
    """Context manager for timing out a block of code via scheduler timers."""
    when = scheduler().time() + delay
    return Timeout(when)


def timeout_at(when: float) -> Timeout:
    """Context manager for timing out a block of code at a specific time via scheduler timers."""
    return Timeout(when)


class _ThreadIdleWaiter:
    def __init__(self, scheduler: SimpleScheduler) -> None:
        self._scheduler = scheduler
        self._wakeup = threading.Event()

    def break_local(self) -> None:
        self._wakeup.set()

    def break_threadsafe(self) -> None:
        self._wakeup.set()

    def wait_for(self, timeout: float | None) -> None:
        self._wakeup.wait(timeout=timeout)
        self._wakeup.clear()


class _AsyncIdleWaiter:
    def __init__(self, scheduler: SimpleScheduler) -> None:
        self._scheduler = scheduler
        self._awakeup = asyncio.Event()

    def break_local(self) -> None:
        self._awakeup.set()

    def break_threadsafe(self) -> None:
        loop = self._scheduler._loop
        if loop is not None:
            loop.call_soon_threadsafe(self._awakeup.set)
        elif _current_scheduler() is self._scheduler:
            self._awakeup.set()

    async def wait_for(self, timeout: float | None) -> None:
        if timeout is None:
            # No scheduler timer is pending. We may still be alive because one or
            # more tealets are blocked in wait_async() on external asyncio
            # awaitables, so block until an explicit wakeup arrives.
            await self._awakeup.wait()
            self._awakeup.clear()
            return
        try:
            async with asyncio.timeout(timeout):
                await self._awakeup.wait()
        except TimeoutError:
            pass
        finally:
            self._awakeup.clear()


class SimpleScheduler(BaseScheduler):
    """Very small cooperative scheduler for runnable tealets."""

    def __init__(self) -> None:
        self._tasks: deque[tealet.tealet] = deque()
        self._task_set: set[tealet.tealet] = set()
        self._runner = None
        self._running = False
        self._stopping = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._threadsafe_callbacks: deque[tuple[Callable[..., object], tuple[object, ...]]] = deque()
        self._threadsafe_lock = threading.Lock()
        self._pending_async_waits: set[tealet.tealet] = set()
        self._timers: list[tuple[float, int, TimerHandle]] = []
        self._timer_sequence = itertools.count()
        self._n_scheduled = 0
        self._target_count = None
        self._thread_idle_waiter = _ThreadIdleWaiter(self)
        self._async_idle_waiter = _AsyncIdleWaiter(self)
        self._idle_waiter: _ThreadIdleWaiter | _AsyncIdleWaiter = self._thread_idle_waiter

    def time(self) -> float:
        return time.monotonic()

    def is_running(self) -> bool:
        return self._running

    def _verify_current_scheduler(self) -> None:
        if _current_scheduler() is not self:
            raise RuntimeError("operation requires this scheduler to be the current scheduler")
        if self._running:
            raise RuntimeError("Scheduler already running")

    def stop(self) -> None:
        self._stopping = True
        self._break_wait_local()

    def call_soon(self, callback: Callable[..., object], *args: object) -> TimerHandle:
        return self.call_at(self.time(), callback, *args)

    def call_soon_threadsafe(self, callback: Callable[..., object], *args: object) -> None:
        with self._threadsafe_lock:
            self._threadsafe_callbacks.append((callback, args))
        self._break_wait_threadsafe()

    def call_later(self, delay: float, callback: Callable[..., object], *args: object) -> TimerHandle:
        if delay < 0:
            delay = 0
        return self.call_at(self.time() + delay, callback, *args)

    def call_at(self, when: float, callback: Callable[..., object], *args: object) -> TimerHandle:
        handle = TimerHandle(when, callback, args)
        self._enqueue_timer(when, handle)
        return handle

    def _enqueue_timer(self, when: float, handle: TimerHandle) -> None:
        heapq.heappush(self._timers, (when, next(self._timer_sequence), handle))
        self._break_wait_local()

    def _drain_threadsafe_callbacks(self) -> None:
        while True:
            with self._threadsafe_lock:
                if not self._threadsafe_callbacks:
                    return
                callback, args = self._threadsafe_callbacks.popleft()
            callback(*args)

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

    def spawn(self, func: Callable[..., T], *args, **kwargs) -> TealetTask:
        def task_main(current: tealet.tealet, _arg: object):
            return func(*args, **kwargs)

        t = TealetTask(self)
        t.prepare(task_main)
        self._make_runnable(t)
        return t

    def _schedule(self, enqueue=None) -> None:
        self._run_ready_timers()
        if enqueue is not None:
            enqueue()
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
        loop = self._loop
        if loop is None:
            raise RuntimeError("wait_async requires scheduler.arun() with an active asyncio loop")

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
            done_evt.wait()
        finally:
            if state["active"]:
                state["active"] = False
                self._pending_async_waits.discard(current)
            fut.remove_done_callback(_resume_waiter)

        return fut.result()

    def _make_runnable(self, t: tealet.tealet) -> None:
        if t in self._task_set:
            return
        try:
            t.link = self
        except AttributeError:
            pass
        if isinstance(t, TealetTask):
            t._scheduler = self
        self._tasks.append(t)
        self._task_set.add(t)
        self._break_wait_local()

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
        if isinstance(target, TealetTask):
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

    def _break_wait_threadsafe(self) -> None:
        self._idle_waiter.break_threadsafe()

    def _break_wait_local(self) -> None:
        self._idle_waiter.break_local()

    def _break_wait(self) -> None:
        self._break_wait_local()

    def _wait_thread(self) -> None:
        self._thread_idle_waiter.wait_for(self._time_to_next_timer())

    async def _wait_async(self) -> None:
        await self._async_idle_waiter.wait_for(self._time_to_next_timer())

    def run(self) -> None:
        """Run scheduler synchronously until no runnable tasks or timers remain.
        This method is intended for single threaded context with no
        asyncio loop interaction.

        This sync runner only considers local runnable state (`_tasks`) and
        scheduled timer callbacks (`_timers`). Tealets blocked in
        `wait_async()` are not progressed here; use `arun()` for that mode.
        """
        self._verify_current_scheduler()
        previous_idle_waiter = self._idle_waiter
        self._idle_waiter = self._thread_idle_waiter
        self._running = True
        try:
            while self._tasks or self._timers:
                self._run_ready_timers()
                if self._tasks:
                    self._pump()
                if self._tasks or self._timers:
                    self._wait_thread()
        finally:
            self._running = False
            self._idle_waiter = previous_idle_waiter

    def run_forever(self) -> None:
        self._verify_current_scheduler()
        previous_idle_waiter = self._idle_waiter
        self._idle_waiter = self._thread_idle_waiter
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
            self._idle_waiter = previous_idle_waiter

    def run_until_complete(
        self,
        future: Future[T] | Callable[..., T],
        *args: object,
        **kwargs: object,
    ) -> T:
        self._verify_current_scheduler()
        if isinstance(future, Future):
            target: Future[T] = future
            if isinstance(target, TealetTask) and target.get_scheduler() is not self:
                raise RuntimeError("Future is bound to a different scheduler")
        elif callable(future):
            target = self.spawn(future, *args, **kwargs)
        else:
            raise TypeError("future must be a Future or callable")

        previous_idle_waiter = self._idle_waiter
        self._idle_waiter = self._thread_idle_waiter
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
            self._idle_waiter = previous_idle_waiter

        if not target.done():
            raise RuntimeError("Scheduler stopped before Future completed.")
        return target.result()

    async def arun(self) -> None:
        self._verify_current_scheduler()
        previous_idle_waiter = self._idle_waiter
        self._idle_waiter = self._async_idle_waiter
        self._loop = asyncio.get_running_loop()
        self._running = True
        try:
            while self._tasks or self._timers or self._pending_async_waits:
                if self._tasks or self._timers:
                    self._pump()
                await self._wait_async()
        finally:
            self._running = False
            self._loop = None
            self._idle_waiter = previous_idle_waiter

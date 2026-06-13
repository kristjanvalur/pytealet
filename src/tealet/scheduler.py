from __future__ import annotations

import asyncio
import heapq
import inspect
import itertools
import threading
import time
from collections import deque
from typing import Callable, Generic, TypeVar

import tealet

T = TypeVar("T")


# a thread local scheduler
_scheduler = threading.local()


def scheduler() -> SimpleScheduler:
    if not hasattr(_scheduler, "instance"):
        _scheduler.instance = SimpleScheduler()
    return _scheduler.instance


class ScheduledTealet(tealet.tealet):
    """Tealet wrapper that tracks scheduler/event placement."""

    def __init__(self):
        super().__init__()
        self.link = None
        self._future: Future[object] | None = None

    def is_waiting(self):
        return isinstance(self.link, Event)

    def is_runnable(self):
        return isinstance(self.link, SimpleScheduler) and scheduler()._is_runnable(self)

    def is_blocked(self):
        return scheduler()._is_blocked(self)

    def is_running(self):
        return tealet.current() is self

    def _unlink(self):
        if self.link is not None:
            self.link._unlink(self)
        scheduler()._unlink_pending_async_wait(self)

    def run(self):
        scheduler()._target_run(self)

    def throw(self, exc: BaseException):
        scheduler()._target_throw(self, exc)

    def _throw_from_scheduler(self, exc: BaseException):
        super().throw(exc)

    def resolve_target(self, result, exc, exc_target):
        clear = False
        if self._future is not None and not self._future.done():
            if exc is None:
                self._future.set_result(result)
            elif not self._future.cancelled():
                self._future.set_exception(exc)
                clear = True

        # Scheduler-owned tasks always route via scheduler target selection,
        # even if task startup immediately raises before user code returns.
        return scheduler()._find_target(task_exit=True), None, clear


class Event:
    """Minimal event primitive for scheduler-driven wait/wake."""

    def __init__(self) -> None:
        self._waiters: list[tealet.tealet] = []
        self._async_waiters: list[asyncio.Future[bool]] = []
        self._is_set = False

    def _link(self, t: tealet.tealet) -> None:
        assert t.link is None
        assert t not in self._waiters
        try:
            t.link = self
        except AttributeError:
            pass  # main tealet may not have a ``link`` attribute
        self._waiters.append(t)

    def _unlink(self, t: tealet.tealet) -> None:
        try:
            self._waiters.remove(t)
            try:
                t.link = None
            except AttributeError:
                pass  # main tealet may not have a ``link`` attribute
        except ValueError:
            pass

    def wait(self) -> bool:
        if self._is_set:
            return True

        current = tealet.current()
        try:
            scheduler()._schedule(lambda: self._link(current))
        except BaseException:
            self._unlink(current)
            raise

        return True

    async def async_wait(self) -> bool:
        if self._is_set:
            return True

        waiter: asyncio.Future[bool] = asyncio.get_running_loop().create_future()
        self._async_waiters.append(waiter)
        try:
            return await waiter
        finally:
            try:
                self._async_waiters.remove(waiter)
            except ValueError:
                pass

    def set(self) -> None:
        self._is_set = True
        for waiter in self._waiters:
            scheduler()._make_runnable(waiter)
        self._waiters.clear()
        for waiter in self._async_waiters:
            if not waiter.done():
                # support cross thread wakeup
                waiter.get_loop().call_soon_threadsafe(waiter.set_result, True)
        self._async_waiters.clear()

    def clear(self) -> None:
        self._is_set = False


class Lock:
    """A tealet-compatible mutual exclusion lock."""

    def __init__(self) -> None:
        self._locked = False
        self._waiters: deque[Event] = deque()

    def locked(self) -> bool:
        return self._locked

    def sync_acquire(self) -> bool:
        if not self._locked:
            self._locked = True
            return True

        waiter = Event()
        self._waiters.append(waiter)
        try:
            waiter.wait()
        except BaseException:
            try:
                self._waiters.remove(waiter)
            except ValueError:
                pass
            raise

        self._locked = True
        return True

    async def acquire(self) -> bool:
        if not self._locked:
            self._locked = True
            return True

        waiter = Event()
        self._waiters.append(waiter)
        try:
            await waiter.async_wait()
        except BaseException:
            try:
                self._waiters.remove(waiter)
            except ValueError:
                pass
            raise

        self._locked = True
        return True

    def release(self) -> None:
        if not self._locked:
            raise RuntimeError("Lock is not acquired")

        self._locked = False
        while self._waiters:
            waiter = self._waiters.popleft()
            waiter.set()
            break

    def __enter__(self) -> "Lock":
        self.sync_acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.release()

    async def __aenter__(self) -> "Lock":
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.release()


class Condition:
    """A tealet-compatible condition variable."""

    def __init__(self, lock: Lock | None = None) -> None:
        self._lock = lock if lock is not None else Lock()
        self._waiters: deque[Event] = deque()

    def locked(self) -> bool:
        return self._lock.locked()

    def sync_acquire(self) -> bool:
        return self._lock.sync_acquire()

    async def acquire(self) -> bool:
        return await self._lock.acquire()

    def release(self) -> None:
        self._lock.release()

    def sync_wait(self) -> bool:
        if not self.locked():
            raise RuntimeError("cannot wait on un-acquired lock")

        waiter = Event()
        self._waiters.append(waiter)
        self._lock.release()
        try:
            waiter.wait()
            return True
        finally:
            try:
                self._waiters.remove(waiter)
            except ValueError:
                pass
            self._lock.sync_acquire()

    async def wait(self) -> bool:
        if not self.locked():
            raise RuntimeError("cannot wait on un-acquired lock")

        waiter = Event()
        self._waiters.append(waiter)
        self._lock.release()
        try:
            await waiter.async_wait()
            return True
        finally:
            try:
                self._waiters.remove(waiter)
            except ValueError:
                pass
            await self._lock.acquire()

    def sync_wait_for(self, predicate: Callable[[], bool]) -> bool:
        result = predicate()
        while not result:
            self.sync_wait()
            result = predicate()
        return result

    async def wait_for(self, predicate: Callable[[], bool]) -> bool:
        result = predicate()
        while not result:
            await self.wait()
            result = predicate()
        return result

    def notify(self, n: int = 1) -> None:
        if not self.locked():
            raise RuntimeError("cannot notify on un-acquired lock")
        if n <= 0:
            return

        while self._waiters and n > 0:
            waiter = self._waiters.popleft()
            waiter.set()
            n -= 1

    def notify_all(self) -> None:
        self.notify(len(self._waiters))

    def __enter__(self) -> "Condition":
        self.sync_acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.release()

    async def __aenter__(self) -> "Condition":
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.release()


class Barrier:
    """A tealet-compatible barrier with sync and async waits."""

    def __init__(self, parties: int) -> None:
        if parties <= 0:
            raise ValueError("parties must be > 0")
        self.parties = parties
        self._count = 0
        self._generation = 0
        self._waiters: deque[tuple[Event, int, int]] = deque()

    @property
    def n_waiting(self) -> int:
        return self._count

    def _arrive(self) -> tuple[int, Event | None, int]:
        generation = self._generation
        index = self.parties - self._count - 1
        self._count += 1

        if self._count == self.parties:
            self._count = 0
            self._generation += 1
            while self._waiters:
                waiter, _, _ = self._waiters.popleft()
                waiter.set()
            return 0, None, generation

        waiter = Event()
        self._waiters.append((waiter, index, generation))
        return index, waiter, generation

    def _cancel_waiter(self, waiter: Event, index: int, generation: int) -> None:
        try:
            self._waiters.remove((waiter, index, generation))
            if generation == self._generation and self._count > 0:
                self._count -= 1
        except ValueError:
            pass

    def sync_wait(self) -> int:
        index, waiter, generation = self._arrive()
        if waiter is None:
            return index

        try:
            waiter.wait()
            return index
        except BaseException:
            self._cancel_waiter(waiter, index, generation)
            raise

    async def wait(self) -> int:
        index, waiter, generation = self._arrive()
        if waiter is None:
            return index

        try:
            await waiter.async_wait()
            return index
        except BaseException:
            self._cancel_waiter(waiter, index, generation)
            raise


class Channel:
    """A tealet-style rendezvous channel with Stackless-like balance/preference."""

    def __init__(self, preference: int = -1) -> None:
        self._waiters: deque[tealet.tealet] = deque()
        self._balance = 0
        self._packets: dict[tealet.tealet, tuple[bool, object]] = {}
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

    def _link_sender(self, t: tealet.tealet, packet: tuple[bool, object]) -> None:
        self._packets[t] = packet
        self._waiters.append(t)
        self._balance += 1
        try:
            t.link = self
        except AttributeError:
            pass

    def _link_receiver(self, t: tealet.tealet) -> None:
        self._waiters.append(t)
        self._balance -= 1
        try:
            t.link = self
        except AttributeError:
            pass

    def _unlink(self, t: tealet.tealet) -> None:
        removed = False
        self._packets.pop(t, None)
        try:
            self._waiters.remove(t)
            removed = True
        except ValueError:
            pass
        if removed:
            if self._balance > 0:
                self._balance -= 1
            elif self._balance < 0:
                self._balance += 1
        self._clear_link(t)

    def _send_packet(self, packet: tuple[bool, object]) -> None:
        if self._balance < 0:
            receiver = self._waiters.popleft()
            self._balance += 1
            self._clear_link(receiver)
            self._packets[receiver] = packet
            if self._preference < 0:
                receiver.run()
            else:
                scheduler()._make_runnable(receiver)
            return

        current = tealet.current()
        try:
            scheduler()._schedule(lambda: self._link_sender(current, packet))
        except BaseException:
            self._unlink(current)
            raise

    def send(self, value: object) -> None:
        self._send_packet((False, value))

    def send_exception(self, klass: type[BaseException] | BaseException, *args: object) -> None:
        if isinstance(klass, BaseException):
            if args:
                raise TypeError("args must be empty when sending an exception instance")
            exc = klass
        else:
            if not isinstance(klass, type) or not issubclass(klass, BaseException):
                raise TypeError("klass must be a BaseException instance or subclass")
            exc = klass(*args)
        self._send_packet((True, exc))

    def receive(self) -> object:
        if self._balance > 0:
            sender = self._waiters.popleft()
            self._balance -= 1
            packet = self._packets.pop(sender)
            self._clear_link(sender)
            if self._preference > 0:
                sender.run()
            else:
                scheduler()._make_runnable(sender)
            return self._deliver(packet)

        current = tealet.current()
        try:
            scheduler()._schedule(lambda: self._link_receiver(current))
        except BaseException:
            self._unlink(current)
            raise

        return self._deliver(self._packets.pop(current))


class Semaphore:
    """A tealet-compatible counting semaphore."""

    def __init__(self, value: int = 1) -> None:
        if value < 0:
            raise ValueError("Semaphore initial value must be >= 0")
        self._value = value
        self._waiters: deque[Event] = deque()

    def locked(self) -> bool:
        return self._value == 0

    def sync_acquire(self) -> bool:
        if self._value > 0:
            self._value -= 1
            return True

        waiter = Event()
        self._waiters.append(waiter)
        try:
            waiter.wait()
        except BaseException:
            try:
                self._waiters.remove(waiter)
            except ValueError:
                pass
            raise

        self._value -= 1
        return True

    async def acquire(self) -> bool:
        if self._value > 0:
            self._value -= 1
            return True

        waiter = Event()
        self._waiters.append(waiter)
        try:
            await waiter.async_wait()
        except BaseException:
            try:
                self._waiters.remove(waiter)
            except ValueError:
                pass
            raise

        self._value -= 1
        return True

    def release(self) -> None:
        self._value += 1
        while self._waiters:
            waiter = self._waiters.popleft()
            waiter.set()
            break

    def __enter__(self) -> "Semaphore":
        self.sync_acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.release()

    async def __aenter__(self) -> "Semaphore":
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.release()


class BoundedSemaphore(Semaphore):
    """A semaphore that cannot be released above its initial value."""

    def __init__(self, value: int = 1) -> None:
        super().__init__(value)
        self._bound_value = value

    def release(self) -> None:
        if self._value >= self._bound_value:
            raise ValueError("BoundedSemaphore released too many times")
        super().release()


class Queue(Generic[T]):
    """A tealet-compatible FIFO queue modeled after asyncio.Queue."""

    def __init__(self, maxsize: int = 0) -> None:
        if maxsize < 0:
            raise ValueError("maxsize must be >= 0")
        self.maxsize = maxsize
        self._getters: deque[Event] = deque()
        self._putters: deque[Event] = deque()
        self._unfinished_tasks = 0
        self._finished = Event()
        self._finished.set()
        self._init(maxsize)

    def _init(self, maxsize: int) -> None:
        self._queue: deque[T] = deque()

    def _put(self, item: T) -> None:
        self._queue.append(item)

    def _get(self) -> T:
        return self._queue.popleft()

    def qsize(self) -> int:
        return len(self._queue)

    def empty(self) -> bool:
        return self.qsize() == 0

    def full(self) -> bool:
        return self.maxsize > 0 and self.qsize() >= self.maxsize

    def _wakeup_next(self, waiters: deque[Event]) -> None:
        while waiters:
            waiters.popleft().set()
            return

    def put_nowait(self, item: T) -> None:
        if self.full():
            raise QueueFull
        self._put(item)
        self._unfinished_tasks += 1
        self._finished.clear()
        self._wakeup_next(self._getters)

    def sync_put(self, item: T) -> None:
        while self.full():
            waiter = Event()
            self._putters.append(waiter)
            try:
                waiter.wait()
            finally:
                try:
                    self._putters.remove(waiter)
                except ValueError:
                    pass
        self.put_nowait(item)

    async def put(self, item: T) -> None:
        while self.full():
            waiter = Event()
            self._putters.append(waiter)
            try:
                await waiter.async_wait()
            finally:
                try:
                    self._putters.remove(waiter)
                except ValueError:
                    pass
        self.put_nowait(item)

    def get_nowait(self) -> T:
        if self.empty():
            raise QueueEmpty
        item = self._get()
        self._wakeup_next(self._putters)
        return item

    def sync_get(self) -> T:
        while self.empty():
            waiter = Event()
            self._getters.append(waiter)
            try:
                waiter.wait()
            finally:
                try:
                    self._getters.remove(waiter)
                except ValueError:
                    pass
        return self.get_nowait()

    async def get(self) -> T:
        while self.empty():
            waiter = Event()
            self._getters.append(waiter)
            try:
                await waiter.async_wait()
            finally:
                try:
                    self._getters.remove(waiter)
                except ValueError:
                    pass
        return self.get_nowait()

    def task_done(self) -> None:
        if self._unfinished_tasks <= 0:
            raise ValueError("task_done() called too many times")
        self._unfinished_tasks -= 1
        if self._unfinished_tasks == 0:
            self._finished.set()

    def sync_join(self) -> None:
        while self._unfinished_tasks:
            self._finished.wait()

    async def join(self) -> None:
        while self._unfinished_tasks:
            await self._finished.async_wait()


class PriorityQueue(Queue[T]):
    """A tealet-compatible priority queue."""

    def _init(self, maxsize: int) -> None:
        self._queue: list[T] = []

    def _put(self, item: T) -> None:
        heapq.heappush(self._queue, item)

    def _get(self) -> T:
        return heapq.heappop(self._queue)


class LifoQueue(Queue[T]):
    """A tealet-compatible LIFO queue."""

    def _init(self, maxsize: int) -> None:
        self._queue: list[T] = []

    def _put(self, item: T) -> None:
        self._queue.append(item)

    def _get(self) -> T:
        return self._queue.pop()


class DeadlockError(RuntimeError):
    """Raised when the scheduler has no runnable tasks."""


InvalidStateError = asyncio.InvalidStateError
QueueEmpty = asyncio.QueueEmpty
QueueFull = asyncio.QueueFull


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
        return True

    def set_result(self, value: T) -> None:
        if self._done:
            raise InvalidStateError("Future already done")
        self._result = value
        self._done = True
        self._event.set()

    def set_exception(self, exc: BaseException) -> None:
        if self._done:
            raise InvalidStateError("Future already done")
        if not isinstance(exc, BaseException):
            raise TypeError("exc must be a BaseException instance")
        self._exception = exc
        self._done = True
        self._event.set()

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


class SimpleScheduler:
    """Very small cooperative scheduler for runnable tealets."""

    def __init__(self) -> None:
        self._tasks: deque[tealet.tealet] = deque()
        self._task_set: set[tealet.tealet] = set()
        self._runner = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._pending_async_waits: set[tealet.tealet] = set()
        self._timers: list[tuple[float, int, TimerHandle]] = []
        self._timer_sequence = itertools.count()
        self._wakeup = threading.Event()
        self._awakeup = asyncio.Event()
        self._n_scheduled = 0
        self._target_count = None

    def time(self) -> float:
        return time.monotonic()

    def call_soon(self, callback: Callable[..., object], *args: object) -> TimerHandle:
        return self.call_at(self.time(), callback, *args)

    def call_later(self, delay: float, callback: Callable[..., object], *args: object) -> TimerHandle:
        if delay < 0:
            delay = 0
        return self.call_at(self.time() + delay, callback, *args)

    def call_at(self, when: float, callback: Callable[..., object], *args: object) -> TimerHandle:
        handle = TimerHandle(when, callback, args)
        heapq.heappush(self._timers, (when, next(self._timer_sequence), handle))
        self._break_wait()
        return handle

    def _run_ready_timers(self) -> None:
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

    def spawn(self, func: Callable[..., T], *args, **kwargs) -> Future[T]:
        future: Future[T] = Future()

        def task_main(current: tealet.tealet, _arg: object):
            return func(*args, **kwargs)

        t = ScheduledTealet()
        t._future = future
        t.prepare(task_main)
        self._make_runnable(t)
        return future

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
        evt = Event()
        with self.call_later(delay, evt.set):
            evt.wait()

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
        if isinstance(target, ScheduledTealet):
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

    def pump(self, n=0) -> None:
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

    def _break_wait(self) -> None:
        self._wakeup.set()
        self._awakeup.set()

    def _wait_thread(self) -> None:
        sleep_for = self._time_to_next_timer()
        if sleep_for is not None:
            self._wakeup.wait(timeout=sleep_for)
            self._wakeup.clear()

    async def _wait_async(self) -> None:
        sleep_for = self._time_to_next_timer()
        if sleep_for is None:
            # No scheduler timer is pending. We may still be alive because one or
            # more tealets are blocked in wait_async() on external asyncio
            # awaitables, so block until an explicit wakeup arrives.
            await self._awakeup.wait()
            self._awakeup.clear()
            return
        try:
            async with asyncio.timeout(sleep_for):
                await self._awakeup.wait()
        except TimeoutError:
            pass
        finally:
            self._awakeup.clear()

    def run(self) -> None:
        while self._tasks or self._timers:
            self.pump()
            self._wait_thread()

    async def arun(self) -> None:
        self._loop = asyncio.get_running_loop()
        try:
            while self._tasks or self._timers or self._pending_async_waits:
                if self._tasks or self._timers:
                    self.pump()
                await self._wait_async()
        finally:
            self._loop = None

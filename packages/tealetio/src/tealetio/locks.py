from __future__ import annotations

import asyncio
import heapq
from collections import deque
from typing import TYPE_CHECKING, Any, Callable, Generic, TypeVar, cast

import tealet

T = TypeVar("T")

__all__ = [
    "Barrier",
    "BoundedSemaphore",
    "Condition",
    "Event",
    "InvalidStateError",
    "LifoQueue",
    "Lock",
    "PriorityLock",
    "PriorityQueue",
    "Queue",
    "QueueEmpty",
    "QueueFull",
    "QueueShutDown",
    "Semaphore",
    "Timeout",
    "TimeoutError",
    "timeout",
    "timeout_at",
]

if TYPE_CHECKING:
    from .scheduler import BaseScheduler, TimerHandle


_get_current_scheduler: Callable[[], BaseScheduler]


class RawTimeoutError(BaseException):
    """Internal timeout sentinel thrown into tealets by scheduler timeouts."""

    pass


TimeoutError = asyncio.TimeoutError
InvalidStateError = asyncio.InvalidStateError
QueueEmpty = asyncio.QueueEmpty
QueueFull = asyncio.QueueFull


class _QueueShutDown(Exception):
    """Raised when put/get is attempted on a shut-down Queue."""

    pass


QueueShutDown: Any = getattr(asyncio, "QueueShutDown", _QueueShutDown)


def set_scheduler_resolver(resolver: Callable[[], BaseScheduler]) -> None:
    """Install the callback used by primitives to find the running scheduler."""

    global _get_current_scheduler
    _get_current_scheduler = resolver


def timeout(delay: float) -> "Timeout":
    """Context manager for timing out a block of code via scheduler timers."""
    sched = _get_current_scheduler()
    when = sched.time() + delay
    return Timeout(when)


def timeout_at(when: float) -> "Timeout":
    """Context manager for timing out a block of code at a specific time via scheduler timers."""
    return Timeout(when)


class Timeout:
    """Timer-backed synchronous timeout context manager."""

    def __init__(self, when: float):
        self._when = when
        self._handle: TimerHandle | None = None
        self._exc = RawTimeoutError()
        self._expired = False

    # -- Public state --------------------------------------------------

    def reschedule(self, when: float):
        """Move this timeout to a new absolute scheduler time."""

        if not self._expired and self._handle is not None:
            self._handle.cancel()
            self._when = when
            self._handle = _get_current_scheduler().call_at(self._when, self._timeout, tealet.current())

    def expired(self) -> bool:
        """Return True if this timeout has fired."""

        return self._expired

    # -- Context manager ----------------------------------------------

    def __enter__(self) -> "Timeout":
        self._handle = _get_current_scheduler().call_at(self._when, self._timeout, tealet.current())
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

    def _query_waiting(self) -> bool:
        return True

    def _query_runnable(self) -> bool:
        return False

    def _unlink(self, t: tealet.tealet) -> None:
        try:
            self._waiters.remove(t)
            try:
                t.link = None
            except AttributeError:
                pass  # main tealet may not have a ``link`` attribute
        except ValueError:
            pass

    def swait(self) -> bool:
        """Block the current tealet until the event is set."""

        if self._is_set:
            return True

        current = tealet.current()
        sched = _get_current_scheduler()
        try:
            sched._schedule(lambda: self._link(current))
        except BaseException as exc:
            self._unlink(current)
            if isinstance(exc, RawTimeoutError) and self._is_set:
                return True
            raise

        return True

    async def wait(self) -> bool:
        """Await until the event is set."""

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
        """Set the event and wake all sync and async waiters."""

        self._is_set = True
        if self._waiters:
            scheduler = _get_current_scheduler()
            for waiter in self._waiters:
                scheduler._make_runnable(waiter)
        self._waiters.clear()
        for waiter in self._async_waiters:
            if not waiter.done():
                waiter.set_result(True)
        self._async_waiters.clear()

    def clear(self) -> None:
        """Reset the event to the unset state."""

        self._is_set = False


class Lock:
    """A tealet-compatible mutual exclusion lock."""

    def __init__(self) -> None:
        self._locked = False
        self._waiters: deque[Event] = deque()

    def locked(self) -> bool:
        """Return True if the lock is currently acquired."""

        return self._locked

    def sacquire(self) -> bool:
        """Acquire the lock from synchronous tealet code."""

        if not self._locked:
            self._locked = True
            return True

        waiter = Event()
        self._waiters.append(waiter)
        try:
            waiter.swait()
        except BaseException:
            try:
                self._waiters.remove(waiter)
            except ValueError:
                pass
            raise

        self._locked = True
        return True

    async def acquire(self) -> bool:
        """Acquire the lock from async code."""

        if not self._locked:
            self._locked = True
            return True

        waiter = Event()
        self._waiters.append(waiter)
        try:
            await waiter.wait()
        except BaseException:
            try:
                self._waiters.remove(waiter)
            except ValueError:
                pass
            raise

        self._locked = True
        return True

    def release(self) -> None:
        """Release the lock and wake one waiter, if any."""

        if not self._locked:
            raise RuntimeError("Lock is not acquired")

        self._locked = False
        while self._waiters:
            waiter = self._waiters.popleft()
            waiter.set()
            break

    def __enter__(self) -> "Lock":
        self.sacquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.release()

    async def __aenter__(self) -> "Lock":
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.release()


class PriorityLock(Lock):
    """A tealet-compatible lock with priority inheritance."""

    def __init__(self) -> None:
        super().__init__()
        self._owner: object | None = None
        self._waiter_tasks: set[object] = set()

    def _current_task(self) -> tealet.tealet:
        return tealet.current()

    def _current_async_owner(self) -> object:
        task = asyncio.current_task()
        if task is None:
            return tealet.current()
        return task

    def _is_current(self, owner: object) -> bool:
        if owner is tealet.current():
            return True
        try:
            task = asyncio.current_task()
        except RuntimeError:
            task = None
        return owner is task

    def _task_effective_priority(self, task: object) -> float:
        try:
            return cast(Any, task).get_effective_priority()
        except AttributeError:
            return 0.0

    def _take_lock(self, task: object) -> None:
        self._locked = True
        self._owner = task
        try:
            cast(Any, task).add_owned_priority_lock(self)
        except AttributeError:
            pass

    def _drop_lock(self) -> None:
        owner = self._owner
        if not self._is_current(owner):
            raise RuntimeError("PriorityLock can only be released by its owner")
        self._owner = None
        try:
            cast(Any, owner).remove_owned_priority_lock(self)
            cast(Any, owner).modified()
        except AttributeError:
            pass

    def get_effective_priority(self) -> float | None:
        """Return the best inherited priority among waiting tasks."""

        if not self._waiter_tasks:
            return None
        return min(self._task_effective_priority(task) for task in self._waiter_tasks)

    def _propagate_priority(self, source: object) -> None:
        del source
        if self._owner is not None:
            try:
                cast(Any, self._owner)._propagate_priority(self)
            except AttributeError:
                pass

    def sacquire(self) -> bool:
        """Acquire the lock from synchronous tealet code with priority inheritance."""

        task = self._current_task()
        if not self._locked:
            super().sacquire()
            self._take_lock(task)
            return True

        self._waiter_tasks.add(task)
        try:
            cast(Any, task).set_waiting_on_priority(self)
        except AttributeError:
            pass
        self._propagate_priority(task)
        try:
            super().sacquire()
        finally:
            self._waiter_tasks.discard(task)
            try:
                cast(Any, task).set_waiting_on_priority(None)
            except AttributeError:
                pass
            if self._locked and self._owner is not task:
                self._propagate_priority(task)

        self._take_lock(task)
        return True

    async def acquire(self) -> bool:
        """Acquire the lock from async code with priority inheritance."""

        task = self._current_async_owner()
        if not self._locked:
            await super().acquire()
            self._take_lock(task)
            return True

        self._waiter_tasks.add(task)
        self._propagate_priority(task)
        try:
            await super().acquire()
        finally:
            self._waiter_tasks.discard(task)
            if self._locked and self._owner is not task:
                self._propagate_priority(task)

        self._take_lock(task)
        return True

    def release(self) -> None:
        """Release the priority lock from its owning task."""

        if not self._locked:
            raise RuntimeError("Lock is not acquired")

        self._drop_lock()
        super().release()


class Condition:
    """A tealet-compatible condition variable."""

    def __init__(self, lock: Lock | None = None) -> None:
        self._lock = lock if lock is not None else Lock()
        self._waiters: deque[Event] = deque()

    def locked(self) -> bool:
        """Return True if the underlying lock is currently acquired."""

        return self._lock.locked()

    def sacquire(self) -> bool:
        """Acquire the underlying lock from synchronous tealet code."""

        return self._lock.sacquire()

    async def acquire(self) -> bool:
        """Acquire the underlying lock from async code."""

        return await self._lock.acquire()

    def release(self) -> None:
        """Release the underlying lock."""

        self._lock.release()

    def swait(self) -> bool:
        """Release the lock and block until notified from sync tealet code."""

        if not self.locked():
            raise RuntimeError("cannot wait on un-acquired lock")

        waiter = Event()
        self._waiters.append(waiter)
        self._lock.release()
        try:
            waiter.swait()
            return True
        finally:
            try:
                self._waiters.remove(waiter)
            except ValueError:
                pass
            self._lock.sacquire()

    async def wait(self) -> bool:
        """Release the lock and await until notified from async code."""

        if not self.locked():
            raise RuntimeError("cannot wait on un-acquired lock")

        waiter = Event()
        self._waiters.append(waiter)
        self._lock.release()
        try:
            await waiter.wait()
            return True
        finally:
            try:
                self._waiters.remove(waiter)
            except ValueError:
                pass
            await self._lock.acquire()

    def swait_for(self, predicate: Callable[[], bool]) -> bool:
        """Synchronously wait until `predicate` returns a truthy value."""

        result = predicate()
        while not result:
            self.swait()
            result = predicate()
        return result

    async def wait_for(self, predicate: Callable[[], bool]) -> bool:
        """Asynchronously wait until `predicate` returns a truthy value."""

        result = predicate()
        while not result:
            await self.wait()
            result = predicate()
        return result

    def notify(self, n: int = 1) -> None:
        """Wake up to `n` tasks waiting on this condition."""

        if not self.locked():
            raise RuntimeError("cannot notify on un-acquired lock")
        if n <= 0:
            return

        while self._waiters and n > 0:
            waiter = self._waiters.popleft()
            waiter.set()
            n -= 1

    def notify_all(self) -> None:
        """Wake all tasks waiting on this condition."""

        self.notify(len(self._waiters))

    def __enter__(self) -> "Condition":
        self.sacquire()
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
        """Return the number of parties currently waiting at the barrier."""

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

    def swait(self) -> int:
        """Synchronously wait for all barrier parties and return this party index."""

        index, waiter, generation = self._arrive()
        if waiter is None:
            return index

        try:
            waiter.swait()
            return index
        except BaseException:
            self._cancel_waiter(waiter, index, generation)
            raise

    async def wait(self) -> int:
        """Asynchronously wait for all barrier parties and return this party index."""

        index, waiter, generation = self._arrive()
        if waiter is None:
            return index

        try:
            await waiter.wait()
            return index
        except BaseException:
            self._cancel_waiter(waiter, index, generation)
            raise


class Semaphore:
    """A tealet-compatible counting semaphore."""

    def __init__(self, value: int = 1) -> None:
        if value < 0:
            raise ValueError("Semaphore initial value must be >= 0")
        self._value = value
        self._waiters: deque[Event] = deque()

    def locked(self) -> bool:
        """Return True if the semaphore cannot be acquired immediately."""

        return self._value == 0

    def sacquire(self) -> bool:
        """Acquire one semaphore slot from synchronous tealet code."""

        if self._value > 0:
            self._value -= 1
            return True

        waiter = Event()
        self._waiters.append(waiter)
        try:
            waiter.swait()
        except BaseException:
            try:
                self._waiters.remove(waiter)
            except ValueError:
                pass
            raise

        self._value -= 1
        return True

    async def acquire(self) -> bool:
        """Acquire one semaphore slot from async code."""

        if self._value > 0:
            self._value -= 1
            return True

        waiter = Event()
        self._waiters.append(waiter)
        try:
            await waiter.wait()
        except BaseException:
            try:
                self._waiters.remove(waiter)
            except ValueError:
                pass
            raise

        self._value -= 1
        return True

    def release(self) -> None:
        """Release one semaphore slot and wake one waiter, if any."""

        self._value += 1
        while self._waiters:
            waiter = self._waiters.popleft()
            waiter.set()
            break

    def __enter__(self) -> "Semaphore":
        self.sacquire()
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
        """Release one slot, failing if it would exceed the initial value."""

        if self._value >= self._bound_value:
            raise ValueError("BoundedSemaphore released too many times")
        super().release()


class Queue(Generic[T]):
    """A tealet-compatible FIFO queue modeled after asyncio.Queue."""

    _queue: Any

    def __init__(self, maxsize: int = 0) -> None:
        if maxsize < 0:
            raise ValueError("maxsize must be >= 0")
        self.maxsize = maxsize
        self._getters: deque[Event] = deque()
        self._putters: deque[Event] = deque()
        self._unfinished_tasks = 0
        self._is_shutdown = False
        self._finished = Event()
        self._finished.set()
        self._init(maxsize)

    def _init(self, maxsize: int) -> None:
        self._queue = deque()

    def _put(self, item: T) -> None:
        self._queue.append(item)

    def _get(self) -> T:
        return self._queue.popleft()

    def qsize(self) -> int:
        """Return the current number of items in the queue."""

        return len(self._queue)

    def empty(self) -> bool:
        """Return True if the queue currently has no items."""

        return self.qsize() == 0

    def full(self) -> bool:
        """Return True if the queue has reached `maxsize`."""

        return self.maxsize > 0 and self.qsize() >= self.maxsize

    def _wakeup_next(self, waiters: deque[Event]) -> None:
        while waiters:
            waiters.popleft().set()
            return

    def _wakeup_all(self, waiters: deque[Event]) -> None:
        while waiters:
            waiters.popleft().set()

    def put_nowait(self, item: T) -> None:
        """Put `item` without blocking, or raise QueueFull/QueueShutDown."""

        if self._is_shutdown:
            raise QueueShutDown
        if self.full():
            raise QueueFull
        self._put(item)
        self._unfinished_tasks += 1
        self._finished.clear()
        self._wakeup_next(self._getters)

    def sput(self, item: T) -> None:
        """Put `item`, blocking the current tealet while the queue is full."""

        while self.full():
            if self._is_shutdown:
                raise QueueShutDown
            waiter = Event()
            self._putters.append(waiter)
            try:
                waiter.swait()
            finally:
                try:
                    self._putters.remove(waiter)
                except ValueError:
                    pass
        self.put_nowait(item)

    async def put(self, item: T) -> None:
        """Put `item`, awaiting while the queue is full."""

        while self.full():
            if self._is_shutdown:
                raise QueueShutDown
            waiter = Event()
            self._putters.append(waiter)
            try:
                await waiter.wait()
            finally:
                try:
                    self._putters.remove(waiter)
                except ValueError:
                    pass
        self.put_nowait(item)

    def get_nowait(self) -> T:
        """Remove and return an item without blocking."""

        if self.empty():
            if self._is_shutdown:
                raise QueueShutDown
            raise QueueEmpty
        item = self._get()
        self._wakeup_next(self._putters)
        if self.empty() and self._is_shutdown:
            self._wakeup_all(self._getters)
        return item

    def sget(self) -> T:
        """Remove and return an item, blocking the current tealet if needed."""

        while self.empty():
            if self._is_shutdown:
                raise QueueShutDown
            waiter = Event()
            self._getters.append(waiter)
            try:
                waiter.swait()
            finally:
                try:
                    self._getters.remove(waiter)
                except ValueError:
                    pass
        return self.get_nowait()

    async def get(self) -> T:
        """Remove and return an item, awaiting if needed."""

        while self.empty():
            if self._is_shutdown:
                raise QueueShutDown
            waiter = Event()
            self._getters.append(waiter)
            try:
                await waiter.wait()
            finally:
                try:
                    self._getters.remove(waiter)
                except ValueError:
                    pass
        return self.get_nowait()

    def task_done(self) -> None:
        """Mark one formerly queued item as fully processed."""

        if self._unfinished_tasks <= 0:
            raise ValueError("task_done() called too many times")
        self._unfinished_tasks -= 1
        if self._unfinished_tasks == 0:
            self._finished.set()

    def sjoin(self) -> None:
        """Block the current tealet until all queued work is marked done."""

        while self._unfinished_tasks:
            self._finished.swait()

    async def join(self) -> None:
        """Await until all queued work is marked done."""

        while self._unfinished_tasks:
            await self._finished.wait()

    def shutdown(self, immediate: bool = False) -> None:
        """Shut down the queue and wake blocked putters/getters."""

        self._is_shutdown = True
        self._wakeup_all(self._putters)
        if immediate:
            while self._queue:
                self._get()
                if self._unfinished_tasks > 0:
                    self._unfinished_tasks -= 1
            if self._unfinished_tasks == 0:
                self._finished.set()
        self._wakeup_all(self._getters)


class PriorityQueue(Queue[T]):
    """A tealet-compatible priority queue."""

    def _init(self, maxsize: int) -> None:
        self._queue = []

    def _put(self, item: T) -> None:
        heapq.heappush(cast(list[Any], self._queue), cast(Any, item))

    def _get(self) -> T:
        return cast(T, heapq.heappop(cast(list[Any], self._queue)))


class LifoQueue(Queue[T]):
    """A tealet-compatible LIFO queue."""

    def _init(self, maxsize: int) -> None:
        self._queue = []

    def _put(self, item: T) -> None:
        self._queue.append(item)

    def _get(self) -> T:
        return self._queue.pop()

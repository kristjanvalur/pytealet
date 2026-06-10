"""Examples for tealet usage.

This module is intentionally outside the installed ``tealet`` package to keep
example code separate from runtime APIs.
"""

from __future__ import annotations

import heapq
import itertools
import time
from collections.abc import Iterable, Iterator
from typing import Callable, Generic, TypeVar

import tealet
import threading
import asyncio

T = TypeVar("T")


def raw_simple_generator(current: tealet.tealet, source: Iterable[T]) -> tealet.tealet:
    """Yield values from ``source`` by switching to the previous tealet.

    This is the raw tealet protocol:
    1. Resolve the previous tealet once.
    2. For each produced item, ``previous.switch(value)`` and keep the returned
       tealet for the next transfer.
    3. Signal completion with ``StopIteration`` and return to ``previous``.
    """

    previous = current.previous()
    for value in source:
        previous.switch(value)

    previous.set_exception(StopIteration())
    return previous


class GeneratorTealet(tealet.tealet, Iterator[T], Generic[T]):
    """Simple iterable wrapper implemented directly as a tealet subclass."""

    def __new__(cls, source: Iterable[T]):
        # Base ``tealet`` constructor takes no args; consume subclass args here.
        return super().__new__(cls)

    def __init__(self, source: Iterable[T]):
        super().__init__()
        self._source = source
        self._closed = False
        self.prepare(self._main)

    def __iter__(self) -> "GeneratorTealet[T]":
        return self

    def __next__(self) -> T:
        if self._closed:
            raise StopIteration
        try:
            return self.switch(tealet.current())
        except StopIteration:
            self._closed = True
            raise

    def _main(self, current: tealet.tealet, previous: tealet.tealet) -> tealet.tealet:
        for value in self._source:
            previous = previous.switch(value)
        previous.set_exception(StopIteration())
        return previous


def simple_generator(source: Iterable[T]) -> GeneratorTealet[T]:
    """Return a generator-style iterator backed by a tealet subclass."""

    return GeneratorTealet(source)


# a simple scheduler and event object.

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
        self.where = None

    def is_waiting(self):
        return isinstance(self.where, Event)

    def is_runnable(self):
        return isinstance(self.where, SimpleScheduler) and scheduler().is_runnable(self)

    def is_running(self):
        return tealet.current() is self


class Event:
    """Minimal event primitive for scheduler-driven wait/wake."""

    def __init__(self) -> None:
        self._waiters: list[tealet.tealet] = []
        self._is_set = False

    def _remove_waiter(self, waiter: tealet.tealet) -> None:
        try:
            self._waiters.remove(waiter)
        except ValueError:
            pass

    def wait(self, timeout: float | None = None) -> bool:
        if self._is_set:
            return True

        if timeout is not None and timeout < 0:
            timeout = 0.0

        current = tealet.current()
        timed_out = False

        timeout_handle: TimerHandle | None = None

        if timeout is not None:

            def _wake_timeout() -> None:
                nonlocal timed_out
                timed_out = True
                self._remove_waiter(current)
                scheduler().make_runnable(current)

            timeout_handle = scheduler().call_later(timeout, _wake_timeout)

        current.where = self
        try:
            self._waiters.append(current)
            scheduler().schedule()
        finally:
            if timeout_handle is not None:
                timeout_handle.cancel()
            self._remove_waiter(current)
            current.where = None

        return not timed_out

    def set(self) -> None:
        self._is_set = True
        for waiter in self._waiters:
            scheduler().make_runnable(waiter)
        self._waiters.clear()

    def clear(self) -> None:
        self._is_set = False


class DeadlockError(RuntimeError):
    """Raised when the scheduler has no runnable tasks."""

    pass


class InvalidStateError(RuntimeError):
    """Raised when attempting to complete a Future more than once."""

    pass


class TimerHandle:
    """Cancellable callback scheduled to run in the future."""

    def __init__(
        self,
        when: float,
        callback: Callable[..., object],
        args: tuple[object, ...]=(),
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
        self._result: T | None = None
        self._exception: BaseException | None = None
        self._event = Event()

    def done(self) -> bool:
        return self._done

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

    def _wait(self, timeout: float | None = None) -> bool:
        if self._done:
            return True

        return self._event.wait(timeout=timeout)

    def result(self, timeout: float | None = None) -> T:
        if not self._wait(timeout=timeout):
            raise TimeoutError("Future timed out")
        if self._exception is not None:
            raise self._exception
        return self._result

    def exception(self, timeout: float | None = None) -> BaseException | None:
        if not self._wait(timeout=timeout):
            raise TimeoutError("Future timed out")
        return self._exception


class SimpleScheduler:
    """Very small cooperative scheduler for runnable tealets."""

    def __init__(self) -> None:
        self._tasks: list[tealet.tealet] = []
        self._runner = None
        self._timers: list[tuple[float, int, TimerHandle]] = []
        self._timer_sequence = itertools.count()
        self._wakeup = threading.Event()
        self._awakeup = asyncio.Event()

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
        self.break_wait()
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

    def is_runnable(self, t: tealet.tealet) -> bool:
        return t in self._tasks

    def spawn(self, func: Callable[..., T], *args, **kwargs) -> Future[T]:
        future: Future[T] = Future()

        def task_main(current: tealet.tealet, _arg: object) -> tealet.tealet:
            try:
                result = func(*args, **kwargs)
            except BaseException as exc:
                future.set_exception(exc)
            else:
                future.set_result(result)
            return scheduler().find_target(task_exit=True)

        t = ScheduledTealet().prepare(task_main)
        self.make_runnable(t)
        return future

    def schedule(self) -> None:
        self._run_ready_timers()
        self.find_target().switch()

    def yield_(self) -> None:
        self.make_runnable(tealet.current())
        self.schedule()

    def sleep(self, delay: float) -> None:
        evt = Event()
        with self.call_later(delay, evt.set):
            evt.wait()

    def make_runnable(self, t: tealet.tealet) -> None:
        if t in self._tasks:
            return
        t.where = self
        self._tasks.append(t)
        self.break_wait()

    def find_target(self, task_exit=False) -> tealet.tealet:
        if self._tasks:
            result = self._tasks.pop(0)
        elif self._runner is not None:
            result = self._runner
        # fall back to main
        elif not task_exit:
            raise DeadlockError("No tasks to switch to")
        else:
            result = tealet.main()
        try:
            result.where = None
        except AttributeError:
            pass  # main tealet may not have a ``where`` attribute
        return result

    def pump(self, n=0) -> None:
        if self._runner is not None:
            raise RuntimeError("Scheduler already running")
        if n == 0:
            n = len(self._tasks)
        pumped = 0
        self._runner = tealet.current()
        try:
            self._run_ready_timers()                
            while n != 0:
                target = self.find_target()
                if target is None:
                    return pumped
                pumped += 1
                n -= 1
                target.switch()
        finally:
            self._runner = None

    def break_wait(self) -> None:
        self._wakeup.set()
        self._awakeup.set()

    def wait_thead(self) -> None:
        sleep_for = self._time_to_next_timer()
        if sleep_for is not  None:
            self._wakeup.wait(timeout=sleep_for)
            self._wakeup.clear()

    async def wait_async(self) -> None:
        sleep_for = self._time_to_next_timer()
        if sleep_for is not  None:
            try:
                async with asyncio.timeout(sleep_for):
                    await self._awakeup.wait()
            except TimeoutError:
                # Timer expiry is a normal wake path for the scheduler loop.
                pass
            finally:
                self._awakeup.clear()
    

    def run(self) -> None:
        # run untile there are no tasks or timers left. This is a simple example of a scheduler main loop
        while self._tasks or self._timers:
            self.pump()
            self.wait_thead()            

    async def arun(self) -> None:
        # async version of run, for use in async contexts. This is a simple example of how to integrate with an async event loop.
        while self._tasks or self._timers:
            self.pump()
            await self.wait_async()
                

def demo_scheduler_append_with_yield() -> list[str]:
    """Run a few tealets that append while yielding to each other."""

    s = scheduler()
    seen: list[str] = []

    def worker(name: str, count: int) -> None:
        for i in range(count):
            seen.append(f"{name}{i}")
            s.yield_()

    s.spawn(worker, "a", 3)
    s.spawn(worker, "b", 2)
    s.spawn(worker, "c", 1)
    s.run()
    return seen


def demo_wait_for_event_start() -> list[str]:
    """Run one tealet that waits on an event until another starts it."""

    s = scheduler()
    evt = Event()
    seen: list[str] = []

    def waiter() -> None:
        seen.append("waiter:waiting")
        evt.wait()
        seen.append("waiter:started")

    def starter() -> None:
        seen.append("starter:set")
        evt.set()

    s.spawn(waiter)
    s.spawn(starter)
    s.run()
    return seen


def demo_wait_for_event_between_runs() -> list[str]:
    """Run twice with external event wakeup between runs."""

    s = scheduler()
    evt = Event()
    seen: list[str] = []

    def waiter() -> None:
        seen.append("waiter:waiting")
        evt.wait()
        seen.append("waiter:resumed")

    s.spawn(waiter)
    s.run()
    seen.append("after:first-run")

    evt.set()

    s.run()
    seen.append("after:second-run")
    return seen


def demo_future_result() -> list[str]:
    """Run a task via Future and consume it from another tealet."""

    s = scheduler()
    seen: list[str] = []

    def producer() -> int:
        seen.append("producer:start")
        s.yield_()
        seen.append("producer:done")
        return 42

    future = s.spawn(producer)

    def consumer() -> None:
        seen.append(f"consumer:result={future.result()}")

    s.spawn(consumer)
    s.run()
    return seen


def demo_sleep() -> list[str]:
    """Run a tealet that sleeps and resumes via scheduled timer callback."""

    s = scheduler()
    seen: list[str] = []

    def worker() -> None:
        seen.append("before:sleep")
        s.sleep(0.001)
        seen.append("after:sleep")

    s.spawn(worker)
    s.run()
    return seen


def demo_future_timeout_then_success() -> list[str]:
    """Show timeout then successful completion while cancelling timeout wait."""

    s = scheduler()
    evt = Event()
    seen: list[str] = []

    def timeout_waiter() -> None:
        ok = evt.wait(timeout=0.001)
        seen.append(f"timeout_waiter:{ok}")

    def success_waiter() -> None:
        ok = evt.wait(timeout=0.01)
        seen.append(f"success_waiter:{ok}")

    def setter() -> None:
        s.sleep(0.002)
        evt.set()

    s.spawn(timeout_waiter)
    s.spawn(success_waiter)
    s.spawn(setter)
    s.run()
    return seen


def demo() -> None:
    values = list(GeneratorTealet([1, 2, 3]))
    assert values == [1, 2, 3]

    assert demo_scheduler_append_with_yield() == ["a0", "b0", "c0", "a1", "b1", "a2"]
    assert demo_wait_for_event_start() == [
        "waiter:waiting",
        "starter:set",
        "waiter:started",
    ]
    assert demo_wait_for_event_between_runs() == [
        "waiter:waiting",
        "after:first-run",
        "waiter:resumed",
        "after:second-run",
    ]
    assert demo_future_result() == [
        "producer:start",
        "producer:done",
        "consumer:result=42",
    ]
    assert demo_sleep() == ["before:sleep", "after:sleep"]
    assert demo_future_timeout_then_success() == [
        "timeout_waiter:False",
        "success_waiter:True",
    ]


if __name__ == "__main__":
    demo()

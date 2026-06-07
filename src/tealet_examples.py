"""Examples for tealet usage.

This module is intentionally outside the installed ``tealet`` package to keep
example code separate from runtime APIs.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import Callable, Generic, TypeVar

import tealet
import threading

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

# a thread local sheduler
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

    def wait(self) -> None:
        if self._is_set:
            return

        current = tealet.current()
        current.where = self
        try:
            self._waiters.append(current)
            scheduler().schedule()
        finally:
            current.where = None

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

class SimpleScheduler:
    """Very small cooperative scheduler for runnable tealets."""

    def __init__(self) -> None:
        self._tasks: list[tealet.tealet] = []

    def is_runnable(self, t: tealet.tealet) -> bool:
        return t in self._tasks

    def spawn(self, func: Callable[..., object], *args, **kwargs) -> None:
        def task_main(current: tealet.tealet, _arg: object) -> tealet.tealet:
            result = func(*args, **kwargs)
            if isinstance(result, tealet.tealet):
                return result
            return current.main()

        t = ScheduledTealet().prepare(task_main)
        self.make_runnable(t)

    def schedule(self) -> None:
        while self._tasks:
            t = self._tasks.pop(0)
            if t.state == tealet.STATE_EXIT:
                continue
            t.where = None
            t.switch()
            return
        raise DeadlockError("No tasks to switch to")

    def yield_(self) -> None:
        c = tealet.current()
        c.where = self
        try:
            self._tasks.append(tealet.current())
            self.schedule()
        except DeadlockError:
            del self._tasks[-1]  # remove the current task that we just added
            c.where = None
            raise

    def make_runnable(self, t: tealet.tealet) -> None:
        if t.state == tealet.STATE_EXIT:
            return
        if t in self._tasks:
            return
        t.where = self
        self._tasks.append(t)

    def run(self) -> None:
        while self._tasks:
            self.schedule()

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
            

def demo() -> None:
    values = list(GeneratorTealet([1, 2, 3]))
    assert values == [1, 2, 3]

    assert demo_scheduler_append_with_yield() == ["a0", "b0", "c0", "a1", "b1", "a2"]
    assert demo_wait_for_event_start() == ["waiter:waiting", "starter:set", "waiter:started"]


if __name__ == "__main__":
    demo()

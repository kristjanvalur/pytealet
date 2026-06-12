"""Examples for tealet usage.

This module is intentionally outside the installed ``tealet`` package to keep
example code separate from runtime APIs.
"""

from __future__ import annotations

import heapq
import itertools
import time
from collections.abc import Iterable, Iterator
from typing import Generic, TypeVar

import tealet
from tealet.scheduler import (
    BoundedSemaphore,
    CancelledError,
    DeadlockError,
    Event,
    Future,
    InvalidStateError,
    Lock,
    RawTimeoutError,
    ScheduledTealet,
    Semaphore,
    SimpleScheduler,
    Timeout,
    TimeoutError,
    TimerHandle,
    _scheduler,
    scheduler,
    timeout,
    timeout_at,
)

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

    previous.set_pending_exception(StopIteration())
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
        previous.set_pending_exception(StopIteration())
        return previous


def simple_generator(source: Iterable[T]) -> GeneratorTealet[T]:
    """Return a generator-style iterator backed by a tealet subclass."""

    return GeneratorTealet(source)
                

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
        future.wait()
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
    """Show timeout then successful completion using timeout contexts."""

    s = scheduler()
    evt = Event()
    seen: list[str] = []

    def timeout_waiter() -> None:
        tm = timeout(0.001)
        try:
            with tm:
                evt.wait()
        except TimeoutError:
            pass
        seen.append(f"timeout_waiter:{not tm.expired()}")

    def success_waiter() -> None:
        tm = timeout(0.01)
        with tm:
            evt.wait()
        seen.append(f"success_waiter:{not tm.expired()}")

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

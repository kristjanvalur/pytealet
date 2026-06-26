"""Examples for core tealet usage.

This module is intentionally outside the installed ``tealet`` package to keep
example code separate from runtime APIs.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import Generic, TypeVar, cast

import tealet
from tealet.simple_scheduler import SimpleScheduler

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
    if previous is None:
        raise RuntimeError("raw generator requires a previous tealet")
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
            return cast(T, self.switch(tealet.current()))
        except StopIteration:
            self._closed = True
            raise

    def _main(self, current: tealet.tealet, previous: tealet.tealet) -> tealet.tealet:
        for value in self._source:
            previous = cast(tealet.tealet, previous.switch(value))
        previous.set_pending_exception(StopIteration())
        return previous


def simple_generator(source: Iterable[T]) -> GeneratorTealet[T]:
    """Return a generator-style iterator backed by a tealet subclass."""

    return GeneratorTealet(source)


def demo_simple_scheduler_append_with_yield() -> list[str]:
    """Run a few tealets with the minimal core scheduler example."""

    scheduler = SimpleScheduler()
    seen: list[str] = []

    def worker(name: str, count: int) -> None:
        for i in range(count):
            seen.append(f"{name}{i}")
            scheduler.yield_()

    scheduler.spawn(worker, "a", 3)
    scheduler.spawn(worker, "b", 2)
    scheduler.spawn(worker, "c", 1)
    scheduler.run()
    return seen


def demo() -> None:
    values = list(GeneratorTealet([1, 2, 3]))
    assert values == [1, 2, 3]
    assert demo_simple_scheduler_append_with_yield() == ["a0", "b0", "c0", "a1", "b1", "a2"]


if __name__ == "__main__":
    demo()

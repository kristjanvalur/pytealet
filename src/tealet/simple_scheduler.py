"""Minimal cooperative scheduler example built on core tealet primitives."""

from __future__ import annotations

from collections import deque
from typing import Any, Callable, Generic, TypeVar, cast

import tealet

T = TypeVar("T")


class DeadlockError(RuntimeError):
    """Raised when the scheduler has no runnable tealet to switch to."""


class SimpleTask(tealet.tealet, Generic[T]):
    """Small task wrapper used by :class:`SimpleScheduler`."""

    def __new__(cls, scheduler: "SimpleScheduler", func: Callable[[], T]):
        return super().__new__(cls)

    def __init__(self, scheduler: "SimpleScheduler", func: Callable[[], T]):
        super().__init__()
        self._scheduler = scheduler
        self._func = func
        self._done = False
        self._result: T | None = None
        self._exception: BaseException | None = None
        self.prepare(self._main)

    def _main(self, current: tealet.tealet, arg: object) -> tealet.tealet:
        try:
            self._result = self._func()
        except BaseException as exc:
            self._exception = exc
        self._done = True
        return self._scheduler._find_target(task_exit=True)

    def done(self) -> bool:
        return self._done

    def result(self) -> T:
        if not self._done:
            raise RuntimeError("task is not done")
        if self._exception is not None:
            raise self._exception
        return cast(T, self._result)

    def exception(self) -> BaseException | None:
        if not self._done:
            raise RuntimeError("task is not done")
        return self._exception


class SimpleScheduler:
    """Very small cooperative scheduler for runnable tealets.

    This is deliberately an example, not a framework. It has no timers, IO
    polling, thread-safe callbacks, futures, cancellation, or asyncio bridge.
    """

    def __init__(self) -> None:
        self._ready: deque[tealet.tealet] = deque()
        self._runner: tealet.tealet | None = None

    def spawn(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> SimpleTask[T]:
        task = SimpleTask(self, lambda: func(*args, **kwargs))
        self.make_runnable(task)
        return task

    def make_runnable(self, task: tealet.tealet) -> None:
        if task not in self._ready:
            self._ready.append(task)

    def yield_(self) -> None:
        current = tealet.current()
        self.make_runnable(current)
        self._find_target(exclude=current).switch()

    def _find_target(self, task_exit: bool = False, exclude: tealet.tealet | None = None) -> tealet.tealet:
        for _ in range(len(self._ready)):
            task = self._ready.popleft()
            if task is exclude:
                self._ready.append(task)
                continue
            return task
        if self._runner is not None and self._runner is not exclude:
            return self._runner
        if task_exit:
            return tealet.main()
        raise DeadlockError("no runnable tealets")

    def run(self) -> None:
        if self._runner is not None:
            raise RuntimeError("scheduler is already running")
        self._runner = tealet.current()
        try:
            while self._ready:
                self._find_target().switch()
        finally:
            self._runner = None

    def run_until_complete(self, func: Callable[..., T] | SimpleTask[T], *args: Any, **kwargs: Any) -> T:
        task = func if isinstance(func, SimpleTask) else self.spawn(func, *args, **kwargs)
        if self._runner is not None:
            raise RuntimeError("scheduler is already running")
        self._runner = tealet.current()
        try:
            while not task.done():
                self._find_target().switch()
        finally:
            self._runner = None
        return task.result()

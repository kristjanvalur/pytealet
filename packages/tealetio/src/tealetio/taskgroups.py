"""Structured concurrency for scheduler tasks.

``TaskGroup`` is a synchronous nursery: children spawned inside ``with`` are
joined before the block exits, and the first real failure cancels the rest.
Cancellation is a one-shot ``Task.throw()``, not a sticky cancel scope.
"""

from __future__ import annotations

import contextvars
import sys
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any, TypeVar

import tealet

from .locks import Event
from .scheduler import get_scheduler
from .tasks import CancelledError, Future, Task, get_current

if TYPE_CHECKING:
    from .scheduler import BaseScheduler

if sys.version_info >= (3, 11):
    from builtins import ExceptionGroup
else:

    class ExceptionGroup(Exception):
        """PEP 654 ``ExceptionGroup`` subset for Python 3.10.

        Supports ``.message`` and ``.exceptions``. ``except*`` requires 3.11+.
        """

        def __init__(self, message: str, exceptions: Sequence[BaseException]) -> None:
            self.message = message
            self.exceptions = tuple(exceptions)
            super().__init__(message)

        def __str__(self) -> str:
            return self.message


T = TypeVar("T")

__all__ = [
    "ExceptionGroup",
    "TaskGroup",
]


class _TaskGroupCancelled(CancelledError):
    """Sentinel thrown into a group's parent and children on abort.

    Same idea as ``RawTimeoutError``: a private flavour of a public exception
    so the group can tell *its* cancel from an outer one. Each throw uses a new
    instance (tracebacks are per-task) tagged with ``task_group``.
    """

    def __init__(self, task_group: TaskGroup) -> None:
        super().__init__()
        self.task_group = task_group


class TaskGroup:
    """Synchronous task group (asyncio ``TaskGroup`` / Trio nursery).

    Spawn children with ``spawn()`` (``create_task()`` is an alias). The
    ``with`` block does not leave until every child has finished. A child
    exception other than cancellation cancels the rest, then raises
    ``ExceptionGroup``. ``cancel()`` cancels remaining children without
    cancelling the parent body — the happy-eyeballs winner path.
    """

    def __init__(self) -> None:
        self._entered = False
        self._exiting = False
        self._aborting = False
        self._parent: Task | None = None
        self._scheduler: BaseScheduler | None = None
        self._tasks: set[Task] = set()
        self._errors: list[Exception] = []
        self._base_error: BaseException | None = None
        self._done = Event()
        self._done.set()

    def __repr__(self) -> str:
        info = [""]
        if self._tasks:
            info.append(f"tasks={len(self._tasks)}")
        if self._errors:
            info.append(f"errors={len(self._errors)}")
        if self._aborting:
            info.append("cancelling")
        elif self._entered:
            info.append("entered")
        return f"<TaskGroup{' '.join(info)}>"

    def __enter__(self) -> TaskGroup:
        if self._entered:
            raise RuntimeError(f"TaskGroup {self!r} has already been entered")
        parent = get_current()
        if parent is None:
            raise RuntimeError(f"TaskGroup {self!r} cannot determine the parent task")
        self._parent = parent
        self._scheduler = parent.get_scheduler()
        self._entered = True
        return self

    def __exit__(self, et: type[BaseException] | None, exc: BaseException | None, tb: object) -> bool:
        del tb
        self._exiting = True
        try:
            return self._exit(et, exc)
        finally:
            self._parent = None
            self._errors = []
            self._base_error = None
            exc = None

    def spawn(
        self,
        func: Callable[[], T],
        *,
        context: contextvars.Context | None = None,
        eager_start: bool | None = None,
        **kwargs: Any,
    ) -> Task:
        """Spawn ``func`` as a child of this group and return the ``Task``."""

        if not self._entered:
            raise RuntimeError(f"TaskGroup {self!r} has not been entered")
        if self._exiting and not self._tasks:
            raise RuntimeError(f"TaskGroup {self!r} is finished")
        if self._aborting:
            raise RuntimeError(f"TaskGroup {self!r} is shutting down")
        scheduler = self._scheduler
        if scheduler is None:
            scheduler = get_scheduler()
            self._scheduler = scheduler
        if not self._tasks:
            self._done = Event()
        task = scheduler.spawn(func, context=context, eager_start=eager_start, **kwargs)
        self._tasks.add(task)
        task.add_done_callback(self._on_task_done)
        return task

    create_task = spawn

    def cancel(self) -> None:
        """Cancel unfinished children. Does not cancel the parent body."""

        self._abort_children()

    def _ours(self, exc: BaseException | None) -> bool:
        return isinstance(exc, _TaskGroupCancelled) and exc.task_group is self

    def _abort_children(self) -> None:
        self._aborting = True
        current = tealet.current()
        for task in tuple(self._tasks):
            if task is current or task.done():
                continue
            task.throw(_TaskGroupCancelled(self))

    def _request_abort(self) -> None:
        self._abort_children()
        if self._exiting:
            return
        parent = self._parent
        if parent is None or parent.done():
            return
        current = tealet.current()
        if parent is current:
            raise _TaskGroupCancelled(self)
        parent.throw(_TaskGroupCancelled(self))

    def _on_task_done(self, future: Future[Any]) -> object:
        if not isinstance(future, Task):
            return None
        self._tasks.discard(future)
        if not self._tasks:
            self._done.set()
        if future.cancelled():
            return None
        exc = future.exception()
        if exc is None:
            return None
        if isinstance(exc, (SystemExit, KeyboardInterrupt)):
            if self._base_error is None:
                self._base_error = exc
        elif isinstance(exc, Exception):
            self._errors.append(exc)
        elif self._base_error is None:
            self._base_error = exc
        if not self._aborting:
            scheduler = self._scheduler
            if scheduler is None:
                scheduler = get_scheduler()
            # throw from drain, not from the dying child's resolve_target
            scheduler.call_soon(self._request_abort)
        return None

    def _join(self) -> None:
        while self._tasks:
            try:
                self._done.swait()
            except CancelledError as exc:
                if not self._ours(exc):
                    self._abort_children()
                continue

    def _raise_errors(self) -> None:
        if self._base_error is not None:
            raise self._base_error
        errors = self._errors
        if not errors:
            return
        raise ExceptionGroup("unhandled errors in a TaskGroup", errors) from None

    def _exit(self, et: type[BaseException] | None, exc: BaseException | None) -> bool:
        our_cancel = self._ours(exc)
        if et is not None and not self._aborting:
            self._abort_children()
        self._join()
        if self._base_error is not None:
            raise self._base_error
        if our_cancel:
            self._raise_errors()
            return False
        if et is None:
            self._raise_errors()
            return False
        if isinstance(exc, CancelledError):
            if self._errors:
                self._raise_errors()
            return False
        if isinstance(exc, Exception):
            self._errors.append(exc)
            self._raise_errors()
            return False
        # BaseException in the body (RawTimeoutError, SystemExit, …): join is
        # done; re-raise so Timeout.__exit__ can convert its sentinel.
        return False

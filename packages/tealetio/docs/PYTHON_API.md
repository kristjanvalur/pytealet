# tealetio Python API Reference

This document describes the Python-facing API for `tealetio`, the scheduler,
synchronisation, selector, runner, and asyncio coexistence package built on top
of `tealet`.

Status note:
- The project is pre-1.0 and APIs may evolve.
- `tealetio` depends on `tealet`; `tealet` does not depend on `tealetio`.

## Import Surface

The top-level `tealetio` package re-exports the common scheduler, task/future,
lock, selector, runner, and asyncio bridge APIs. This follows the `asyncio`
pattern: submodules define explicit public names with `__all__`, and
`tealetio.__init__` imports and combines those surfaces.

Common imports can use the package root:

```python
from tealetio import Event, Future, Scheduler, gather, run, wait_for
from tealetio import AsyncRunner, AsyncScheduler, SelectorScheduler
```

Submodule imports remain supported when code wants to make the implementation
home explicit:

```python
from tealetio.scheduler import Scheduler
from tealetio.runner import run
```

## Scheduler Accessors

`tealetio.set_scheduler(scheduler)` binds a scheduler as current in the active
context. Passing `None` clears the current scheduler binding.

`tealetio.get_scheduler()` returns the current scheduler, whether or not
it is actively running. It raises `RuntimeError` if no scheduler is currently
bound and never creates one implicitly.

`tealetio.get_running_scheduler()` returns the current scheduler only while it is
actively driving work. It raises `RuntimeError` if no scheduler is running and
never creates one implicitly.

`tealetio.get_current()` returns the currently running `TealetTask`, or `None`
when the caller is outside a scheduler-owned tealet task. Asyncio tasks therefore
see `None` rather than an unrelated low-level tealet object. This includes
coroutines that a tealet task waits for through `BaseScheduler.await_(...)`.

`tealetio.asyncio_get_current()` returns the current `asyncio.Task`, but returns
`None` while execution is inside a scheduler-owned tealet task. Asyncio runners
hosted by `run_asyncio_in_tealet(...)` clear that tealetio task scope before
entering the asyncio entry point, so ordinary asyncio tasks remain visible.

## Scheduler Asyncio Bridge

`BaseScheduler.await_(awaitable) -> object` waits for an asyncio awaitable from
the current tealet task and returns its result.

For coroutine objects and awaitables that `await_()` wraps in a new asyncio task,
execution starts in a copy of the current `contextvars.Context`. Existing asyncio
`Future` and `Task` objects keep the context they already captured.

When optional `asynkit` support is available, coroutine objects are started with
`asynkit.CoroStart`; if they complete synchronously, `await_()` returns without
creating an asyncio task. If they block, their continuation is handed to asyncio
and waited on normally.

## Scheduler Waiting Helpers

`scheduler.runnable_tasks()` returns the scheduler-owned `TealetTask` instances
currently waiting to run, in scheduler order. It is an introspection helper for
advanced scheduling and debugging; blocked and completed tasks are not included.

`scheduler.reschedule(task, position=0)` moves a runnable task to a new runnable
queue position. Position `0` makes it the next scheduler-owned task to run.
Negative positions count back from the end, with `-1` placing the task at the
tail. The task must belong to that scheduler and must already be runnable.

`scheduler.yield_to(task, insert_current_at=-1)` yields from the current tealet to
a runnable task and keeps the current task runnable. The target is removed from
the runnable queue before interpreting `insert_current_at`, so position `0` means
the current task is next in line once the target blocks or completes. The default
`-1` leaves the current task at the end of the runnable queue; lower negative
values count back from there, so `-2` inserts before the final queued task.

`scheduler.ensure_future(entry)` returns a scheduler `Future` for one entry.
Existing scheduler futures are returned unchanged, and zero-argument callables
are spawned as scheduler tasks.
`tealetio.ensure_future(entry)` delegates to the current scheduler.

`tealetio.gather(*entries, return_exceptions=False)` returns a scheduler
`Future` for ordered child results. Entries may be scheduler futures/tasks or
zero-argument callables, which are spawned as scheduler tasks.

`tealetio.wait(entries, *, timeout=None, return_when=ALL_COMPLETED)`
returns a scheduler `Future` whose result is `(done, pending)`. The
`return_when` value may be `FIRST_COMPLETED`, `FIRST_EXCEPTION`, or
`ALL_COMPLETED`. Timeout completion does not cancel pending children.

`tealetio.wait_for(entry, timeout)` returns a scheduler `Future` for one child
result. If the timeout expires, the wrapper raises `TimeoutError` and cancels the
child future.

`tealetio.as_completed(entries, *, timeout=None)` is a tealet-blocking
iterator that yields scheduler futures in child completion order. If the timeout
expires before all children finish, iteration raises `TimeoutError`; unfinished
children are not cancelled by `as_completed(...)`.

## Synchronisation Primitives

`tealetio` provides scheduler-aware synchronisation primitives modelled after
`asyncio` where practical. Plain method names follow the asyncio-facing API, and
`s`-prefixed methods are tealet-blocking variants for synchronous tealet code.

Common primitives include:
- `Event.wait()` / `Event.swait()`
- `Lock.acquire()` / `Lock.sacquire()`
- `Semaphore.acquire()` / `Semaphore.sacquire()`
- `Condition.wait()` / `Condition.swait()`
- `Barrier.wait()` / `Barrier.swait()`
- `Queue.put()` / `Queue.sput()`
- `Queue.get()` / `Queue.sget()`
- `Queue.join()` / `Queue.sjoin()`

`Queue.shutdown(immediate=False)` follows `asyncio.Queue.shutdown()` semantics
and raises `tealetio.QueueShutDown`. On Python versions with
`asyncio.QueueShutDown`, this is the standard-library exception; on older
versions, `tealetio` provides a same-named fallback exception.
- future `put()` / `put_nowait()` / `sput()` calls raise `QueueShutDown`;
- blocked async and tealet-blocking putters/getters are woken;
- graceful shutdown lets existing queued items drain, then future gets raise
  `QueueShutDown` once the queue is empty;
- immediate shutdown drains queued items, marks each drained item done for
  `join()` accounting, and wakes joiners if unfinished work reaches zero.

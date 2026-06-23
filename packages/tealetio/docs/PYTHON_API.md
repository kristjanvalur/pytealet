# tealetio Python API Reference

This document describes the Python-facing API for `tealetio`, the scheduler,
synchronization, selector, runner, and asyncio coexistence package built on top
of `tealet`.

Status note:
- The project is pre-1.0 and APIs may evolve.
- `tealetio` depends on `tealet`; `tealet` does not depend on `tealetio`.

## Scheduler Accessors

`tealetio.scheduler.set_scheduler(scheduler)` binds a scheduler as current in the
active context. Passing `None` clears the current scheduler binding.

`tealetio.scheduler.get_scheduler()` returns the current scheduler, whether or not
it is actively running. It raises `RuntimeError` if no scheduler is currently
bound and never creates one implicitly.

`tealetio.scheduler.get_running_scheduler()` returns the current scheduler only
while it is actively driving work. It raises `RuntimeError` if no scheduler is
running and never creates one implicitly.

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

`scheduler.ensure_future(entry)` returns a scheduler `Future` for one entry.
Existing scheduler futures are returned unchanged, and zero-argument callables
are spawned as scheduler tasks.
`tealetio.scheduler.ensure_future(entry)` delegates to the current scheduler.

`tealetio.scheduler.gather(*entries, return_exceptions=False)` returns a scheduler
`Future` for ordered child results. Entries may be scheduler futures/tasks or
zero-argument callables, which are spawned as scheduler tasks.

`tealetio.scheduler.wait(entries, *, timeout=None, return_when=ALL_COMPLETED)`
returns a scheduler `Future` whose result is `(done, pending)`. The
`return_when` value may be `FIRST_COMPLETED`, `FIRST_EXCEPTION`, or
`ALL_COMPLETED`. Timeout completion does not cancel pending children.

`tealetio.scheduler.wait_for(entry, timeout)` returns a scheduler `Future` for one
child result. If the timeout expires, the wrapper raises `TimeoutError` and
cancels the child future.

`tealetio.scheduler.as_completed(entries, *, timeout=None)` is a tealet-blocking
iterator that yields scheduler futures in child completion order. If the timeout
expires before all children finish, iteration raises `TimeoutError`; unfinished
children are not cancelled by `as_completed(...)`.

## Synchronization Primitives

`tealetio.locks` provides scheduler-aware synchronization primitives modeled
after `asyncio` where practical. Plain method names follow the asyncio-facing
API, and `s`-prefixed methods are tealet-blocking variants for synchronous
tealet code.

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
and raises `tealetio.locks.QueueShutDown`. On Python versions with
`asyncio.QueueShutDown`, this is the standard-library exception; on older
versions, tealet provides a same-named fallback exception.
- future `put()` / `put_nowait()` / `sput()` calls raise `QueueShutDown`;
- blocked async and tealet-blocking putters/getters are woken;
- graceful shutdown lets existing queued items drain, then future gets raise
  `QueueShutDown` once the queue is empty;
- immediate shutdown drains queued items, marks each drained item done for
  `join()` accounting, and wakes joiners if unfinished work reaches zero.

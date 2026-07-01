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
from tealetio import Event, Future, Scheduler, gather, run, sleep, spawn, wait_for
from tealetio import AsyncRunner, AsyncScheduler, SyncSelectorScheduler
```

Submodule imports remain supported when code wants to make the implementation
home explicit:

```python
from tealetio.scheduler import Scheduler
from tealetio.runner import run
```

`Scheduler` is an alias for the default synchronous scheduler and is backed by a proactor.
Use `SyncProactorScheduler` directly when you want to provide a custom proactor
factory for synchronous driving, and use `AsyncProactorScheduler` for the same
proactor-backed IO model under an async driving facade. `ProactorScheduler` is
the shared abstract proactor core. Likewise, `SelectorScheduler` is the shared
abstract selector core, with `SyncSelectorScheduler` and `AsyncSelectorScheduler`
as concrete driving variants. `run_asyncio_in_tealet(...)` chooses the hosted
asyncio loop from the scheduler it creates: proactor schedulers use
`TealetProactorEventLoop` with `ForwardingProactor`, while selector schedulers
use `TealetSelectorEventLoop` with `ForwardingSelector`. Use `BasicScheduler`
when you deliberately want the small no-IO driver that only waits for timers and
explicit scheduler wakeups. The shared task, timer, future, and callback
behaviour lives in the cooperative scheduling core; the blocking and
asyncio-hosted run loops are separate driving facades.

Proactors expose both `wait(deadline=None)` and `await wait_async(deadline=None)`.
The synchronous form blocks the current thread; the async form waits through the
running asyncio loop so future asyncio-hosted schedulers can pump tealetio-owned
IO completions without blocking asyncio itself. Deadlines use the proactor clock:
`None` waits forever, and `0` always means poll without blocking.

An operation may also complete before it ever reaches the backend wait queue.
In that case the proactor returns an already-done `Operation`, and callers can
read its result directly without switching or waiting. Selector-backed proactors
use this fast path for socket operations that succeed right away.

`UringProactor` also exposes positioned file I/O through io_uring:
`openat(path, flags, mode=0, *, dfd=AT_FDCWD)` returns a caller-owned fd,
`read(fd, n, offset)` and `read_into(fd, buf, offset)` read at an explicit
offset, and `write(fd, data, offset)` writes at an explicit offset. The `dfd`
argument is forwarded to `uring_api.submit_openat()` for directory-relative
opens. Selector-backed proactors do not implement these operations yet. Path,
flags, mode, offsets, and fds are forwarded unchanged to `uring_api`; kernel
and CQE errors surface as operation failures. `uring_api` may still raise
`ValueError` synchronously at submit time for some invalid offsets or buffers.

Long-lived socket operations use `ContinuousOperation`. `accept_many(sock,
callback)` emits `(conn, address)` for each accepted connection and remains
active until it is cancelled or the backend reports a terminal error.
`poll(fd, mask)` waits for fd readiness and returns a one-shot `Operation[int]`.
The result is the event bitmask currently set on the fd (`select.POLL*` bits
among those requested in `mask`). `poll_many(fd, mask, callback)` emits that
bitmask on each readiness event and remains active until cancelled or the
backend reports a terminal error. Poll works on any file descriptor, not only
sockets.

`SelectorProactor` probes immediate readiness with `select.select()` and
registers the fd with the internal selector when the fd is not ready yet. It
maps `POLLIN`, `POLLPRI`, `POLLOUT`, `POLLERR`, `POLLHUP`, and `POLLRDHUP`
(when the platform defines it) onto `select()` read/write/exception fd lists;
other `select.POLL*` bits are not supported and raise `ValueError`. `POLLERR`
and `POLLHUP` register for both read and write selector wakeups, and
immediate probing also checks the read fd list because Linux often reports
peer hangup as readability rather than an exception-set wakeup. It allows
at most one pending operation per fd per direction (so `poll(POLLIN)` conflicts
with an in-flight `recv_many` on the same socket).

`UringProactor` forwards `mask` and `fd` unchanged to io_uring; invalid
arguments surface as operation/CQE errors rather than pre-submit `ValueError`.
Poll results pass through `completion.res` as the kernel reports them and may
include bits outside the requested `mask`; the selector backend intersects
results with the request. Poll and socket receive/send operations may coexist
on the same fd — for example `poll_many(POLLIN)` alongside `recv_many()` on one
socket. That overlap is uncommon and rarely problematic; the uring path
deliberately does not enforce selector-style per-fd exclusivity.

`recv_many(sock, callback)` emits `(index, data)` pairs for each received byte
chunk, where `index` is the ordinal position in the receive stream and `data`
is a read-only `memoryview` into the received bytes. EOF emits one final empty
view before the operation completes. Chunk sizes are backend-defined:
`UringProactor` uses the shared `BufGroup` slot size (16 KiB by default) when
multishot provided-buffer receive is available, and `SelectorProactor` reads up
to 8 KiB per `recv()` call. Each `UringProactor` instance lazily creates one
`BufGroup` (16 KiB × 256 buffers by default) shared by `recv_many` and
`recvall` when multishot receive is in use. `recvgen` creates a dedicated pool
per generator (defaults: 16 KiB × 8). Concurrent long-lived `recv_many`
streams on one `UringProactor` therefore draw from the same provided-buffer
pool: a slow consumer on one stream can trigger `RECV_MANY_BUFFER_PRESSURE` or
stall another stream even when the second would otherwise fit. Use separate
`UringProactor` instances when independent streams need isolated buffer pools.
When the provided-buffer pool is exhausted on `UringProactor`, the callback
receives `(RECV_MANY_BUFFER_PRESSURE, resume)`; drop held views and call
`resume()` to re-arm multishot receive (stream indices continue from the failed
completion's `sequence`). On Python 3.12+, `SelectorProactor.recv_many` uses a
synthetic pool with the same `(RECV_MANY_BUFFER_PRESSURE, resume)` contract;
older CPython falls back to unpaced reads without pool pressure. Callbacks
receive borrowed views:
copy with `bytes(data)` when you need to keep payload past the callback, and
drop view references you no longer need so backend buffers can be recycled
(refcount teardown is enough; `memoryview.release()` is optional for early
release and `memoryview` has no `close()` on 3.12+). On
`UringProactor`, holding too many live views can pin the shared provided-buffer
pool and stall further receives.

When `IORING_RECV_MULTISHOT` is unavailable, `UringProactor.recv_many()` falls
back to repeated one-shot `submit_recv()` calls. Chunks are independent
`memoryview` objects over copied bytes (not leased `BufView` results), chunk
size is up to 8 KiB, stream indices stay in-order, and
`RECV_MANY_BUFFER_PRESSURE` is never emitted. `recvall` and `recvgen` inherit
this degraded mode automatically.

When `IORING_ACCEPT_MULTISHOT` is unavailable, `UringProactor.accept_many()`
falls back to repeated one-shot `submit_accept()` after each accepted
connection.

When `IORING_POLL_MULTISHOT` is unavailable, `UringProactor.poll_many()` falls
back to repeated one-shot `submit_poll()` after each readiness event.
Multishot cancel uses `submit_poll_remove()`; the oneshot fallback cancels the
pending SQE like other degraded `*many` paths.

`UringProactor.capabilities` exposes the `uring_api.probe(entries=...,
flags=...)` result captured once at construction, so callers and the proactor
itself can gate behaviour without re-running runtime probes.

Backends may run these result callbacks from any worker thread; code that needs
thread affinity should marshal from the callback into the appropriate scheduler,
event loop, or application thread.

`recvall(sock, progress=None)` builds on `recv_many(...)` and returns a
normal one-shot `Operation[bytes]`. It keeps chunk views borrowed from
`recv_many` until provided-buffer pressure arrives, then copies every held
chunk to `bytes` so leased slots return to the shared pool. At EOF it
concatenates chunks in stream-index order with `bytes(chunk)`; for stored
`bytes` chunks that is an identity no-op on CPython. Remaining borrowed views
are released by dropping recvall's references. When provided,
`progress(total)` is called after each received non-empty chunk with the
cumulative number of bytes received.

`recvgen(sock)` is a tealet-blocking generator that incrementally yields
`(index, data)` chunks in stream-index order until EOF. Each `data` is a
read-only `memoryview`; copy with `bytes(data)` when owned storage is required
past the current iteration step. Unlike `recv_many`, it does not yield a final
`(index, empty_view)` EOF tuple; iteration ends when the stream completes (the
generator raises `StopIteration` / returns from `sock_recvgen`). Use
`recv_many` directly when you need the documented EOF sentinel and exact
`recv_many` callback semantics.

`(RECV_MANY_BUFFER_PRESSURE, None)` is yielded when the provided-buffer pool is
exhausted. Consumers should drop every receive `memoryview` they still hold when
that token appears and avoid keeping more views than needed between reads.

Out-of-order multishot completions are reordered before yield. The generator
must be consumed from a scheduler tealet so `ThreadsafeEvent.swait()` can
block cooperatively. `ProactorScheduler.sock_recvgen(sock, ...)` exposes the
same surface on scheduler instances.

IO-capable schedulers also expose blocking poll helpers on top of the proactor
or selector backends. `poll(fd, mask)` waits cooperatively and returns the
readiness bitmask. `poll_many(fd, mask, callback)` starts a continuous poll
and forwards each readiness event to `callback`. `ProactorScheduler` implements
these through `wait_operation()` and proactor `poll`/`poll_many`.
`SelectorScheduler` implements them with selector-backed readiness waits and
the same `select.POLL*` mask semantics as `SelectorProactor`.

`sendall(sock, data, progress=None)` also accepts an optional progress callback.
Backends call `progress(total)` with the cumulative number of bytes sent as
progress becomes observable. Some backends may only expose a single completion
for the whole send, in which case they report one final total.

Proactors also expose `set_completion_callback(callback)`. A real thread-backed
proactor should call this callback when completions are queued so an async host
can wake its event loop, for example with `loop.call_soon_threadsafe(...)`.
`break_wait()` remains separate: it interrupts a blocking proactor wait without
reporting an IO completion.

`SelectorProactor` is the simple single-threaded selector-backed prototype.
`ThreadedSelectorProactor` uses the same socket operation surface, but polls the
selector from a worker thread and queues completions for `wait(...)` or
`wait_async(...)`. That makes it useful for exercising the thread-callback shape
expected from future OS-backed proactors without making the default selector
prototype more complicated.

## Scheduler Accessors

`tealetio.set_scheduler(scheduler)` binds a scheduler as current in the active
context. Passing `None` clears the current scheduler binding.

`tealetio.get_scheduler()` returns the current scheduler, whether or not
it is actively running. It raises `RuntimeError` if no scheduler is currently
bound and never creates one implicitly.

`tealetio.get_running_scheduler()` returns the current scheduler only while it is
actively driving work. It raises `RuntimeError` if no scheduler is running and
never creates one implicitly.

`tealetio.get_current()` returns the currently running `Task`, or `None`
when the caller is outside a scheduler-owned tealet task. Asyncio tasks therefore
see `None` rather than an unrelated low-level tealet object. This includes
coroutines that a tealet task waits for through `BaseScheduler.await_(...)`.

`tealetio.asyncio_get_current()` returns the current `asyncio.Task`, but returns
`None` while execution is inside a scheduler-owned tealet task. Asyncio runners
hosted by `run_asyncio_in_tealet(...)` clear that tealetio task scope before
entering the asyncio entry point, so ordinary asyncio tasks remain visible.

`tealetio.sleep(delay)` suspends the current scheduler task on the running
scheduler. `sleep(0)` is the tealetio yield checkpoint, matching the familiar
`asyncio.sleep(0)` pattern without scheduling a timer.

`tealetio.spawn(func)` creates a `Task` on the current scheduler from a
zero-argument callable. `tealetio.create_task(func)` is an asyncio-style alias
for the same operation; `spawn(...)` is the native tealetio spelling.

## Scheduler Main Context

`scheduler.main_context()` is the low-level boundary for direct scheduler task
access from the process main tealet. It temporarily wraps the current main
tealet with the scheduler's configured task class, so operations that transfer
or inspect scheduler-owned tasks see a `Task`-shaped current tealet.

High-level driving APIs enter this context for you. That includes scheduler
drivers such as `run()`, `run_forever()`, `run_until_complete(...)`, and
their async counterparts, as well as `Runner.run()`, `Runner.close()`,
`AsyncRunner.run()`, and `AsyncRunner.aclose()`.

`AsyncScheduler.arun(yield_every=N)`,
`AsyncScheduler.arun_forever(yield_every=N)`, and
`AsyncScheduler.arun_until_complete(..., yield_every=N)` bound each scheduler
batch to at most `N` scheduling transfers before yielding to asyncio with
`asyncio.sleep(0)` if runnable scheduler work remains. With `yield_every=None`,
`arun(...)` and `arun_forever(...)` run each scheduler batch without an early
batch limit, while `arun_until_complete(...)` uses the runnable queue length at
batch entry.

Use it explicitly only when raw main code manipulates scheduler tasks directly:

```python
with scheduler.main_context():
    task.cancel()
    scheduler.run_until_complete(task)
```

Code already running inside a scheduler-owned `Task` does not need this
wrapper, and task transfer methods do not install it implicitly.

## Task Priorities

`PriorityTask` is a `Task` subclass for schedulers that use a priority
runnable queue. Its `priority` property is a float, and changing it notifies the
current task link so the queue can recompute runnable order when the task is
already waiting to run. `get_effective_priority()` returns the priority the
scheduler should use right now, including inherited priority from
`PriorityLock` waiters.

Priority values follow Python priority queue and Unix `nice` intuition: lower
numeric values run first. The public constants provide spaced bands with room
for intermediate values:

```python
TASK_PRIORITY_CRITICAL = -20.0
TASK_PRIORITY_HIGH = -10.0
TASK_PRIORITY_DEFAULT = 0.0
TASK_PRIORITY_LOW = 10.0
TASK_PRIORITY_IDLE = 20.0
```

For example, the built-in task factories can construct priority tasks directly:

```python
scheduler.set_task_factory(DefaultTaskFactory(task_constructor=PriorityTask))
scheduler.spawn(worker, priority=TASK_PRIORITY_HIGH)
```

The factory passes extra `spawn(...)` keyword arguments to the task constructor
before the scheduler makes the task runnable.

Schedulers accept a `runnable_queue_factory` for applications that need a
specific runnable policy. The default is `PrescheduledRunnableQueue`, which keeps
FIFO ordering with an immediate lane for explicit `reschedule(...)` and
`yield_to(...)` operations. `PriorityRunnableQueue` is the built-in priority
policy, and it is intended to be paired with `PriorityTask`:

```python
from tealetio import DefaultTaskFactory, PriorityRunnableQueue, PriorityTask, Scheduler

scheduler = Scheduler(runnable_queue_factory=PriorityRunnableQueue)
scheduler.set_task_factory(DefaultTaskFactory(task_constructor=PriorityTask))
scheduler.spawn(worker, priority=TASK_PRIORITY_HIGH)
```

The public runnable queue symbols are `FifoRunnableQueue`,
`PrescheduledRunnableQueue`, `PriorityRunnableQueue`, `RunnableQueue`, and
`RunnableQueueFactory`. Custom queue implementations should satisfy the
`RunnableQueue` protocol so the scheduler can add, discard, pop, reschedule, and
introspect runnable tasks without knowing the queue's concrete policy.

`PriorityLock` is the priority-aware counterpart to `Lock` for tealet code. It
supports `sacquire()` / `with lock:` from scheduler-owned tasks and
`acquire()` / `async with lock:` from asyncio tasks. It keeps the same FIFO lock
waiter policy as `Lock`. When `PriorityTask` instances participate, a
low-priority owner inherits the best waiter priority until it releases the lock,
which avoids the usual low/medium/high priority inversion. Regular tealet tasks
and asyncio tasks participate with the default priority.

## Scheduler Asyncio Bridge

`BaseScheduler.await_(awaitable) -> object` waits for an asyncio awaitable from
the current tealet task and returns its result.

Coroutine objects are driven directly through their await protocol with
`asynkit.coro_drive`. If the coroutine completes synchronously, `await_()`
returns without creating an asyncio task. If it yields an asyncio future-like
object, tealetio waits for that future and resumes the same coroutine driver
when it completes.

Coroutine objects run in the current `contextvars.Context`, with tealetio's
current-task marker temporarily cleared while the coroutine is driven. Existing
asyncio `Future` and `Task` objects keep the context they already captured.
Generic non-coroutine awaitables that `await_()` wraps in a new asyncio task run
in a copy of the current context.

## Scheduler Waiting Helpers

`scheduler.runnable_tasks()` returns the scheduler-owned `Task` instances
currently waiting to run, in scheduler order. It is an introspection helper for
advanced scheduling and debugging; blocked and completed tasks are not included.

`scheduler.reschedule(task, position=None)` moves a runnable task to a new
runnable queue position. By default, the task returns through normal runnable
policy. Passing an integer inserts the task into the immediate lane; position
`0` makes it the next scheduler-owned task to run. Negative positions count back
from the end of the immediate lane, with out-of-range values truncated like list
insertion. The task must belong to that scheduler and must already be runnable.

`scheduler.yield_to(task, insert_current_at=None)` yields from the current tealet
to a runnable task and keeps the current task runnable. The target is placed in
the immediate lane. By default, the current task is returned through the normal
runnable policy. Passing an integer inserts the current task into the immediate
lane after the target; position `0` means the current task is next once the
target blocks or completes. Negative values count from the end of the immediate
lane, with out-of-range values truncated like list insertion.

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

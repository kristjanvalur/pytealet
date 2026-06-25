# Scheduler Runtime API Spec (Draft)

Status: Active draft with partial implementation landed.

This document defines a top-level runtime API that harmonizes tealet scheduler
selection with asyncio loop selection, while preserving scheduler terminology in
tealet public APIs.

## Current Implementation Snapshot

The repository now contains a meaningful subset of this design.

Implemented:

- Runner split and driving split are in place:
  - `tealetio.runner.Runner` drives sync scheduler execution.
  - `tealetio.asyncio.AsyncRunner` drives async scheduler execution inside an existing asyncio task.
- Shared runner lifecycle has been consolidated in a generic `BaseRunner`.
- Runner factories use per-runner defaults and factory-only construction.
  - `Runner` uses `tealetio.scheduler.Scheduler` as its default factory.
  - `AsyncRunner` uses `tealetio.asyncio.AsyncScheduler` as its default factory.
  - Custom factories are duck typed; runtime does not validate returned objects.
- Top-level convenience helpers exist:
  - `tealetio.runner.run(...)`
  - `tealetio.asyncio.run_async(...)`
  - `tealetio.asyncio.run_in_asyncio(...)`
  - `tealetio.asyncio.run_asyncio_in_tealet(...)`
- `BaseScheduler` contains shared cooperative scheduling mechanics.
- `Scheduler` is the concrete synchronous scheduler implementation.
- `AsyncScheduler` is the concrete asyncio-hosted scheduler implementation.
- `Scheduler` and `AsyncScheduler` can be used directly as factories. They share
  the common scheduler/task/timer APIs from `BaseScheduler`, while implementing
  different driving APIs.
- Low-level IO callback hooks are exposed on the scheduler surface:
  - `add_reader(...)`
  - `remove_reader(...)`
  - `add_writer(...)`
  - `remove_writer(...)`
  `tealetio.asyncio.AsyncScheduler` delegates these hooks to the running asyncio loop.
  `tealetio.selector.SelectorScheduler` implements them through its native selector reactor.
  Selector readiness waits (`wait_readable(...)` and `wait_writable(...)`) are
  layered on top of one-shot reader/writer callbacks that wake tealet `Event`
  waiters.
- Scheduler driving APIs include both sync and async run entry points:
  - `run_until_complete(...)`
  - `run_forever(...)`
  - `arun_until_complete(...)`
  - `arun_forever(...)`
- Future waiting semantics are aligned so wait paths return final results.
- Current scheduler binding is thread-local through
  `tealetio.scheduler.set_scheduler(...)` and is restored by runner scopes. Async
  context-local isolation across multiple asyncio tasks in the same thread is
  future hardening work, not current behavior.
- Cancellation is represented by `asyncio.CancelledError`, matching asyncio
  `Future`/`Task` behavior. A stored `CancelledError` is the cancellation state
  indicator for scheduler futures and tealet tasks.
- Scheduler task introspection includes `BaseScheduler.all_tasks()`, which
  returns unfinished scheduler-owned tealet tasks without keeping those tasks
  alive solely for introspection.
- Scheduler runnable introspection and explicit rescheduling are available
  through `BaseScheduler.runnable_tasks()`, `BaseScheduler.reschedule(...)`, and
  `BaseScheduler.yield_to(...)`. Runnable scheduling is task-centric rather than
  callback-centric, and the default queue preserves FIFO behaviour. `yield_to()`
  keeps the caller runnable. By default, the caller returns through normal queue
  policy; explicit `insert_current_at` indexes place it in the immediate lane
  after the yielded-to target, using normal list-style insertion.
  `reschedule(..., position=None)` likewise returns a task through normal queue
  policy, while integer positions place the task in the immediate lane.
- Scheduler grouping includes `BaseScheduler.ensure_future(...)`,
  `tealetio.scheduler.ensure_future(...)`, `tealetio.scheduler.gather(...)`,
  `tealetio.scheduler.wait(...)`, `tealetio.scheduler.wait_for(...)`, and
  `tealetio.scheduler.as_completed(...)` for entry normalization, ordered
  collection, done/pending waiting, timeout-bounded single waits, and
  completion-order iteration.
- Scheduler-local task creation is configurable with
  `BaseScheduler.set_task_factory(...)` and `BaseScheduler.get_task_factory()`.
  The default factory preserves direct `Task.prepare(...)` behavior.
  `tealetio.tasks.StubTaskFactory` can create and reuse a prepared stub via
  `stub_here()`, then create scheduler tasks from that stub.
- The scheduler main-tealet context is explicit through
  `BaseScheduler.main_context()`. It installs the scheduler's task factory as
  the low-level tealet wrapper factory for the current main tealet, so direct
  task access from main code sees a scheduler-owned `Task` wrapper rather
  than a raw `_tealet.tealet` wrapper.
- Scheduler and runner driving entry points enter `main_context()`
  automatically while constructing, cancelling, transferring, and driving
  scheduler tasks. Low-level code that manipulates scheduler tasks directly
  from the raw main tealet must enter `with scheduler.main_context():`
  explicitly.
- Cancellation propagates across scheduler boundaries in an asyncio-compatible
  way:
  - cancelling an asyncio waiter on a tealet `Future` schedules cancellation of
    the underlying tealet future/task through the running scheduler
  - cancelling a tealet task waiting on a tealet `Future` schedules cancellation
    of the awaited future/task through the running scheduler
  - cancelling a tealet task waiting in `await_(...)` schedules cancellation
    of the awaited asyncio future/task with `loop.call_soon(...)`
  - `await_(awaitable)` delegates awaitable execution through asyncio without
    exposing a separate `context=` argument, matching the shape of Python's
    `await` expression
  - delegated coroutine objects and newly-created asyncio tasks run in a copy
    of the current `contextvars.Context`; existing asyncio `Future` and `Task`
    objects retain their already-captured context
  - awaiting an already-cancelled tealet or asyncio future raises the same
    `CancelledError` through the tealet/asyncio boundary
  - shielding prevents waiter cancellation from propagating into the shielded
    underlying future, matching `asyncio.shield(...)`
- Wait primitives are responsible for exception-safe cleanup. If a `Task`
  receives an exception while blocked, including cancellation or tealet exit,
  the wait path must unlink the task from every scheduler-owned waiter list or
  bridge registration before propagating the exception. This is the core
  robustness rule for cancelled tasks blocked in events, futures, timers,
  selector waits, channels, locks, queues, or asyncio bridge waits.
- Runner-level SIGINT handling is implemented for Python 3.11+, following
  `asyncio.Runner` policy:
  - the runner installs a temporary SIGINT handler while driving the main
    tealet task
  - the first interrupt schedules cancellation of that main task through the
    active scheduler and converts the resulting `CancelledError` into
    `KeyboardInterrupt`
  - a second interrupt raises `KeyboardInterrupt` immediately
  - nested asyncio runner handlers are temporarily overridden and restored
    after the inner runner exits
  - runner construction accepts `handle_sigint=False` for embedding scenarios
    where an inner runner should own interrupt handling
- Runner shutdown follows the asyncio runner pattern and uses normal task
  cancellation rather than a distinct shutdown-specific wait policy:
  - unfinished scheduler tasks are cancelled and drained with
    `tealetio.scheduler.gather(..., return_exceptions=True)`; this is robust as
    long as synchronisation primitives correctly clean up when blocked
    `Task` instances receive cancellation or tealet-exit exceptions
  - `CoreSchedulerDrivingAPI.shutdown_default_executor(timeout=300.0)` returns
    a scheduler `Future` that waits for the detached default executor to shut
    down cleanly, warning and continuing if the asyncio-parity timeout expires
  - sync `Runner.close()` drives the shutdown futures with
    `run_until_complete(...)`; async `AsyncRunner.aclose()` uses
    `arun_until_complete(...)`

Remaining from this proposal:

- Continue hardening the low-level IO and tealet-hosted asyncio loop surfaces.

## Goals

- Provide a simple top-level API to configure both:
  - asyncio loop creation
  - tealet scheduler creation
- Keep tealet naming explicit: scheduler, not loop.
- Offer high-level run-until-done entry points for sync and async use cases.
- Mirror modern asyncio direction (explicit factories, not global policy).
- Prefer explicit scheduler construction and runner factories over global helper
  creation.

## Non-goals

- Reintroducing a global event loop policy equivalent.
- Supporting multiple running schedulers in a single thread/task context.
- Full replacement of asyncio task semantics in this phase.

## Terms

- Current scheduler: thread-local scheduler that may exist but may not be
  running in the current implementation. A context-local async binding model is
  future work.
- Running scheduler: scheduler currently executing/pumping work.
- Runtime scope: bounded execution context created by a high-level runner.

## Proposed Public API

### Scheduler Access Functions

```python
def set_scheduler(scheduler: BaseScheduler | None) -> None: ...
def get_scheduler() -> BaseScheduler: ...
def get_running_scheduler() -> BaseScheduler: ...
```

Semantics:

- `set_scheduler(scheduler)`:
  - Installs scheduler as current in the active context.
  - If argument is `None`, clears current scheduler binding.

- `get_scheduler()`:
  - Returns the current scheduler in this execution context, whether or not it
    is actively running.
  - Raises `RuntimeError` if no current scheduler is bound.
  - Never creates or installs a scheduler.

- `get_running_scheduler()`:
  - Returns the scheduler currently running in this execution context.
  - Raises `RuntimeError` if no running scheduler exists.
  - Never creates or installs a scheduler.

### Scheduler Main Context

```python
class BaseScheduler:
    def main_context(self) -> ContextManager[None]: ...
```

Semantics:

- `main_context()` temporarily configures the low-level tealet factory so the
  current main tealet is wrapped using this scheduler's configured task constructor.
  With the default factory that wrapper is a `Task`; with a priority
  factory it may be a `PriorityTask` or another scheduler-compatible subclass.
- The context is reentrant for the same scheduler. Entering it again while the
  same scheduler factory is active is a no-op, and exiting restores the previous
  low-level tealet factory.
- Code running inside a scheduler-owned `Task` already has the right
  scheduler shape. Code running from the raw process main tealet must enter
  `with scheduler.main_context():` before directly cancelling, throwing into,
  rescheduling, yielding to, or otherwise transferring scheduler-owned tasks.
- `Task.run()`, `Task.throw(...)`, and cancellation do not create
  this context themselves. The boundary is owned by scheduler/runner driving
  APIs and by explicit low-level callers.
- Sync driving entry points (`run()`, `run_forever()`,
  `run_until_complete(...)`, and `pump(...)`) and async driving entry points
  (`arun(...)`, `arun_forever()`, and `arun_until_complete(...)`) enter
  `main_context()` automatically. `Runner.run()`, `Runner.close()`,
  `AsyncRunner.run()`, and `AsyncRunner.aclose()` also enter it while creating
  and draining their main and shutdown tasks.
- Priority-based schedulers also temporarily give the driving main tealet an
  internal infinity priority while the scheduler is being driven. This keeps
  real runnable work ahead of the driver and is an implementation detail rather
  than a public priority band.

Example:

```python
with scheduler.main_context():
    task.cancel()
    scheduler.run_until_complete(task)
```

### Scheduler Grouping

```python
def ensure_future(
  entry: Future[Any] | Callable[[], Any],
) -> Future[Any]: ...

class BaseScheduler:
    def ensure_future(
        self,
    entry: Future[Any] | Callable[[], Any],
    ) -> Future[Any]: ...

def gather(
    *entries: Future[Any] | Callable[[], Any],
    return_exceptions: bool = False,
) -> Future[list[Any]]: ...

def wait(
  entries: Iterable[Future[Any] | Callable[[], Any]],
  *,
  timeout: float | None = None,
  return_when: Literal["FIRST_COMPLETED", "FIRST_EXCEPTION", "ALL_COMPLETED"] = "ALL_COMPLETED",
) -> Future[tuple[set[Future[Any]], set[Future[Any]]]]: ...

def wait_for(
  entry: Future[Any] | Callable[[], Any],
  timeout: float | None,
) -> Future[Any]: ...

def as_completed(
  entries: Iterable[Future[Any] | Callable[[], Any]],
  *,
  timeout: float | None = None,
) -> Iterator[Future[Any]]: ...

def sleep(delay: float) -> None: ...

def spawn(func: Callable[[], T], **kwargs: Any) -> Task: ...
create_task = spawn
```

Semantics:

- `BaseScheduler.ensure_future(...)` returns existing scheduler futures
  unchanged and spawns zero-argument callables as scheduler tasks.
  `tealetio.scheduler.ensure_future(...)` delegates to the current scheduler.
- `gather(...)` accepts scheduler futures/tasks and zero-argument callables,
  converts callables into scheduler-owned tealet tasks, and returns results in
  input order.
- With `return_exceptions=False`, the first child exception completes the group
  future with that exception.
- With `return_exceptions=True`, child exceptions are collected into the result
  list alongside successful values.
- Cancelling the group future requests cancellation of unfinished children.
- `wait(...)` completes with `(done, pending)` sets and does not cancel pending
  children when its timeout expires.
- `wait_for(...)` completes with the child result, propagates child exceptions,
  and cancels the wrapped child future on timeout.
- `as_completed(...)` is a tealet-blocking iterator that yields scheduler
  futures in child completion order. If its timeout expires before all inputs
  finish, iteration raises `TimeoutError` without cancelling the unfinished
  children.

### Scheduler Task Factories

```python
from tealetio.tasks import (
    DefaultTaskFactory,
    PriorityTask,
    StubTaskFactory,
    TaskConstructor,
    TaskFactory,
    TASK_PRIORITY_CRITICAL,
    TASK_PRIORITY_DEFAULT,
    TASK_PRIORITY_HIGH,
    TASK_PRIORITY_IDLE,
    TASK_PRIORITY_LOW,
)

TaskConstructor = Callable[..., Task]


class TaskFactory(Protocol):
    @property
    def task_constructor(self) -> TaskConstructor: ...

    def __call__(
        self,
        scheduler: BaseScheduler,
        func: Callable[[], object],
        *,
        context: contextvars.Context,
        eager_start: bool | None = None,
        **kwargs: Any,
    ) -> Task: ...

class DefaultTaskFactory:
    def __init__(
        self,
        *,
        task_constructor: TaskConstructor = Task,
        eager_start: bool = False,
    ) -> None: ...

class StubTaskFactory:
    def __init__(
        self,
        stub: tealet.tealet | None = None,
        *,
        task_constructor: TaskConstructor = Task,
        eager_start: bool = False,
    ) -> None: ...
    def stub_here(self) -> tealet.tealet: ...

class PriorityTask(Task):
    def __init__(
        self,
        owning_scheduler: BaseScheduler,
        priority: float = TASK_PRIORITY_DEFAULT,
    ) -> None: ...

    @property
    def priority(self) -> float: ...

    @priority.setter
    def priority(self, value: float) -> None: ...

    def get_effective_priority(self) -> float: ...

class PriorityLock(Lock):
    def sacquire(self) -> bool: ...
    async def acquire(self) -> bool: ...
    def get_effective_priority(self) -> float | None: ...

class BaseScheduler:
    def get_task_factory(self) -> TaskFactory: ...
    def set_task_factory(self, factory: TaskFactory | None) -> None: ...
```

Semantics:

- Task factories are tealet scheduler construction strategies, not asyncio task
  factory compatibility hooks.
- A factory receives the target callable and already selected context, creates
  and prepares a `Task`, and returns it unscheduled.
- `BaseScheduler.spawn(..., **kwargs)` forwards extra keyword arguments to the
  configured task factory, mirroring `asyncio.create_task(coro, **kwargs)`. This
  allows custom factories to accept construction-time options such as
  `priority=...` before the task becomes runnable.
- `DefaultTaskFactory` and `StubTaskFactory` accept a `task_constructor`.
  They instantiate it as `task_constructor(scheduler, **kwargs)`, so extra spawn
  keyword arguments are handled by the task constructor. With the default
  `Task`, unsupported keywords are rejected by `Task.__init__`;
  with `PriorityTask`, `priority=...` is accepted directly.
- `PriorityTask` is a scheduler task with a float `priority` property. Lower
  numeric values run first in priority queues, matching Python priority queue
  and Unix `nice` conventions. The standard public bands are
  `TASK_PRIORITY_CRITICAL = -20.0`, `TASK_PRIORITY_HIGH = -10.0`,
  `TASK_PRIORITY_DEFAULT = 0.0`, `TASK_PRIORITY_LOW = 10.0`, and
  `TASK_PRIORITY_IDLE = 20.0`, leaving space for intermediate values.
- Changing `PriorityTask.priority` calls `modified()`, so a runnable queue can
  recompute ordering when the task is already linked.
- `PriorityTask.get_effective_priority()` is the scheduling priority used by
  priority-aware queues. It includes inherited priority from owned
  `PriorityLock` instances.
- `PriorityLock` supports both tealet `sacquire()` and asyncio `acquire()`.
  Regular `Task` instances and asyncio tasks can acquire and release it
  normally. It keeps `Lock`'s FIFO waiter policy, and priority inheritance only
  affects scheduler ordering. When `PriorityTask` instances participate, a lock
  owner inherits the best waiting priority while the lock is held.
- Class factories expose an `eager_start` default. `BaseScheduler.spawn(..., eager_start=...)`
  passes an optional per-spawn override to the factory.
- When eagerness resolves true and the scheduler is already running, the factory
  calls `task.run()` before returning the task, so the task starts immediately
  and may complete before `spawn(...)` returns. Eager startup is deferred when
  the scheduler is not running, matching asyncio's `eager_start` condition.
- `BaseScheduler.spawn(...)` remains responsible for registering the task and
  making it runnable if eager startup did not already complete it.
- Passing `None` to `set_task_factory(...)` restores the default direct-prep
  factory.
- `StubTaskFactory.stub_here()` creates the reusable stub at the caller's current
  tealet stack point. If a stub was not created explicitly, the factory creates
  one lazily on first use.

### Runner Factory Types

```python
from collections.abc import Callable

LoopFactory = Callable[[], asyncio.AbstractEventLoop]
SchedulerFactory = Callable[[], Scheduler]
AsyncSchedulerFactory = Callable[[], AsyncScheduler]
```

### High-Level Runner API

```python
class Runner:
    def __init__(
        self,
        *,
        scheduler_factory: SchedulerFactory | None = None,
        context: contextvars.Context | None = None,
        debug: bool | None = None,
        handle_sigint: bool = True,
    ) -> None: ...

    def run(self, entry, /, *, context: contextvars.Context | None = None): ...
    def close(self) -> None: ...

class AsyncRunner:
    def __init__(
        self,
        *,
        scheduler_factory: AsyncSchedulerFactory | None = None,
        context: contextvars.Context | None = None,
        debug: bool | None = None,
        handle_sigint: bool = True,
    ) -> None: ...

    async def run(self, entry, /, *, context: contextvars.Context | None = None): ...
    async def aclose(self) -> None: ...
    async def __aenter__(self) -> AsyncRunner: ...
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None: ...
```

And convenience functions:

```python
def run(entry, /, *args,
        scheduler_factory: SchedulerFactory | None = None,
        **kwargs): ...

async def run_async(entry, /, *args,
                    scheduler_factory: SchedulerFactory | None = None,
                    **kwargs): ...

def run_in_asyncio(entry, /, *args,
       loop_factory: LoopFactory | None = None,
       scheduler_factory: SchedulerFactory | None = None,
       **kwargs): ...

def run_asyncio_in_tealet(entry, /, *args,
  loop_factory: LoopFactory | None = None,
  scheduler_factory: SchedulerFactory | None = None,
  **kwargs): ...
```

`entry` accepted forms:

- sync callable
- async callable
- awaitable object

Return behavior:

- returns final entry result
- propagates unhandled exceptions

## Execution Model

### tealetio.runner.Runner.run(...)

- Creates a scheduler using `scheduler_factory` (or default scheduler creator).
- Installs scheduler as current for runtime scope.
- Runs entry to completion.
- Ensures deterministic cleanup:
  - pending scheduler waits resolved/cancelled
  - scheduler running marker cleared
  - scheduler binding restored
  - scheduler default executor shut down cleanly before scheduler close

### tealetio.asyncio.AsyncRunner.run(...)

- Requires a currently running asyncio loop.
- Creates a scheduler using `scheduler_factory` (or default async scheduler
  creator).
- Installs scheduler as current for runtime scope.
- Runs entry to completion inside the active asyncio task.
- Uses `aclose()` for deterministic async cleanup, and supports
  `async with AsyncRunner() as runner: ...` as the async counterpart to
  `with Runner() as runner: ...`. `AsyncRunner` deliberately follows Python's
  async-resource convention and does not expose `close()`.

### tealetio.asyncio.run_async(...)

- Requires a currently running asyncio loop.
- Creates or installs scheduler for runtime scope.
- Runs entry to completion inside the active asyncio task.
- Restores prior scheduler binding after completion.
- Does not close the ambient asyncio loop.

### tealetio.asyncio.run_in_asyncio(...)

- Creates a temporary `asyncio.Runner`, using `loop_factory` when provided.
- Creates a temporary `AsyncRunner` inside that asyncio runner.
- Runs the entry to completion through `AsyncRunner.run(...)`.
- Restores prior scheduler binding after completion.
- Lets `asyncio.Runner` handle loop shutdown and closure.

### tealetio.asyncio.run_asyncio_in_tealet(...)

- Creates a temporary `tealetio.runner.Runner` with a
  `tealetio.selector.SelectorScheduler` by default.
- Disables the outer tealet runner's SIGINT handler by default so the inner
  `asyncio.Runner` can install its normal interrupt handler.
- Creates a temporary `asyncio.Runner` whose default loop is
  `tealetio.asyncio.TealetSelectorEventLoop` hosted by the active selector
  scheduler.
- Runs the coroutine/awaitable using the same entry semantics as
  `asyncio.Runner.run(...)` inside the tealet-hosted asyncio loop.
- Restores prior scheduler binding after completion and closes the default
  selector scheduler created by the helper.

## Context and Scope Rules

- Current scheduler binding is thread-local in the current implementation.
- Context-local current scheduler binding for async tasks is future hardening
  work.
- Running scheduler binding is strictly scoped and never lazy-created.
- Main-tealet scheduler context is also scoped. The scheduler/runner driving
  entry points install it automatically, but raw main code that touches
  scheduler-owned tasks directly must install `scheduler.main_context()` itself.
- Nested runtime scopes are allowed and use stack discipline:
  - inner scope overrides current scheduler
  - outer scope restored on exit
- Initializing a new runner while another scheduler is actively running in the
  same context is rejected.

## Error Behavior

- `get_running_scheduler()` with no running scheduler:
  - `RuntimeError("no running scheduler")`
- `tealetio.asyncio.run_async(...)` outside running asyncio loop:
  - `RuntimeError` consistent with asyncio wording/style
- `tealetio.asyncio.run_in_asyncio(...)` on Python versions without `asyncio.Runner`:
  - `RuntimeError` indicating Python 3.11+ is required
- `tealetio.asyncio.run_asyncio_in_tealet(...)` without `asyncio.Runner`:
  - `RuntimeError` indicating Python 3.11+ is required
- `tealetio.asyncio.run_asyncio_in_tealet(...)` with a non-selector scheduler and
  no custom loop factory:
  - `RuntimeError` indicating that a `SelectorScheduler` is required
- invalid factory return values:
  - Factories are duck typed; failures surface naturally when required scheduler
    operations are used.

## Backward Compatibility

- Use `Scheduler` as the primary scheduler class name.
- Use `Scheduler` and `AsyncScheduler` classes directly as runner factories; do
  not add separate `new_sync_scheduler()` / `new_async_scheduler()` helpers
  unless a later API decision requires them.
- Direct `Scheduler` and `AsyncScheduler` usage remains valid through their
  owning modules, `tealetio.scheduler` and `tealetio.asyncio` respectively.

## Asyncio Mapping Table

- `asyncio.new_event_loop()` -> `Scheduler` or `AsyncScheduler` used directly as
  a factory
- `asyncio.get_running_loop()` -> `get_running_scheduler()`
- `asyncio.get_event_loop()` legacy current-loop lookup -> `get_scheduler()`
  only for strict lookup of an explicitly-bound scheduler; it never creates one.
- `asyncio.run(...)` from sync code -> `tealetio.runner.run(...)`
- `asyncio.Runner(...)` from sync code -> `tealetio.runner.Runner(...)`
- async code that needs a scoped tealet scheduler -> `tealetio.asyncio.AsyncRunner(...)`
- `asyncio.run(...)` inside tealet -> `tealetio.asyncio.run_asyncio_in_tealet(...)`

## Migration Notes

- Prefer `tealetio.runner.Runner` for synchronous code that wants reusable runner
  state or explicit lifecycle control.
- Prefer `tealetio.runner.run(...)` for one-shot synchronous entry points.
- Prefer `tealetio.asyncio.AsyncRunner` plus `async with` or
  `await runner.aclose()` for async code that needs explicit scheduler lifetime
  control.
- Prefer `tealetio.asyncio.run_async(...)` for one-shot async entry points inside
  an existing asyncio task.
- Prefer `tealetio.asyncio.run_in_asyncio(...)` when synchronous code should own a
  temporary asyncio runner and an inner tealet async scheduler.
- Prefer `tealetio.asyncio.run_asyncio_in_tealet(...)` when tealet code should host
  a temporary asyncio runner.
- Direct `Scheduler` / `AsyncScheduler` APIs remain appropriate for low-level
  tests, integrations, and custom driving loops.

## Suggested Phase Plan

Phase 1: Accessor Semantics

Status: Implemented with strict current-scheduler and running-scheduler lookup.

- Keep explicit `Scheduler` / `AsyncScheduler` construction.
- Keep `set_scheduler` for runner/manual binding.
- Add `get_scheduler` and `get_running_scheduler`; neither creates schedulers.
- Remove global default scheduler factory and lazy scheduler lookup behavior.

Phase 2: Runner Surface

Status: Implemented with `Runner`/`AsyncRunner` and top-level
`tealetio.runner.run`/`tealetio.asyncio.run_async` helpers. A separate `Runtime`
class is not part of the current public surface.

- Keep `Runner`/`AsyncRunner` as the primary public runtime surface.
- The synchronous runner implementation lives in `tealetio.runner`; the older
  `tealet.runtime` module name is intentionally not preserved.

Phase 3: Context Scoping Hardening

Status: Partially implemented. Runner binding/restoration and focused nested
scope tests are in place; async context-local isolation across asyncio task
boundaries remains future work.

- Ensure async context-local behavior across task boundaries.
- Keep nested runtime scope tests covering sync, async, and running-scheduler
  rejection behavior.

Phase 4: Docs and Migration Notes

Status: In progress.

- Add user-facing examples and migration guidance.
- Clarify when to use direct scheduler APIs vs high-level runtime APIs.

## Immediate Next Steps

1. Continue hardening the low-level IO callback and socket helper surface.
2. Continue auditing `TealetSelectorEventLoop` compatibility boundaries.

## Next Alignment Backlog (Asyncio Interop)

This backlog is not a goal of broad functional parity with asyncio. The near
term goal is low-level IO support: expose the callback hooks and readiness
building blocks needed to build tealet-native streams, transports, and external
IO-manager adapters.

1. Running-State API

Status: Implemented.

- `scheduler.is_running()` is defined as: a run call is currently active (not
  merely "created" or "not closed").
- Runner initialization rejects replacement while the currently bound scheduler
  is running.

2. Loop-Style Run APIs

Status: Implemented for current sync/async scheduler drivers.

- `run_forever()`, `run_until_complete(...)`, `arun_forever()`, and
  `arun_until_complete(...)` are available on the appropriate driving APIs.
- `arun(yield_every=N)`, `arun_forever(yield_every=N)`, and
  `arun_until_complete(..., yield_every=N)` periodically yield to asyncio after
  bounded scheduler batches when runnable scheduler work remains.
- `pump(...)` remains an explicit low-level primitive.
- `stop()` is available to request driver termination.

3. Runner Context Support

Status: Implemented.

- Runner construction accepts an optional `contextvars.Context` and each
  `run(...)` call can override that context.

4. Low-Level Scheduler Surface

- Continue defining and documenting the scheduler's low-level APIs (timers,
  callback enqueue, fd reader/writer callbacks, stepping/pump, stop/run state)
  similarly to asyncio's loop low-level surface.
- Treat `add_reader`, `remove_reader`, `add_writer`, and `remove_writer` as the
  portable low-level IO seam. They are useful for selector loops, and also map
  well to completion-oriented or external IO managers that wake callbacks when
  operations become ready or complete.
- Keep blocking convenience waits such as `wait_readable(...)` and
  `wait_writable(...)` layered over that callback seam, rather than maintaining
  a separate readiness-wait registration path.

5. Asyncio Transport/Protocol Stream Surface

- Consider a tealet-native stream layer built on asyncio transports and
  protocols. This is the faster asyncio socket path for performance-sensitive
  stream IO, compared with the `loop.sock_*` helper methods, which are useful
  but intentionally convenience-oriented.
- The likely first shape is a custom `asyncio.Protocol` implementation created
  by special protocol factories passed to `loop.create_connection(...)` and
  `loop.create_server(...)`.
- The protocol would receive normal asyncio callbacks such as
  `connection_made`, `data_received`, `eof_received`, `pause_writing`,
  `resume_writing`, and `connection_lost`, then translate those callbacks into
  tealet-compatible blocking methods such as `read(...)`, `readexactly(...)`,
  `readline(...)`, `write(...)`, `drain()`, `close()`, and `wait_closed()`.
- Prefer implementing a tealet protocol or protocol-owned stream facade over
  subclassing `asyncio.StreamReaderProtocol` initially. The latter is tied to
  asyncio's stream machinery and may bring private assumptions that do not match
  tealet blocking semantics.
- `asyncio.BufferedProtocol` is a possible later optimization once the plain
  `Protocol` semantics are proven. It may reduce copies for inbound data but
  requires more careful buffer ownership.

6. Tealet-Hosted Asyncio Loop Experiment

Status: Initial Unix selector prototype implemented.

- A tealet-hosted asyncio loop may be feasible if the asyncio loop's raw
  blocking points can be delegated to the outer tealet scheduler.
- `tealetio.asyncio.TealetSelectorEventLoop` is an experimental
  `asyncio.SelectorEventLoop` subclass hosted by `tealetio.selector.SelectorScheduler`.
- The implementation uses a selector adapter whose fd registration is backed by
  `SelectorScheduler.add_reader(...)` and `add_writer(...)`; when asyncio's
  selector would block, the pump tealet parks on a scheduler `Event` and wakes
  from fd readiness, a scheduler timer, or asyncio's self-pipe.
- The critical hook is file-descriptor readiness. Asyncio selector loops use
  reader and writer callbacks as their low-level IO surface, so a
  tealet-aware selector or selector-style `SchedulerLoop` would need correct
  `add_reader`, `remove_reader`, `add_writer`, and `remove_writer` behavior.
- The loop's sleep/block/wakeup behavior must also delegate to the outer
  scheduler. When asyncio would block in its selector, the pump tealet should
  park until the outer scheduler observes fd readiness, a timer deadline, or an
  explicit wakeup.
- Many higher-level asyncio mechanisms can then remain delegated to their
  existing implementations: socket transports/protocols, `loop.sock_*` helpers,
  DNS helpers that use threads, `run_in_executor`, and callback scheduling.
- Areas that need special audit before claiming broad compatibility:
  - subprocess support, especially child watchers, process-exit notifications,
    and pipe transports
  - signal handling, which is main-thread and event-loop specific
  - SSL/TLS transports, because they combine socket readiness, buffering,
    handshake state, and flow control
  - async generator shutdown, default-executor shutdown, and loop-close cleanup
  - exception-handler/debug hooks and task/future factory behavior
  - cross-thread wakeups through `call_soon_threadsafe(...)`
- The most promising Unix-first experiment remains a selector-compatible outer
  scheduler. Proactor loops and uv/libuv loops are separate reactor families and
  should be treated as later designs rather than extensions of the same adapter.

7. KeyboardInterrupt Handling Parity

Status: Implemented for Python 3.11+.

- Define interrupt policy comparable to asyncio runner behavior.
- Route `KeyboardInterrupt` to the active user "main task" created by runner,
  distinct from the process main tealet/main thread.

## Open Design Questions

- Should `run_async` permit reusing an already running scheduler, or always create
  an inner scope scheduler by default?

## Minimal Usage Sketch

```python
# sync entry with explicit factories
result = tealetio.runner.run(
    main,
    scheduler_factory=tealetio.scheduler.Scheduler,
)

# async entry inside existing loop
async def app_main():
  return await tealetio.asyncio.run_async(async_entry)

# asyncio entry inside a tealet selector scheduler
result = tealetio.asyncio.run_asyncio_in_tealet(async_entry())

# low-level direct construction
s = tealetio.scheduler.Scheduler()
tealetio.scheduler.set_scheduler(s)
```

# Scheduler Runtime API Spec (Draft)

Status: Active draft with partial implementation landed.

This document defines a top-level runtime API that harmonizes tealet scheduler
selection with asyncio loop selection, while preserving scheduler terminology in
tealet public APIs.

## Current Implementation Snapshot

The repository now contains a meaningful subset of this design.

Implemented:

- Runner split and driving split are in place:
  - `tealet.runtime.Runner` drives sync scheduler execution.
  - `tealet.asyncio.AsyncRunner` drives async scheduler execution inside an existing asyncio task.
- Shared runner lifecycle has been consolidated in a generic `BaseRunner`.
- Runner factories use per-runner defaults and factory-only construction.
  - `Runner` uses `tealet.scheduler.Scheduler` as its default factory.
  - `AsyncRunner` uses `tealet.asyncio.AsyncScheduler` as its default factory.
  - Custom factories are duck typed; runtime does not validate returned objects.
- Top-level convenience helpers exist:
  - `tealet.runtime.run(...)`
  - `tealet.asyncio.run_async(...)`
  - `tealet.asyncio.run_in_asyncio(...)`
  - `tealet.asyncio.run_asyncio_in_tealet(...)`
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
  `tealet.asyncio.AsyncScheduler` delegates these hooks to the running asyncio loop.
  `tealet.selector.SelectorScheduler` implements them through its native selector reactor.
  Selector readiness waits (`wait_readable(...)` and `wait_writable(...)`) are
  layered on top of one-shot reader/writer callbacks that wake tealet `Event`
  waiters.
- Scheduler driving APIs include both sync and async run entry points:
  - `run_until_complete(...)`
  - `run_forever(...)`
  - `arun_until_complete(...)`
  - `arun_forever(...)`
- Future waiting semantics are aligned so wait paths return final results.
- Cancellation is represented by `asyncio.CancelledError`, matching asyncio
  `Future`/`Task` behavior. A stored `CancelledError` is the cancellation state
  indicator for scheduler futures and tealet tasks.
- Scheduler task introspection includes `BaseScheduler.all_tasks()`, which
  returns unfinished scheduler-owned tealet tasks without keeping those tasks
  alive solely for introspection.
- Scheduler grouping includes `tealet.scheduler.gather(...)`, which returns a
  future for ordered child results and can optionally collect child exceptions
  as result values.
- Cancellation propagates across scheduler boundaries in an asyncio-compatible
  way:
  - cancelling an asyncio waiter on a tealet `Future` schedules cancellation of
    the underlying tealet future/task through the running scheduler
  - cancelling a tealet task waiting on a tealet `Future` schedules cancellation
    of the awaited future/task through the running scheduler
  - cancelling a tealet task waiting in `wait_async(...)` schedules cancellation
    of the awaited asyncio future/task with `loop.call_soon(...)`
  - awaiting an already-cancelled tealet or asyncio future raises the same
    `CancelledError` through the tealet/asyncio boundary
  - shielding prevents waiter cancellation from propagating into the shielded
    underlying future, matching `asyncio.shield(...)`
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

Not implemented yet from this proposal:

- A top-level `Runtime` wrapper class coordinating loop and scheduler factories
  in one object.
- Scheduler access has been narrowed to explicit construction plus
  `get_running_scheduler()`.
- Finalized shutdown policy wording.

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

- Current scheduler: context-associated scheduler that may exist but may not be
  running.
- Running scheduler: scheduler currently executing/pumping work.
- Runtime scope: bounded execution context created by a high-level runner.

## Proposed Public API

### Scheduler Access Functions

```python
def set_scheduler(scheduler: BaseScheduler | None) -> None: ...
def get_running_scheduler() -> BaseScheduler: ...
```

Semantics:

- `set_scheduler(scheduler)`:
  - Installs scheduler as current in the active context.
  - If argument is `None`, clears current scheduler binding.

- `get_running_scheduler()`:
  - Returns the scheduler currently running in this execution context.
  - Raises `RuntimeError` if no running scheduler exists.
  - Never creates or installs a scheduler.

### Scheduler Grouping

```python
def gather(
    *entries: Future[object] | Callable[[], object],
    return_exceptions: bool = False,
) -> Future[list[object]]: ...
```

Semantics:

- Accepts scheduler futures/tasks and zero-argument callables.
- Converts callables into scheduler-owned tealet tasks.
- Returns results in input order.
- With `return_exceptions=False`, the first child exception completes the group
  future with that exception.
- With `return_exceptions=True`, child exceptions are collected into the result
  list alongside successful values.
- Cancelling the group future requests cancellation of unfinished children.

### Runtime Factory Types

```python
from collections.abc import Callable

LoopFactory = Callable[[], asyncio.AbstractEventLoop]
SchedulerFactory = Callable[[], Scheduler]
```

### High-Level Runtime API

```python
class Runtime:
    def __init__(
        self,
        *,
        loop_factory: LoopFactory | None = None,
        scheduler_factory: SchedulerFactory | None = None,
    ) -> None: ...

    def run(self, entry, /, *args, **kwargs): ...
    async def run_async(self, entry, /, *args, **kwargs): ...
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

### Runtime.run(...)

- Creates a loop using `loop_factory` (or default loop creator).
- Creates a scheduler using `scheduler_factory` (or default scheduler creator).
- Installs scheduler as current for runtime scope.
- Runs entry to completion.
- Ensures deterministic cleanup:
  - pending scheduler waits resolved/cancelled
  - scheduler running marker cleared
  - scheduler binding restored
  - loop shut down and closed if created by runtime

### tealet.asyncio.run_async(...)

- Requires a currently running asyncio loop.
- Creates or installs scheduler for runtime scope.
- Runs entry to completion inside the active asyncio task.
- Restores prior scheduler binding after completion.
- Does not close the ambient asyncio loop.

### tealet.asyncio.run_in_asyncio(...)

- Creates a temporary `asyncio.Runner`, using `loop_factory` when provided.
- Creates a temporary `AsyncRunner` inside that asyncio runner.
- Runs the entry to completion through `AsyncRunner.run(...)`.
- Restores prior scheduler binding after completion.
- Lets `asyncio.Runner` handle loop shutdown and closure.

### tealet.asyncio.run_asyncio_in_tealet(...)

- Creates a temporary `tealet.runtime.Runner` with a
  `tealet.selector.SelectorScheduler` by default.
- Disables the outer tealet runner's SIGINT handler by default so the inner
  `asyncio.Runner` can install its normal interrupt handler.
- Creates a temporary `asyncio.Runner` whose default loop is
  `tealet.asyncio.TealetSelectorEventLoop` hosted by the active selector
  scheduler.
- Runs the coroutine/awaitable using the same entry semantics as
  `asyncio.Runner.run(...)` inside the tealet-hosted asyncio loop.
- Restores prior scheduler binding after completion and closes the default
  selector scheduler created by the helper.

## Context and Scope Rules

- Current scheduler binding should be context-local for async tasks.
- Thread-local fallback may be kept for sync compatibility paths.
- Running scheduler binding is strictly scoped and never lazy-created.
- Nested runtime scopes are allowed and use stack discipline:
  - inner scope overrides current scheduler
  - outer scope restored on exit

## Error Behavior

- `get_running_scheduler()` with no running scheduler:
  - `RuntimeError("no running scheduler")`
- `tealet.asyncio.run_async(...)` outside running asyncio loop:
  - `RuntimeError` consistent with asyncio wording/style
- `tealet.asyncio.run_in_asyncio(...)` on Python versions without `asyncio.Runner`:
  - `RuntimeError` indicating Python 3.11+ is required
- `tealet.asyncio.run_asyncio_in_tealet(...)` without `asyncio.Runner`:
  - `RuntimeError` indicating Python 3.11+ is required
- `tealet.asyncio.run_asyncio_in_tealet(...)` with a non-selector scheduler and
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
  owning modules, `tealet.scheduler` and `tealet.asyncio` respectively.

## Asyncio Mapping Table

- `asyncio.new_event_loop()` -> `Scheduler` or `AsyncScheduler` used directly as
  a factory
- `asyncio.get_running_loop()` -> `get_running_scheduler()`
- `asyncio.get_event_loop()` legacy pattern -> no direct equivalent; create a
  scheduler explicitly or use a runner factory.
- `asyncio.run(..., loop_factory=...)` -> `run(..., loop_factory=..., scheduler_factory=...)`
- `asyncio.run(...)` inside tealet -> `tealet.asyncio.run_asyncio_in_tealet(...)`
- `asyncio.Runner(...)` -> `Runtime(...)`

## Suggested Phase Plan

Phase 1: Accessor Semantics

Status: Implemented with strict running-scheduler lookup.

- Keep explicit `Scheduler` / `AsyncScheduler` construction.
- Keep `set_scheduler` for runner/manual binding.
- Add `get_running_scheduler` and ensure it never creates schedulers.
- Remove global default scheduler factory and lazy `get_scheduler` behavior.

Phase 2: Runtime Wrapper

Status: Partially covered by existing `Runner`/`AsyncRunner` and top-level
`tealet.runtime.run`/`tealet.asyncio.run_async`; `Runtime` class itself not added.

- Add `Runtime` plus top-level `run` and `run_async`.
- Add lifecycle and cleanup tests.

Phase 3: Context Scoping Hardening

Status: Partially complete.

- Ensure async context-local behavior across task boundaries.
- Add nested runtime scope tests.

Phase 4: Docs and Migration Notes

Status: In progress.

- Add user-facing examples and migration guidance.
- Clarify when to use direct scheduler APIs vs high-level runtime APIs.

## Immediate Next Steps

1. Decide whether to keep introducing a `Runtime` class, or adopt
  `Runner`/`AsyncRunner` as the primary public runtime surface.
2. Add explicit nested scope tests for mixed sync/async runner composition.
3. Document final shutdown guarantees for runner exit paths.
4. Add a short migration section mapping old helper usage to current runner
  and top-level helper APIs.

## Next Alignment Backlog (Asyncio Interop)

This backlog is not a goal of broad functional parity with asyncio. The near
term goal is low-level IO support: expose the callback hooks and readiness
building blocks needed to build tealet-native streams, transports, and external
IO-manager adapters.

1. Running-State API

- Add `scheduler.is_running()` and define it strictly as: a run call is
  currently active (not merely "created" or "not closed").
- Guard scheduler replacement APIs so they fail when the currently bound
  scheduler is running.

2. Loop-Style Run APIs

- Add `run_forever()` and `run_until_complete(...)` equivalents on scheduler.
- Keep `pump(...)` as an explicit low-level primitive.
- Evaluate adding a `stop()` primitive to mirror event-loop lifecycle controls.

3. Runner Context Support

- Add explicit runner context support (`contextvars.Context`-aware behavior)
  and use that context when creating/starting the main task/tealet.

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
- `tealet.asyncio.TealetSelectorEventLoop` is an experimental
  `asyncio.SelectorEventLoop` subclass hosted by `tealet.selector.SelectorScheduler`.
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
- What is the exact cancellation policy for scheduler-blocked tasks during runtime
  shutdown?
- Do we expose a context manager form (`with Runtime(...):`) in phase 1 or later?

## Minimal Usage Sketch

```python
# sync entry with explicit factories
result = tealet.runtime.run(
    main,
    scheduler_factory=tealet.scheduler.Scheduler,
)

# async entry inside existing loop
async def app_main():
  return await tealet.asyncio.run_async(async_entry)

# asyncio entry inside a tealet selector scheduler
result = tealet.asyncio.run_asyncio_in_tealet(async_entry())

# low-level direct construction
s = tealet.scheduler.Scheduler()
tealet.scheduler.set_scheduler(s)
```

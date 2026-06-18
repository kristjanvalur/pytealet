# Scheduler Runtime API Spec (Draft)

Status: Active draft with partial implementation landed.

This document defines a top-level runtime API that harmonizes tealet scheduler
selection with asyncio loop selection, while preserving scheduler terminology in
tealet public APIs.

## Current Implementation Snapshot

The repository now contains a meaningful subset of this design.

Implemented:

- Runner split and driving split are in place:
  - `Runner` drives sync scheduler execution.
  - `AsyncRunner` drives async scheduler execution inside an existing asyncio task.
- Shared runner lifecycle has been consolidated in a generic `BaseRunner`.
- Runner factories use per-runner defaults and factory-only construction.
  - `Runner` uses `Scheduler` as its default factory.
  - `AsyncRunner` uses `AsyncScheduler` as its default factory.
  - Custom factories are duck typed; runtime does not validate returned objects.
- Top-level convenience helpers exist:
  - `run(...)`
  - `run_async(...)`
  - `run_in_asyncio(...)`
- `BaseScheduler` contains shared cooperative scheduling mechanics.
- `Scheduler` is the concrete synchronous scheduler implementation.
- `AsyncScheduler` is the concrete asyncio-hosted scheduler implementation.
- `Scheduler` and `AsyncScheduler` can be used directly as factories. They share
  the common scheduler/task/timer APIs from `BaseScheduler`, while implementing
  different driving APIs.
- Scheduler driving APIs include both sync and async run entry points:
  - `run_until_complete(...)`
  - `run_forever(...)`
  - `arun_until_complete(...)`
  - `arun_forever(...)`
- Future waiting semantics are aligned so wait paths return final results.
- Cancellation is represented by `asyncio.CancelledError`, matching asyncio
  `Future`/`Task` behavior. A stored `CancelledError` is the cancellation state
  indicator for scheduler futures and tealet tasks.
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

### Runtime.run_async(...)

- Requires a currently running asyncio loop.
- Creates or installs scheduler for runtime scope.
- Runs entry to completion inside the active asyncio task.
- Restores prior scheduler binding after completion.
- Does not close the ambient asyncio loop.

### run_in_asyncio(...)

- Creates a temporary `asyncio.Runner`, using `loop_factory` when provided.
- Creates a temporary `AsyncRunner` inside that asyncio runner.
- Runs the entry to completion through `AsyncRunner.run(...)`.
- Restores prior scheduler binding after completion.
- Lets `asyncio.Runner` handle loop shutdown and closure.

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
- `run_async(...)` outside running asyncio loop:
  - `RuntimeError` consistent with asyncio wording/style
- `run_in_asyncio(...)` on Python versions without `asyncio.Runner`:
  - `RuntimeError` indicating Python 3.11+ is required
- invalid factory return values:
  - Factories are duck typed; failures surface naturally when required scheduler
    operations are used.

## Backward Compatibility

- Use `Scheduler` as the primary scheduler class name.
- Use `Scheduler` and `AsyncScheduler` classes directly as runner factories; do
  not add separate `new_sync_scheduler()` / `new_async_scheduler()` helpers
  unless a later API decision requires them.
- Existing direct `Scheduler` and `AsyncScheduler` usage remains valid.

## Asyncio Mapping Table

- `asyncio.new_event_loop()` -> `Scheduler` or `AsyncScheduler` used directly as
  a factory
- `asyncio.get_running_loop()` -> `get_running_scheduler()`
- `asyncio.get_event_loop()` legacy pattern -> no direct equivalent; create a
  scheduler explicitly or use a runner factory.
- `asyncio.run(..., loop_factory=...)` -> `run(..., loop_factory=..., scheduler_factory=...)`
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
`run`/`run_async`; `Runtime` class itself not added.

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

## Next Alignment Backlog (Asyncio Parity)

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

- Define and document the scheduler's low-level APIs (timers, callback enqueue,
  stepping/pump, stop/run state) similarly to asyncio's loop low-level surface.

5. KeyboardInterrupt Handling Parity

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
result = tealet.scheduler.run(
    main,
    loop_factory=asyncio.new_event_loop,
    scheduler_factory=tealet.scheduler.Scheduler,
)

# async entry inside existing loop
async def app_main():
    return await tealet.scheduler.run_async(async_entry)

# low-level direct construction
s = tealet.scheduler.Scheduler()
tealet.scheduler.set_scheduler(s)
```

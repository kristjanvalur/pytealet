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
- Top-level convenience helpers exist:
  - `run(...)`
  - `run_async(...)`
- Scheduler driving APIs include both sync and async run entry points:
  - `run_until_complete(...)`
  - `run_forever(...)`
  - `arun_until_complete(...)`
  - `arun_forever(...)`
- Future waiting semantics are aligned so wait paths return final results.

Not implemented yet from this proposal:

- A top-level `Runtime` wrapper class coordinating loop and scheduler factories
  in one object.
- Public accessor quartet exactly as specified (`new_scheduler`,
  `get_running_scheduler`, and finalized alias behavior for `get_scheduler`).
- Finalized shutdown/cancellation policy wording and KeyboardInterrupt policy
  parity notes.

## Goals

- Provide a simple top-level API to configure both:
  - asyncio loop creation
  - tealet scheduler creation
- Keep tealet naming explicit: scheduler, not loop.
- Offer high-level run-until-done entry points for sync and async use cases.
- Mirror modern asyncio direction (explicit factories, not global policy).
- Keep backward compatibility for existing scheduler helper usage where possible.

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
def new_scheduler() -> SimpleScheduler: ...
def set_scheduler(scheduler: SimpleScheduler | None) -> None: ...
def get_scheduler() -> SimpleScheduler: ...
def get_running_scheduler() -> SimpleScheduler: ...
```

Semantics:

- `new_scheduler()`:
  - Returns a new scheduler instance.
  - Does not install it as current.

- `set_scheduler(scheduler)`:
  - Installs scheduler as current in the active context.
  - If argument is `None`, clears current scheduler binding.

- `get_scheduler()`:
  - Returns current scheduler if bound.
  - If none is bound, creates one with the active scheduler factory and binds it.

- `get_running_scheduler()`:
  - Returns the scheduler currently running in this execution context.
  - Raises `RuntimeError` if no running scheduler exists.

### Runtime Factory Types

```python
from collections.abc import Callable

LoopFactory = Callable[[], asyncio.AbstractEventLoop]
SchedulerFactory = Callable[[], SimpleScheduler]
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
        loop_factory: LoopFactory | None = None,
        scheduler_factory: SchedulerFactory | None = None,
        **kwargs): ...

async def run_async(entry, /, *args,
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
- invalid factory return values:
  - `TypeError` with clear expected type message

## Backward Compatibility

- Keep existing `scheduler()` helper as compatibility alias for `get_scheduler()`.
- Keep existing scheduler class names and primitives unchanged in this phase.
- Existing direct `SimpleScheduler` usage remains valid.

## Asyncio Mapping Table

- `asyncio.new_event_loop()` -> `new_scheduler()`
- `asyncio.get_running_loop()` -> `get_running_scheduler()`
- `asyncio.get_event_loop()` legacy pattern -> `get_scheduler()`
- `asyncio.run(..., loop_factory=...)` -> `run(..., loop_factory=..., scheduler_factory=...)`
- `asyncio.Runner(...)` -> `Runtime(...)`

## Suggested Phase Plan

Phase 1: Accessor Semantics

Status: In progress.

- Add `new_scheduler`, `set_scheduler`, `get_scheduler`, `get_running_scheduler`.
- Preserve `scheduler()` as alias.
- Add tests for running vs get-or-create behavior.

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
2. Finalize accessor naming and behavior (`get_scheduler` vs
  `get_running_scheduler`) and codify strict error semantics.
3. Add explicit nested scope tests for mixed sync/async runner composition.
4. Document final cancellation and shutdown guarantees for runner exit paths.
5. Add a short migration section mapping old helper usage to current runner
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

- Define interrupt policy comparable to asyncio runner behavior.
- Route `KeyboardInterrupt` to the active user "main task" created by runner,
  distinct from the process main tealet/main thread.

## Open Design Questions

- Should default scheduler factory be overridable globally for tests, or only per
  runtime call?
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
    scheduler_factory=tealet.scheduler.new_scheduler,
)

# async entry inside existing loop
async def app_main():
    return await tealet.scheduler.run_async(async_entry)

# low-level direct access
s = tealet.scheduler.get_scheduler()
```

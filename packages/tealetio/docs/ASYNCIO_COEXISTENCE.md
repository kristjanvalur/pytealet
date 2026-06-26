# Tealet and Asyncio Coexistence

This note explores how a tealet-style stack-switching scheduler could coexist
with Python's native `asyncio` coroutine ecosystem.

The short version: `asyncio` should usually own the IO reactor, while a tealet
scheduler owns stackful user-code scheduling. Tealet tasks can then keep the
main ergonomic benefit of stack switching, namely synchronous-looking code that
can suspend cooperatively, while still using modern asyncio-driven IO libraries.

This is design reasoning around the current scheduler/asyncio bridge and a few
possible future directions.

## Current Scheduler Model

The richer scheduler layer now lives in `tealetio`:

- `tealetio.scheduler.Scheduler` owns a runnable queue of tealets.
- `Event.swait()` blocks the current tealet by recording it as a waiter and
  switching to another runnable tealet.
- `Event.set()` marks the event set and moves blocked tealets back to the
  runnable queue.
- `Future.result()` waits synchronously from the point of view of the tealet
  task, using `Event` as its wakeup primitive.

The base `tealet.simple_scheduler.SimpleScheduler` example is deliberately
smaller and does not include futures, IO, or asyncio interoperability.

That model is stackful and scheduler-local. It is not the same suspension
protocol used by native `async def` coroutines.

## Awaitable Tealet Events

Tealet events can be made usable from asyncio, but the meaning is deliberately
scoped.

A direct spelling such as this is attractive for futures, and is the current
future/task API:

```python
await future
```

In asyncio, the spelling is `await event.wait()`: `wait()` is already an
async method. Tealet follows that spelling for asyncio compatibility. The
tealet-blocking operation uses the short synchronous prefix:

```python
event.swait()
```

This keeps an important boundary visible. An asyncio coroutine suspends by
yielding control to the asyncio event loop through `await event.wait()`. A
tealet task suspends through `event.swait()`, which assumes there is a current
tealet task and that it is legal to stack-switch to another tealet.

The asyncio-facing API is therefore:

```python
await event.wait()
result = await future
```

Internally, an event keeps two classes of waiters:

```python
class Event:
    _tealet_waiters: list[tealet.tealet]
    _asyncio_waiters: list[asyncio.Future[None]]
```

Then `Event.set()` would perform both wakeups:

1. Move tealet waiters back to the tealet scheduler runnable queue.
2. Complete asyncio waiters using the owning event loop.
3. Clear the waiter lists.

The asyncio-side future should be completed through `loop.call_soon(...)` when
already on the loop thread, or `loop.call_soon_threadsafe(...)` when cross-thread
completion is possible. Since tealets are thread-owned, the first design can
probably stay same-thread and add cross-thread behavior only when needed.

`Future` uses the same bridge internally:

```python
def result(self) -> T:
    ...  # blocks a tealet task

def __await__(self):
    ...  # awaits from an asyncio task
```

## Tealet Tasks Waiting on Asyncio

A tealet task cannot literally use `await` unless it is an `async def` native
coroutine. That is a Python syntax rule, not a tealet limitation.

The useful bridge is allowing synchronous-looking tealet code to wait for an
asyncio awaitable:

```python
def worker() -> bytes:
    response = get_running_scheduler().await_(fetch_bytes(url))
    return parse_response(response)
```

`await_()` now does this:

1. Require or capture an owning asyncio event loop.
2. For existing asyncio `Future` and `Task` objects, keep their captured context
   and wait for them directly.
3. For coroutine objects, use optional `asynkit.CoroStart` when available.
4. If `CoroStart` completes the coroutine without awaiting, return the result
   right away in the current tealet.
5. If the coroutine blocks, delegate the continuation to the asyncio loop as an
   asyncio `Task` and wait for that task.
6. For other awaitables, wrap them in an asyncio task where possible.
7. Attach completion/cancellation callbacks, block the current tealet, and make
   it runnable again when the asyncio future completes.
8. When resumed, return the result or raise the exception into the tealet task.

That keeps the stackful value proposition: code inside a tealet task can call
ordinary functions that eventually wait on IO without coloring every caller as
`async def`.

## CoroStart and Delegated Blocking

The important fast path is already implemented through `asynkit.CoroStart`.
When a coroutine finishes before it needs to await anything, `await_()` returns
its value immediately. No asyncio task is created for that purely synchronous
work, and no extra tealet scheduling hop is needed.

When the coroutine does need to await, `CoroStart` exposes that boundary. At
that point `await_()` delegates the continuation to the asyncio loop as a real
asyncio `Task`, then parks the current tealet until that task completes. In
effect, non-awaiting work runs right away in the tealet task, while blocking
work is handed back to asyncio's scheduler and IO machinery.

Like Python's `await` expression, `await_()` does not expose a separate
`context=` parameter. Coroutine objects and awaitables that `await_()` wraps in
a new asyncio task run in a copy of the current `contextvars.Context`; existing
asyncio `Future` and `Task` objects keep the context they already captured.

No `async def` is required in the tealet task, but the called functions can still
be native async functions. Coroutines that complete before their first real IO
wait return with very little scheduling overhead. Coroutines that block become
ordinary tealet blocking points backed by asyncio tasks.

There are still important caveats.

First, many asyncio APIs assume a running event loop. For example, an awaitable
may call `asyncio.get_running_loop()` before it ever yields. If the tealet task is
not currently executing inside an asyncio loop context, those awaitables will
fail. This is easier in the asyncio-hosted model, where tealet work is pumped
from the loop thread while a loop exists.

Second, some asyncio APIs assume a current asyncio task. `asyncio.current_task()`,
timeouts, task groups, and cancellation machinery can depend on real Task state.
Delegating blocked continuations to a real asyncio task gives those APIs the task
identity they expect once the coroutine has crossed an await boundary.

Third, cancellation and closing must mirror coroutine protocol semantics. If the
tealet task waiting in `await_()` is cancelled, the bridge must decide whether to
cancel the underlying asyncio future/task or merely stop waiting for it. Current
behaviour follows the scheduler cancellation rules documented in the runtime API
spec.

## Future: Deeper Await Token Interpretation

`CoroStart` gives tealetio the first step: run until completion or the first
await boundary. A possible future tealet-side await runner could go further by
interpreting yielded scheduler tokens directly, parking the current tealet, and
later resuming the same await iterator instead of immediately delegating the rest
of the coroutine to an asyncio task.

The hard part is not driving `__await__`; a normal Python function can obtain an
await iterator and drive it with `send()`, `throw()`, and `close()`. The hard part
is defining which yielded scheduler tokens tealetio understands, and when it
falls back to asyncio's `Task` machinery.

### Research Spike: Pump Futures Before Task Delegation

The next spike should investigate whether tealetio can keep driving coroutine
await iterators when they yield asyncio futures, instead of handing the remaining
coroutine straight to an asyncio `Task` after the first blocking await.

The candidate algorithm is:

1. Start a coroutine with `asynkit.CoroStart` as today.
2. If it completes synchronously, return the value right away.
3. If it yields an asyncio `Future` or `Task`, register a done callback on that
   future and park the current tealet.
4. When the future completes, resume the same await iterator from the tealet
   scheduler, usually by sending `None` so the future's own await iterator can
   collect its result or raise its stored exception.
5. Repeat until the coroutine returns, yields an unsupported scheduler token, or
   observes asyncio machinery that requires real `Task` identity.
6. On unsupported cases, delegate the remaining coroutine to an asyncio `Task`
   as the compatibility fallback.

The spike should answer these questions:

- Do common asyncio futures and tasks expose enough stable protocol behaviour
  for tealetio to hook callbacks and resume safely?
- Which yielded values can tealetio treat as future-like without relying on
  private asyncio internals?
- How should cancellation propagate when the tealet task is parked on a yielded
  asyncio future but still owns the outer coroutine iterator?
- Which APIs force fallback to a real asyncio task, especially
  `asyncio.current_task()`, timeouts, task groups, and shielded cancellation?
- Does this reduce task allocation and context switching enough to justify the
  extra protocol surface?

The success criterion is not full asyncio task replacement. It is a narrow,
well-tested path where ordinary future waits can remain tealet-owned, with a
clear and conservative fallback to asyncio `Task` delegation whenever task
semantics matter.

## Where the Asyncio Loop Lives

Per-task asyncio event loops are probably the wrong model. Asyncio loops are
large ownership objects with assumptions about thread affinity, callbacks,
timers, cancellation, transports, and task lifecycle. Nesting one loop per
tealet task would create reentrancy and cancellation problems.

A better rule is one asyncio loop per thread, shared by the tealet scheduler in
that thread.

There are three broad scheduler arrangements.

## Option 1: Asyncio-Hosted Tealet Scheduler

This is the recommended first architecture.

```python
async def main() -> None:
    sched = TealetScheduler(asyncio.get_running_loop())
    sched.spawn(worker)
    await sched.run_async()
```

Asyncio owns IO readiness, timers, subprocess support, sockets, transports,
signals where supported, and integration with existing libraries. The tealet
scheduler owns stackful runnable tasks.

When tealet has runnable work, it asks asyncio to pump it soon:

```python
loop.call_soon(sched.run_ready_batch)
```

`run_ready_batch()` should run a bounded amount of tealet work, then return to
the asyncio loop. The bound matters because a busy tealet runnable queue must not
starve asyncio callbacks or IO polling.

When a tealet task blocks on an asyncio future, the future's done callback marks
the tealet runnable and schedules another tealet pump.

This can be summarized as: asyncio is the reactor, tealet is a guest scheduler
for stackful tasks.

## Option 2: Tealet-Hosted Asyncio Pump

A tealet scheduler could instead be the top-level scheduler, with one tealet
task dedicated to running asyncio in small bursts.

The shape would be something like this:

```python
def asyncio_pump(loop: asyncio.AbstractEventLoop) -> None:
    while not shutting_down:
        run_one_asyncio_iteration(loop)
    get_running_scheduler().yield_()


def main() -> None:
    sched = TealetScheduler()
    loop = asyncio.new_event_loop()
    sched.spawn(asyncio_pump, loop)
    sched.spawn(stackful_worker)
    sched.run()
```

Then tealet tasks could still block on asyncio awaitables:

```python
def stackful_worker() -> None:
    data = get_running_scheduler().await_(fetch_bytes(url))
    process(data)
```

The difference is that `await_()` would rely on the asyncio-pump tealet to
drive the event loop until the awaitable completes. The application's outermost
control flow would remain tealet-first and could avoid an `asyncio.run(...)`
entry point.

This is feasible in controlled environments, but it has sharper edges than the
asyncio-hosted design.

The first hard part is stepping asyncio. Asyncio has no stable public API named
"run exactly one loop iteration and then return". There are a few imperfect
approaches:

- Use private loop internals such as `_run_once()`. This gives the desired shape
  but depends on CPython implementation details and may not work with alternate
  event loops.
- Use a public nonblocking trick such as arranging `loop.stop()` and calling
  `loop.run_forever()` for a single short burst. This can process ready callbacks
  without relying directly on `_run_once()`, but it is awkward and still not a
  first-class embedding API.
- Call `loop.run_forever()` and arrange for a timer or callback to stop it
  periodically. This lets asyncio block in its selector, but the entire tealet
  scheduler is paused until asyncio returns control.

The second hard part is deciding when asyncio is allowed to block. If the
tealet scheduler has runnable work, the asyncio pump should not block in the
selector. If there is no tealet work, blocking until the next asyncio timer or IO
event is desirable. Asyncio owns that timeout calculation internally, and there
is no clean public way for an outer tealet scheduler to ask for it.

The third hard part is reentrancy. While the asyncio pump tealet is inside
`loop.run_forever()`, callbacks and native coroutine steps run on that tealet's
stack. If one of those callbacks stack-switches away, the asyncio loop is
suspended mid-callback. Tealet can preserve that stack, but the integration must
ensure that no other tealet tries to re-enter the same loop while it is already
considered running.

The strongest version of this design is not a periodically ticked loop. It is a
tealet-aware selector used by an asyncio selector event loop:

```python
selector = TealetSelector(get_running_scheduler())
loop = asyncio.SelectorEventLoop(selector)
```

In that model, asyncio still believes it owns a normal selector-backed event
loop. When it calls `selector.select(timeout)`, the selector registers the
requested file descriptors and timeout with the tealet scheduler, then parks the
asyncio-pump tealet. When the tealet scheduler observes IO readiness or a timer
expiry, it resumes the asyncio-pump tealet and returns readiness events from
`select()`.

That design turns asyncio's blocking point into a tealet scheduling point. It is
much more promising than busy-ticking because it lets the tealet scheduler remain
the top-level runtime without forcing asyncio to spin. But it also means the
tealet scheduler must become a real reactor with file-descriptor readiness,
timers, wakeups, and selector-compatible unregister semantics.

It also needs a careful audit of asyncio's running-loop state. If the pump
tealet stack-switches while Python still considers the event loop to be running
for the OS thread, other tealets may observe surprising `asyncio.get_running_loop()`
behavior or hit asyncio's reentrancy checks. That may be acceptable if the
runtime documents it, but it cannot be left accidental.

This approach may still be useful when the application wants tealet to be the
primary concurrency model and only needs asyncio as an IO compatibility layer.
It is a reasonable experiment target, especially for a same-thread prototype,
but it should be treated as more scheduler-engineering work than the
asyncio-hosted model.

### Unix-First Selector Scheduler Experiment

The current synchronous `Scheduler` has no IO reactor. Its blocking point is a
timer-oriented wait: when no tealet is runnable, it sleeps until the next
scheduler timer or an explicit scheduler wakeup. A Unix-first IO experiment can
preserve this scheduler as the no-IO baseline and introduce a selector-backed
subclass with the same runnable/task semantics plus file-descriptor readiness.

The class shape could be:

```python
class BaseScheduler:
    # runnable queue, timers, callbacks, task/future mechanics
    ...


class Scheduler(BaseScheduler):
    # current no-IO scheduler; waits on a thread event plus timers
    ...


class SelectorScheduler(BaseScheduler):
    # Unix-first scheduler; waits on selectors plus timers plus wakeups
    ...
```

In the selector subclass, `sleep(delay)` should stop being only a thread-event
timeout. It should become one case of the central reactor wait: register a
timer, park the current tealet, and let the scheduler wait in
`selector.select(timeout)` when there is no runnable tealet work. When the timer
expires, the sleeping tealet is made runnable. The same wait machinery can be
used for IO readiness.

The scheduler-owned wait state might look like:

```python
class SelectorScheduler(BaseScheduler):
    _selector: selectors.BaseSelector
    _read_waiters: dict[int, Task]
    _write_waiters: dict[int, Task]

    def wait_readable(self, fileobj) -> None: ...
    def wait_writable(self, fileobj) -> None: ...
```

The driver loop would run ready tealets first. Only when no tealet is runnable
would it compute the next timer deadline and block in `selector.select(timeout)`.
Any ready fd events would move the associated tealets back to the runnable
queue, and then normal tealet pumping would continue.

This gives ordinary sync-looking IO helpers a natural shape:

```python
def read_some(sock: socket.socket, max_bytes: int = 65536) -> bytes:
    sock.setblocking(False)
    while True:
        try:
            return sock.recv(max_bytes)
        except BlockingIOError:
            get_running_scheduler().wait_readable(sock)


def write_all(sock: socket.socket, data: bytes) -> None:
    sock.setblocking(False)
    view = memoryview(data)
    while view:
        try:
            sent = sock.send(view)
            view = view[sent:]
        except BlockingIOError:
            get_running_scheduler().wait_writable(sock)
```

File reads are different from socket reads. Regular disk files are usually
always reported as ready by POSIX selectors, and a blocking disk read can still
block the whole OS thread. For a first IO layer, the selector scheduler should
focus on sockets, pipes, and other selectable nonblocking descriptors. Regular
file IO should either remain explicitly blocking, use a worker thread, or be
handled later by a platform-specific async-file layer.

The selector scheduler also gives a concrete way to host asyncio as a guest. A
single shared selector object, or a selector adapter owned by the tealet
scheduler, can make both tealet IO handles and asyncio's selector-loop handles
participate in the same blocking wait. If a tealet-owned fd becomes readable,
the host loop wakes even if asyncio was otherwise waiting for its own timeout.
If asyncio registers a wakeup fd for `call_soon_threadsafe()`, that fd must also
be part of the same wait set so external asyncio callbacks wake the tealet host.

This leads to three coexistence modes worth keeping distinct:

- Selector loop: most promising Unix prototype. Asyncio's selector wait can be
  backed by a tealet-aware selector or coordinated with the scheduler's selector
  wait. Timers, sockets, pipes, and wakeup fds can share one blocking point.
- Proactor loop: a different integration problem. Windows proactor loops are
  completion-port based rather than selector-timeout based, so the Unix selector
  design does not transfer directly. A proactor bridge would need to park the
  asyncio-pump tealet until IOCP completions or scheduled callbacks arrive.
- uv loop: future research target. `uvloop`/libuv already owns a portable IO
  reactor. A tealet integration could either host tealet work as callbacks on
  the uv loop, or build a scheduler driver around libuv handles. That is likely
  cleaner than reproducing proactor details, but it makes uv/libuv an optional
  dependency and a separate event-loop family.

The concrete first experiment is now narrow and Unix-first:
`tealetio.selector.SelectorScheduler` provides selector-backed readiness callbacks
and socket helpers, and `tealetio.asyncio.TealetSelectorEventLoop` provides an
experimental tealet-aware selector adapter for `asyncio.SelectorEventLoop`.
Asyncio timers, self-pipe wakeups, and socket readiness can share the host
scheduler's blocking point. `tealetio.asyncio.run_asyncio_in_tealet(...)` wraps
that setup in a temporary selector scheduler and lets the inner `asyncio.Runner`
own SIGINT handling.

## Feasibility Comparison

| Topic | Asyncio-hosted tealet scheduler | Tealet-hosted asyncio pump |
| --- | --- | --- |
| Top-level owner | `asyncio.run()` or an existing asyncio loop owns the thread. | The tealet scheduler owns the thread and runs asyncio from a dedicated tealet task. |
| Public API fit | Strong. Uses `call_soon`, `create_task`, futures, callbacks, and normal loop ownership. | Mixed. Python lacks a public one-iteration event-loop stepping API. Robust implementations may need private APIs or timer-based stop callbacks. |
| IO ecosystem compatibility | Best. Asyncio libraries see the normal running loop model. | Adequate only if the pump is disciplined. Some libraries may assume the loop is continuously owned by asyncio. |
| Blocking behavior | Natural. Asyncio blocks in the selector when nothing else is ready, and wakes tealet work through callbacks. | Difficult. Blocking in asyncio pauses all tealet tasks unless the scheduler knows it is safe to let the pump block. |
| Fairness | Tealet work can be bounded per `run_ready_batch()` callback so asyncio callbacks are not starved. | Requires careful pump policy. Nonblocking loop bursts can spin; blocking loop bursts can starve runnable tealets. A tealet-aware selector improves this but requires a reactor-grade scheduler. |
| Reentrancy risk | Lower. Tealet scheduler is entered from asyncio callbacks and returns to the loop regularly. | Higher. Asyncio may be suspended on a saved tealet stack while the loop still considers itself running. |
| Cancellation model | Closer to native asyncio expectations. Asyncio task cancellation is the outer policy. | More custom policy is needed to translate between tealet cancellation and asyncio task/future cancellation. |
| Context propagation | Follows normal asyncio task/callback context behavior, with explicit tealet context capture at spawn boundaries. | Must define context behavior for callbacks running inside the pump tealet and for stack switches out of those callbacks. |
| Portability | Good across event-loop implementations that honor the public asyncio API. | Weaker if it depends on `_run_once()` or assumptions about CPython's loop internals. |
| Best use case | Applications already using asyncio, or libraries that need modern asyncio IO while adding stackful tasks. | Applications that want tealet as the main concurrency runtime and only need selected asyncio facilities. |

The practical conclusion is that tealet-hosted asyncio is possible, but less
generically robust. It is most plausible as an opt-in runtime mode with clear
constraints:

- same-thread event loop ownership;
- one dedicated asyncio-pump tealet;
- preferably a tealet-aware selector rather than a busy tick loop;
- no nested attempts to run the same asyncio loop;
- explicit fairness policy between asyncio bursts and tealet runnable batches;
- explicit shutdown and cancellation translation.

Asyncio-hosted tealet remains the better default because it follows asyncio's
public ownership model and lets tealet focus on stackful scheduling.

Tealet-hosted asyncio remains worth exploring because it answers a different
product question: "Can a mostly stackful tealet application use asyncio IO
libraries without becoming an asyncio application?" The answer is probably yes,
but the implementation needs stricter runtime rules than the inverse embedding.

## Option 3: Combined Scheduler

A combined scheduler could expose one policy object for:

- runnable tealets
- timers
- IO waiters
- asyncio futures
- cancellation
- task groups or nurseries
- shutdown

Even then, it should probably delegate actual IO readiness and asyncio library
## CoroStart and Delegated Blocking

The important fast path is already implemented through `asynkit.CoroStart`.
When a coroutine finishes before it needs to await anything, `await_()` returns
its value immediately. No asyncio task is created for that purely synchronous
work, and no extra tealet scheduling hop is needed.
plus a tealet runnable queue, not as a full replacement reactor.
When the coroutine does need to await, `CoroStart` exposes that boundary. At
that point `await_()` delegates the continuation to the asyncio loop as a real
asyncio `Task`, then parks the current tealet until that task completes. In
effect, non-awaiting work runs right away in the tealet task, while blocking
work is handed back to asyncio's scheduler and IO machinery.

The resulting programming model is still the same:

## Cancellation and Errors

    data = get_running_scheduler().await_(fetch_bytes(url))

When an asyncio task awaits a tealet future and is cancelled, there are at least
two possible meanings:
No `async def` is required in the tealet task, but the called functions can still
be native async functions. Coroutines that complete before their first real IO
wait return with very little scheduling overhead. Coroutines that block become
ordinary tealet blocking points.

Those are different policies and should be visible in the API. A default that
only detaches the waiter is safer. A stronger `cancel_task=True` or explicit
This is the same family of technique explored by `py-asynkit`. `CoroStart`
already gives tealetio the first step: run until completion or the first await
boundary. A future tealet-side await runner could go further by interpreting
yielded scheduler tokens directly, parking the current tealet, and later
resuming the same await iterator instead of immediately delegating the rest of
the coroutine to an asyncio task.
asyncio future and the tealet task is cancelled, the bridge must decide whether
A normal Python function can obtain an await iterator and drive it with
`send()`, `throw()`, and `close()`:
to cancel the underlying asyncio future or merely stop waiting for it.

Error propagation should follow each side's normal conventions:

- Asyncio waiters should receive results, exceptions, and cancellation through
  asyncio futures.
- Tealet waiters should receive results by returning from synchronous wait calls
  and exceptions by raising in the resumed tealet task.

## Context and Threading

Tealet already has explicit thread ownership and a `contextvars.Context` property
on tealets. Asyncio also relies heavily on context variables, especially around
task creation and callback execution.

The bridge should define context propagation deliberately:

- When spawning a tealet task from asyncio, capture the current `contextvars`
  context unless the caller provides one.
- When completing an asyncio future from tealet, use the loop's normal callback
  context behavior where possible.
- Avoid cross-thread wakeups in the first design unless the API explicitly owns
  thread-safe completion.

Same-thread integration is much easier to reason about and matches tealet's
lineage model.

## Suggested API Sketch

The first useful layer could look like this:

```python
class TealetScheduler:
    def __init__(self, loop: asyncio.AbstractEventLoop | None = None): ...

    def spawn(self, func, *args, **kwargs) -> TealetFuture: ...
    def run_ready_batch(self, limit: int | None = None) -> None: ...
    async def run_async(self) -> None: ...

    def yield_(self) -> None: ...
    def await_(self, awaitable) -> object: ...


class Event:
This is the same family of technique explored by `py-asynkit`. `CoroStart`
already gives tealetio the first step: run until completion or the first await
boundary. A future tealet-side await runner could go further by interpreting
yielded scheduler tokens directly, parking the current tealet, and later
resuming the same await iterator instead of immediately delegating the rest of
the coroutine to an asyncio task.
The explicit method names make it clear which side of the bridge is being used:

- `swait()` and future `wait()`/`result()` are tealet-blocking operations.
Cancellation needs explicit design.
- Tealet futures are asyncio-awaitable through `__await__`.
- `await_()` delegates an awaitable to asyncio and blocks the current tealet
  until asyncio completes it.
- A future tealet-side await runner could manually drive the await protocol from
Detaching only this asyncio waiter and leaving the tealet computation running.
Propagating cancellation into the tealet task, likely by throwing an exception at
its next switch boundary.
`Event.wait()`.

## Best Coexistence Strategy

The best first direction is:

1. Keep tealet as a low-level stack-switching primitive plus optional scheduler
   layer.
2. Let asyncio remain the top-level reactor for modern IO integration.
3. Embed the tealet scheduler as a guest inside the asyncio loop.
4. Provide explicit adapters in both directions:
Asyncio waiters should receive results, exceptions, and cancellation through
asyncio futures.
5. Make cancellation, context propagation, and thread ownership explicit rather
   than implicit.

This preserves tealet's main benefit: stackful cooperative concurrency for code
that wants to look synchronous. It also avoids asking tealet to reimplement the
large ecosystem surface that asyncio already owns.
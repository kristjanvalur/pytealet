# Tealet and Asyncio Coexistence

This note explores how a tealet-style stack-switching scheduler could coexist
with Python's native `asyncio` coroutine ecosystem.

The short version: `asyncio` should usually own the IO reactor, while a tealet
scheduler owns stackful user-code scheduling. Tealet tasks can then keep the
main ergonomic benefit of stack switching, namely synchronous-looking code that
can suspend cooperatively, while still using modern asyncio-driven IO libraries.

This is design reasoning, not a committed public API.

## Current Example Model

The scheduler in `src/tealet_examples.py` is intentionally small:

- `Scheduler` owns a runnable queue of tealets.
- `Event.wait()` blocks the current tealet by recording it as a waiter and
  switching to another runnable tealet.
- `Event.set()` marks the event set and moves blocked tealets back to the
  runnable queue.
- `Future.result()` waits synchronously from the point of view of the tealet
  task, using `Event` as its wakeup primitive.

That model is stackful and scheduler-local. It is not the same suspension
protocol used by native `async def` coroutines.

## Awaitable Tealet Events

Tealet events can be made usable from asyncio, but the meaning should be
carefully scoped.

A direct spelling such as this is attractive:

```python
await event
await future
```

However, making the raw objects implement `__await__` may hide an important
boundary. An asyncio coroutine must suspend by yielding control to the asyncio
event loop. It must not call the existing tealet-blocking `Event.wait()`,
because `Event.wait()` assumes there is a current tealet task and that it is
legal to stack-switch to another tealet.

A clearer first API would expose explicit adapters:

```python
await event.async_wait()
result = await future.async_result()
```

Internally, an event would likely keep two classes of waiters:

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

`Future` can use the same bridge:

```python
def result(self) -> T:
    ...  # blocks a tealet task

async def async_result(self) -> T:
    ...  # awaits from an asyncio task
```

The raw `__await__` convenience can be added later if the explicit adapter
semantics prove stable.

## Tealet Tasks Waiting on Asyncio

A tealet task cannot literally use `await` unless it is an `async def` native
coroutine. That is a Python syntax rule, not a tealet limitation.

The more useful bridge is allowing synchronous-looking tealet code to wait for
an asyncio awaitable:

```python
def worker() -> bytes:
  response = get_running_scheduler().wait_async(fetch_bytes(url))
    return parse_response(response)
```

`wait_async()` would roughly do this:

1. Require or capture an owning asyncio event loop.
2. Convert the awaitable to an asyncio task or future.
3. Attach a completion callback.
4. Mark the current tealet as blocked on that asyncio future.
5. Switch to another runnable tealet.
6. When the asyncio future completes, make the blocked tealet runnable again.
7. When resumed, return the result or raise the exception into the tealet task.

That keeps the stackful value proposition: code inside a tealet task can call
ordinary functions that eventually wait on IO without coloring every caller as
`async def`.

It is also possible for `spawn()` to accept an `async def` function and drive the
native coroutine protocol. That is a less compelling first target, because once
code is already a native coroutine, asyncio is already the natural scheduler for
it. The stronger use case is stackful tealet code consuming asyncio-backed IO.

## Manually Driving the Await Protocol

There is a more direct bridge than delegating the whole awaitable to asyncio:
the tealet scheduler can manually drive the await protocol from a normal
function.

The proposed spelling might be:

```python
def worker() -> bytes:
  data = get_running_scheduler().await_(fetch_bytes(url))
    return data
```

This is feasible. A normal Python function can obtain an await iterator and
drive it with `send()`, `throw()`, and `close()`:

```python
iterator = awaitable.__await__()
try:
    yielded = iterator.send(None)
except StopIteration as exc:
    return exc.value
```

This is the same family of technique explored by `py-asynkit`. In particular,
`asynkit.CoroStart` starts a coroutine eagerly by sending into it until it either
returns, raises, or yields a blocking object. `asynkit.await_sync()` then treats
"the coroutine yielded" as failure, because a purely synchronous caller has no
scheduler available to finish the operation.

Tealet changes that last step. If the coroutine yields,
`get_running_scheduler().await_()` does not need to fail. It can interpret the
yielded value as a wait request, park the current tealet, and resume the await
iterator later.

Conceptually:

```python
def await_(self, awaitable):
    iterator = awaitable.__await__()
    send_value = None
    pending_error = None

    while True:
        try:
            if pending_error is not None:
                yielded = iterator.throw(pending_error)
                pending_error = None
            else:
                yielded = iterator.send(send_value)
                send_value = None
        except StopIteration as exc:
            return exc.value

        try:
            send_value = self.wait_yielded_awaitable(yielded)
        except BaseException as exc:
            pending_error = exc
```

The real design problem is `wait_yielded_awaitable(yielded)`. The Python await
protocol defines how an awaitable yields control, but the yielded values are a
contract with a scheduler. Asyncio's scheduler understands asyncio futures,
tasks, bare `None` yields used by some fast paths, cancellation, and loop state.
A tealet scheduler would need its own interpretation layer.

Useful cases are straightforward:

- If the coroutine yields a tealet event or tealet future, wait on it directly.
- If it yields a pending asyncio future, add a done callback, park the current
  tealet, and resume when the future completes.
- If it yields an already-done asyncio future, resume the await iterator
  immediately.
- If it yields `None`, treat that as a cooperative checkpoint and reschedule the
  current tealet soon.
- If it yields an unsupported scheduler token, raise a clear error or delegate
  the remaining coroutine to a real asyncio task.

For asyncio futures, the driver should usually resume the await iterator with
`None`, not with `future.result()`. `asyncio.Future.__await__()` yields the
future while pending; when it is resumed after completion, the future's own await
iterator calls `future.result()` and either returns the value or raises the
stored exception.

This is different from `wait_async(awaitable)`. A `wait_async()` operation can be
implemented by handing the whole awaitable to `asyncio.create_task()` and waiting
for that task. An `await_()` operation is a tealet-side task runner. It drives
the coroutine step by step and only delegates the leaves it cannot service
itself.

That gives tealet an interesting depth-first execution model:

```python
def service_request() -> Response:
  user = get_running_scheduler().await_(load_user())
  permissions = get_running_scheduler().await_(load_acl())
    return render(user, permissions)
```

No `async def` is required in the tealet task, but the called functions can still
be native async functions. Coroutines that complete before their first real IO
wait return with very little scheduling overhead. Coroutines that block become
ordinary tealet blocking points.

There are important caveats.

First, many asyncio APIs assume a running event loop. For example, an awaitable
may call `asyncio.get_running_loop()` before it ever yields. If the tealet task is
not currently executing inside an asyncio loop context, those awaitables will
fail. This is easier in the asyncio-hosted model, where tealet work is pumped
from the loop thread while a loop exists.

Second, some asyncio APIs assume a current asyncio task. `asyncio.current_task()`,
timeouts, task groups, and cancellation machinery can depend on real Task state.
Asynkit handles related eager-start cases by creating a real task on demand when
task identity is observed. Tealet could use a similar escape hatch:

1. Start the coroutine manually.
2. If it completes, return the result immediately.
3. If it yields ordinary future-like IO, keep driving it from tealet.
4. If it asks for task identity or unsupported asyncio machinery, wrap the
   remaining await iterator in a real asyncio task and wait for that task.

Third, cancellation and closing must mirror coroutine protocol semantics. If the
tealet task waiting in `await_()` is cancelled, the scheduler should decide
whether to:

- detach from the awaitable and leave underlying asyncio work running;
- cancel the yielded asyncio future or task;
- throw a cancellation exception into the await iterator;
- close the await iterator with `GeneratorExit`.

The safest first API should make those policies explicit.

The promising conclusion is that `get_running_scheduler().await_(awaitable)` is
not only possible, it may be the most tealet-native bridge. It is essentially
`await_sync()` plus a cooperative scheduler. The hard part is not driving
`__await__`; the hard part is defining which yielded scheduler tokens tealet
understands, and when it falls back to asyncio's Task machinery.

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
  data = get_running_scheduler().wait_async(fetch_bytes(url))
    process(data)
```

The difference is that `wait_async()` would rely on the asyncio-pump tealet to
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
    _read_waiters: dict[int, TealetTask]
    _write_waiters: dict[int, TealetTask]

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

The concrete first experiment is now narrow and Unix-first: `SelectorScheduler`
provides selector-backed readiness callbacks and socket helpers, and
`TealetSelectorEventLoop` provides an experimental tealet-aware selector adapter
for `asyncio.SelectorEventLoop`. Asyncio timers, self-pipe wakeups, and socket
readiness can share the host scheduler's blocking point.

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
compatibility to an asyncio event loop. Reimplementing selectors, transports,
TLS, subprocess handling, and third-party asyncio integration would move tealet
away from its core strength.

So a combined scheduler is best viewed as a policy facade over an asyncio loop
plus a tealet runnable queue, not as a full replacement reactor.

## Cancellation and Errors

Cancellation needs explicit design.

When an asyncio task awaits a tealet future and is cancelled, there are at least
two possible meanings:

- Detach only this asyncio waiter and leave the tealet computation running.
- Propagate cancellation into the tealet task, likely by throwing an exception at
  its next switch boundary.

Those are different policies and should be visible in the API. A default that
only detaches the waiter is safer. A stronger `cancel_task=True` or explicit
`future.cancel()` operation can inject cancellation into the tealet task.

The same issue exists in the other direction. If a tealet task blocks on an
asyncio future and the tealet task is cancelled, the bridge must decide whether
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
    def wait_async(self, awaitable) -> object: ...
    def await_(self, awaitable) -> object: ...


class Event:
    def wait(self) -> None: ...
    async def async_wait(self) -> None: ...
    def set(self) -> None: ...


class TealetFuture(Generic[T]):
    def result(self) -> T: ...
    async def async_result(self) -> T: ...
    def cancel(self) -> bool: ...
```

The explicit method names make it clear which side of the bridge is being used:

- `wait()` and `result()` are tealet-blocking operations.
- `async_wait()` and `async_result()` are asyncio-awaiting operations.
- `wait_async()` delegates an awaitable to asyncio and blocks the current tealet
  until asyncio completes it.
- `await_()` manually drives the await protocol from the tealet scheduler,
  falling back to asyncio task machinery only when needed.

Later, if the behavior is stable, `Event.__await__` and `TealetFuture.__await__`
could forward to the explicit asyncio methods.

## Best Coexistence Strategy

The best first direction is:

1. Keep tealet as a low-level stack-switching primitive plus optional scheduler
   layer.
2. Let asyncio remain the top-level reactor for modern IO integration.
3. Embed the tealet scheduler as a guest inside the asyncio loop.
4. Provide explicit adapters in both directions:
   - asyncio code awaits tealet events and futures;
  - tealet code delegates asyncio awaitables with `wait_async()`;
  - tealet code can experimentally drive awaitables with `await_()`.
5. Make cancellation, context propagation, and thread ownership explicit rather
   than implicit.

This preserves tealet's main benefit: stackful cooperative concurrency for code
that wants to look synchronous. It also avoids asking tealet to reimplement the
large ecosystem surface that asyncio already owns.
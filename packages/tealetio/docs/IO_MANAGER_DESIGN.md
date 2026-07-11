# IO manager design

Status: **Implemented** (phases 1–3). `ProactorIOManager` and `scheduler.io` are
the proactor blocking-IO entry point. Selector blocking helpers remain on
`SelectorScheduler` for now; a future `SelectorIOManager` can adopt the same
`scheduler.io` gate without changing proactor callers.

## Motivation

`asyncio` folds scheduling, blocking socket helpers, file IO, DNS, transports,
and more into a single `loop` object. `tealetio` already does better at the
*submission* layer (`Proactor` with `SelectorProactor` and `UringProactor`), but
the scheduler type used to carry a wide IO surface alongside scheduling concerns.

**Goal (achieved):** make IO capability an explicit, composable object bound to
the scheduler's proactor backend, while keeping scheduling on `BaseScheduler`.

## Current layering

```text
tealetio.streams          open_connection, start_server, StreamReader/Writer
        │
        ▼
scheduler.io              ProactorIOManager — IOWaiter-returning sock_*, poll, open
        │
        ▼
Proactor (Protocol)       recv/send/accept/… → Operation[T]
        │
        ├── SelectorProactor
        └── UringProactor
```

Parallel path (unchanged for now):

```text
SelectorScheduler + SelectorMixin
        │
        ├── add_reader / add_writer  (asyncio guest-loop seam)
        └── sock_* / poll* on scheduler surface (selector driving path)
```

A future `SelectorIOManager` could move selector blocking helpers behind
`scheduler.io` the same way, leaving `SelectorMixin` focused on fd-callback
registration.

## `ProactorIOManager` (concrete)

Composition over subclassing `Scheduler` per IO backend.

```python
class ProactorIOManager:
    def __init__(self, proactor: Proactor) -> None: ...

    def sock_recv(self, sock: socket.socket, n: int) -> IOWaiter[bytes]: ...
    # … remaining sock_*, poll, file helpers; callers block via IOWaiter.wait()
```

`ProactorScheduler` holds `self._io: ProactorIOManager` and exposes it as
`scheduler.io`. Blocking IO methods are **not** forwarded on the scheduler
surface anymore; callers use `scheduler.io`.

**Why composition?**

- Test IO without a full scheduler (mock waiter + fake `Proactor`).
- Swap `SelectorProactor` vs `UringProactor` without touching task logic.
- `streams` and other callers depend on `scheduler.io` or a protocol, not
  `isinstance(scheduler, ProactorScheduler)`.
- Avoid growing `BaseScheduler` with `NotImplementedError` IO stubs.

### Protocol split (narrow interfaces)

Avoid one giant `IOManager` protocol. Suggested slices:

| Protocol | Responsibility | Status |
|----------|----------------|--------|
| `Proactor` | submit IO, return `Operation` | exists |
| `IOWaiter` | one-shot blocking handle; `.wait()` / `.forget()` | `io_waiter.py`; returned by `ProactorIOManager` helpers |
| `SocketIO` | `sock_recv`, `sock_connect`, `sock_create`, … | protocol in `io_manager.py`; `ProactorIOManager` |
| `PollIO` | `poll`, `poll_many` | protocol in `io_manager.py`; `ProactorIOManager` |
| `FileIO` | positioned `open` | protocol in `io_manager.py`; `ProactorIOManager` |
| `IOFile` | positioned binary read/write/seek/close on an opened handle | protocol in `files.py`; `ProactorFile` today |
| `ServerIO` | `SocketIO` + proactor submission (`accept_many`, …) | protocol in `io_manager.py`; stream servers |
| `StreamIO` (optional) | `open_connection`, `start_server` | module-level in `streams` |

### Return-type decoupling (partial)

IO manager *entry* protocols (`SocketIO`, `PollIO`, `FileIO`) should not leak
backend-specific concrete types in their return annotations where a neutral handle
protocol suffices.

Done:

- `FileIO.open()` annotates `-> IOFile`; `ProactorIOManager.open()` returns the
  proactor-backed `ProactorFile` implementation.
- `ServerIO` documents the stream-server requirement (`SocketIO` + `proactor`
  submission) without naming `ProactorIOManager` at call sites.

Still open:

- Rename the concrete file handle (`ProactorFile` → neutral name) so
  implementation naming matches the decoupling goal.
- Neutral `RecvBufferPool` protocol in `SocketIO` signatures instead of the
  proactor-module type.

`SocketIO` already returns stdlib `socket.socket`; `PollIO` returns shared
`ContinuousOperation[int]`. Lifecycle (`close()`, and similar) stays on returned
handle protocols, not on `SocketIO` / `FileIO` themselves.

Slices overlap at the concrete manager: `ProactorIOManager` implements
`SocketIO`, `PollIO`, and `FileIO` on one object. That is intentional — callers
that only need sockets can type against `SocketIO` without depending on the full
manager. Lifecycle helpers such as `close()` belong on handles (`ProactorFile`,
sockets, `ContinuousOperation`) and scheduler/proactor shutdown, not on the IO
protocols. `ProactorFile` holds a `ProactorIOManager` reference and blocks
through `IOWaiter.wait()` on positioned read/write/close operations.

Module helpers:

```python
def open_connection(*, addr=..., path=..., scheduler=None):
    sched = scheduler or get_running_scheduler()
    io = sched.io  # raises when the scheduler has no IO backend
```

### Proactor vs selector manager flavours

Do **not** force a single inheritance tree for all IO styles.

- **`ProactorIOManager`** — implemented; backend is any `Proactor` implementation.
  Primary path for `tealetio.streams` and blocking socket/file IO on proactor
  schedulers.
- **`SelectorIOManager`** — *not implemented*. Would wrap selector-backed
  blocking `sock_*` / `poll*` currently on `SelectorMixin`, exposed as
  `scheduler.io` on `SelectorScheduler`. `add_reader` / `add_writer` stay on the
  scheduler as the asyncio guest-loop seam regardless.
- **`SelectorProactor`** — a proactor *implementation* inside `ProactorIOManager`,
  not a separate manager type.

## What stays on `Scheduler`

- `spawn`, `call_soon`, timers, `run_forever` / `run_until_complete`
- `run_in_executor`, `getaddrinfo`, `ensure_resolved`
- `get_running_scheduler()` / thread-local binding
- task queues, cancellation, `all_tasks`, and related introspection
- selector fd callbacks (`add_reader`, `add_writer`, …) on selector schedulers

## What lives on `scheduler.io` (proactor path)

- all `sock_*` helpers (return `IOWaiter`; callers use `.wait()`)
- `create_recv_buffer_pool` / `sock_recv_iter`
- `poll` / `poll_many`
- positioned file `open` → `IOFile` (`ProactorFile` on proactor schedulers)

Stream helpers (`open_connection`, `start_server`) remain module-level in
`streams`; they take an optional `scheduler=` and use `scheduler.io` internally.

`StreamServer` lifecycle stays in `streams`; it needs `scheduler.io` (for
`accept_many`), `spawn`, and `call_soon_threadsafe`.

## One-shot IO composition via `IOWaitGroup`

Multi-leg socket work (create → connect → send, connect → send) is composed in
`ProactorIOManager` with `IOWaitGroup`, not inside the proactor. Each leg is a
normal proactor `Operation`; the group wires advance handlers and a single
`ThreadsafeEvent` park for the caller's `.wait()`.

```text
ProactorIOManager.sock_create(connect_to=…, initial_data=…)
        │
        ▼
IOWaitGroup
  attach(create_socket) → advance_connect(sock)
    attach(connect) → finish_connected
      attach(send) when initial_data → group.finish(sock)
      else group.finish(sock)
```

```text
ProactorIOManager.sock_connect(…, initial=…)
        │
        ▼
IOWaitGroup
  attach(connect) → advance_connect
    attach(send) when initial → group.finish(None)
```

| Helper | Role |
|--------|------|
| `IOWaitGroup.attach` | Register one leg; optional `advance` runs on worker thread after success |
| `IOWaitGroupChild.value()` | One-shot handoff of a leg's result into the next advance handler |
| `on_cleanup(fail, value)` | Per-leg teardown (for example `abortive_close(sock)` on connect failure) |

Production entry points:

| Entry point | Composition |
|-------------|-------------|
| `sock_connect(…, initial=…)` | connect → optional send |
| `sock_create(…, connect_to=…)` | create → connect → optional send |
| `sock_accept(…, recv_size=…)` | accept → optional recv |
| `sock_create_streams(…, connect_to=…)` | create → connect → optional send → stream pair |

Intermediate legs are not awaited by the scheduler task. Only the returned
`IOWaiter` / `IOWaitGroup` is blocked on (via `.wait()`); the next leg is
submitted from advance handlers as completions arrive on worker threads.

### Succeed or raise

Blocking IO helpers (`sock_create`, `sock_connect`, `sock_recv`, …) either
return the requested value or raise. There is no partial-success tuple,
hint-honour flag, or internal fallback inside `ProactorIOManager`.

In the create→connect→send composition, ``sock_create(connect_to=…)`` returns
the connected ``socket.socket`` after connect (and optional send) succeed.
``sock_connect()`` returns ``None``.

### `proactor.connect` and `AF_UNIX`

`ProactorIOManager` always composes through `proactor.connect` (including the
connect leg of `sock_create(..., connect_to=…)`). Both proactor backends route
``AF_UNIX`` sockets through ``ProactorBase._sync_unix_connect()``: a brief
blocking ``sock.connect()`` followed by ``deliver()``. io_uring
``submit_connect`` does not accept UNIX sockaddr paths today, so this is not a
special-case deferral at the io_manager layer — the connect child ``Operation``
may simply complete synchronously before the root finishes. That is acceptable in
practice: Unix-domain connects are near-instant on the local machine, so a brief
blocking ``connect()`` on the completion thread does not carry the same latency
risk as a remote TCP handshake. Inet sockets still use the backend async connect
path. If io_uring gains Unix ``submit_connect`` support,
``UringProactor.connect()`` can switch ``AF_UNIX`` to the same async completion
path as inet; no io_manager or composition changes are required beyond that
backend routing.

### Cancel propagation and error cleanup

``IOWaitGroup`` tracks active legs in ``_members``. Exceptional ``wait()`` exit
cancels every tracked ``Operation`` and runs per-leg ``on_cleanup`` for unreleased
success values. ``forget()`` drops waiter interest without cancelling backend
compose work or setting ``_closed`` — the chain may still ``attach()`` later legs.
Per-leg ``on_cleanup`` hooks also run on worker-thread failure.

Cancellation always races in-flight backend completions; see
``OPERATION_CALLBACKS.md`` (cancel vs in-flight completion).

Error cleanup (for example closing a created socket when connect fails) lives in
``on_cleanup`` handlers on each ``IOWaitGroupChild`` leg.

## Continuous callback composition

Long-lived proactor operations (`accept_many`, `recv_many`, `poll_many`, …)
emit results through a single result callback (or `callback_factory(parent)` when
composition needs the parent `ContinuousOperation`). Nested work started from
that callback — accept-time pre-read, echo sends in tests, and similar — is
use the same suboperation model as one-shot ops via `chain_suboperation`;
continuous-specific helpers live in `continuous_callbacks.py`.

### Sub-operations: cancel propagation only

`ContinuousOperation._active_suboperations` exists so `cancel()` can reach
children submitted from result callbacks. It is **not** a drain barrier.

| Parent event | Attached children |
|--------------|-------------------|
| Normal finish (EOF, final multishot CQE, …) | Keep running; completion handlers may still run |
| Error finish (`_finish(exception=…)`) | Keep running — e.g. `ECONNRESET` on the parent socket must not cancel accepts/recvs already started from earlier callbacks |
| `cancel()` | `_finish(cancelled=True)` on each child once the parent is `_done` |

`_finish()` does not clear or cancel `_active_suboperations`. Each child removes
itself with `detach_suboperation()` from its done handler (typically via
`chain_suboperation()`). Once the parent is done, `attach_suboperation()` returns
false so new children are not linked; in-flight children drain independently.

Fire-and-forget nested work that should not participate in parent cancel should
not call `attach_suboperation()` (or should detach after submit).

### Accept-time pre-read

Built-in uring `receive_on_accept` was removed from the proactor. Accept-time
pre-read is composed with `accept_read_delivery()` and wired from
`ProactorIOManager.accept_many(..., recv_size=…)` / `start_server(...,
recv_size=…)`. The proactor emits bare `socket` connections; tuple delivery
`(conn, initial_data, recv_error)` is the callback/io_manager layer.

When multishot accept ends (`IORING_CQE_F_MORE` clears), the parent may finish
while a nested recv from the last accept is still in flight. That is expected:
the accept stream has ended, not that all per-connection work has completed.
User delivery for connections handed off while the parent was still active is
normal drain behaviour.

## Capability gate

| Concern | asyncio | tealetio (proactor) |
|---------|---------|---------------------|
| IO submission | loop → proactor internally | `scheduler.proactor` |
| Blocking facade | `loop.sock_recv`, … | `scheduler.io.sock_*` |
| High-level streams | `asyncio.start_server` | `tealetio.streams` via `scheduler.io` |
| Capability gate | `get_running_loop()` | `get_running_scheduler().io` |

`BasicScheduler.io` and `SelectorScheduler.io` raise `RuntimeError` when the
scheduler has no proactor IO facade (selector schedulers get a targeted message).
Only `ProactorScheduler` exposes a real `scheduler.io`. Prefer accessing `.io`
over probing scheduler concrete types in new code; use `SupportsProactorIO` for
static typing after `ProactorScheduler` narrowing.

## Migration (completed)

| Phase | Work | Status |
|-------|------|--------|
| 1 | `ProactorIOManager` + `scheduler.io` property | done |
| 2 | `streams` uses `scheduler.io`; IO methods removed from scheduler surface | done (breaking) |
| 3 | Document `scheduler.io`; steer new code away from `isinstance(..., ProactorScheduler)` for IO | done (this doc pass) |
| 4 | Optional: `SelectorIOManager`; stream methods only at module level | streams already module-only; selector manager open |
| 5 | Decouple IO protocol return types (`IOFile`, neutral handle names) | partial (`IOFile`, `ServerIO`); rename `ProactorFile` open |

## Resolved decisions

1. **Blocking one-shot IO** — `ProactorIOManager` helpers return `IOWaiter`;
   callers block via `IOWaiter.wait()`. `ProactorFile` holds `ProactorIOManager`
   directly and uses the same `IOWaiter.wait()` path for positioned I/O.
2. **`BasicScheduler.io`** — property raises `RuntimeError`, not `None`.
3. **`AsyncProactorScheduler`** — shares the same `ProactorIOManager` instance as
   the sync proactor core on a given scheduler object.
4. **Stream helpers** — module-level only (`open_connection`, `start_server`);
   optional `scheduler=` for callers outside a running driver turn.

## IOWaiter and interrupted waits

One-shot `ProactorIOManager` helpers return `IOWaiter` handles. The underlying
`Operation` is submitted when the helper returns. The code that owns the handle
calls either `wait()` or `forget()` — not both, and not as a public end-user
API (`streams` / `files` call `wait()` internally today). `IOWaiter` does not
enforce that contract; calling `wait()` after `forget()` is undefined. There is
no public `cancel()` on `IOWaiter` — cancellation is an internal concern at the
operation / proactor layer, not a third blocking-IO disposition.

If `wait()` exits exceptionally (for example `timeout()` throwing into the
blocked tealet while `ThreadsafeEvent.swait()` is parked), the waiter cancels
pending backend work and re-raises — unless delivery already completed, in
which case the interrupt is swallowed and the result (or completion exception)
is returned. ``IOWaiter`` checks the underlying ``Operation``; ``IOWaitGroup``
serialises ``finish()`` / ``_complete()`` and the interrupt path on a lock so a
worker-thread delivery that wins the race is not torn down by a concurrent
timeout. An interrupted ``wait()`` sets ``IOWaitGroup._closed``; a late
``finish()`` then returns ``False`` and compose handlers discard the result
(for example ``abortive_close`` on a socket). The handle cannot be waited on
again after a genuine interrupt; the caller must submit fresh work. `forget()`
is different: it drops waiter
interest without cancelling backend work — mostly to break callback cycles by
nulling the waiter's ``_operation`` reference.

**Resource-creating helpers must use ``wait()``.** ``forget()`` on handles from
``sock_accept``, ``sock_create`` (with ``connect_to``), ``sock_create_streams``,
and other helpers that hand back sockets or streams is undefined behaviour.
Callers always want the created resource; use ``wait()`` (or let ``streams`` /
``files`` call it internally). ``forget()`` remains available for narrow
internal uses on non-resource one-shot ops.

**Grouped waiters (`IOWaitGroup`).** Multi-leg helpers (`sock_create` with
``connect_to``, ``sock_connect`` with ``initial``, ``sock_accept`` with
``recv_size``, ``sock_create_streams``) return an ``IOWaitable`` backed by a
group. Each leg is registered with ``attach()``;
advance handlers run on worker threads and submit the next leg. The group parks
once on a single ``ThreadsafeEvent`` until ``finish()`` or an error.
``IOWaitGroupChild.value()`` is one-shot and hands a leg result into the next
advance handler. An optional ``on_cleanup`` hook on each leg receives failures
(``fail=True``) or unreleased success values when ``wait()`` exits exceptionally
or from ``__del__``. ``sock_create_streams`` passes ``abortive_close`` on connect
failure and closes locally when stream open fails after ``value()``.

**Data loss on interrupted waits (current behaviour).** We do **not** currently
guarantee that bytes already read from the kernel but not yet delivered to the
caller remain visible on the socket after a timed-out or otherwise interrupted
wait. A timeout on `sock_recv` / stream `recv` should be treated as aborting
that receive attempt, not as a restartable partial read. Compare with asyncio's
stream and socket receive timeout semantics in a follow-up — document whether
asyncio preserves kernel/socket buffer state on timeout or also abandons the
in-flight read.

**UringProactor races.** Delivery runs on worker threads while `wait()` blocks
on the scheduler tealet. Completion delivery, `Operation._finish`, and
wait-side cancellation can race: a CQE may be processed on the worker thread
around the same time the tealet cancels from a timeout. That makes uring paths
especially sensitive here.

**Subsequent PR (waitable cancel).** A likely direction is *waitable cancel*:
route cancellation through the proactor / uring submission path so cancel and
completion are ordered relative to the same ring, rather than racing a Python
`Operation.cancel()` on the main thread against worker delivery. Until that
exists, treat interrupted waits as best-effort abort, not atomic “keep bytes,
drop waiter only”.

## Open follow-ups

- **Waitable cancel for interrupted `IOWaiter` waits** — design and implement
  proactor/uring-integrated cancel so timeout and exceptional `wait()` exits
  race less with worker-thread delivery; revisit asyncio parity for buffered
  bytes on recv timeout.
- Implement `SelectorIOManager` and wire `SelectorScheduler.io` when selector
  blocking IO should share the same capability gate as proactor schedulers.
- `SocketIO`, `PollIO`, and `FileIO` entry protocols are implemented; a future
  `SelectorIOManager` could implement the same slices for selector schedulers.
- Rename `ProactorFile` and review proactor-specific names in other protocol
  signatures (`RecvBufferPool`, and similar). `IOFile` and `ServerIO` are done.
- `StreamServer.serve_forever()` sugar (implemented); signal handling stays in
  `Runner`, not the server object.
- **Stream transport close via `io.sock_close`** — `SocketTransport.close()` still
  calls `socket.close()` directly; route through `sock_shutdown` / `sock_close`
  in a follow-up PR when stream closing semantics are cleaned up (this PR adds
  the proactor/manager close paths as preparation).

## References

- `packages/tealetio/src/tealetio/io_manager.py` — `ProactorIOManager`
- `packages/tealetio/src/tealetio/io_waiter.py` — `IOWaiter`, `IOWaitGroup`, grouped composition
- `packages/tealetio/src/tealetio/operation_callbacks.py` — `chain_suboperation` for continuous nested work
- `packages/tealetio/src/tealetio/continuous_callbacks.py` — continuous result-callback composition
- `packages/tealetio/src/tealetio/proactor.py` — `Proactor`, `ProactorScheduler`
- `packages/tealetio/src/tealetio/files.py` — `ProactorFile`, `IOFile`
- `packages/tealetio/src/tealetio/streams.py` — streams API
- `packages/tealetio/docs/PYTHON_API.md` — user-facing API
- `packages/tealetio/docs/OPERATION_CALLBACKS.md` — callback module layout and composition contracts
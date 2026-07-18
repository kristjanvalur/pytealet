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
| `IOWaiter` | one-shot blocking handle over a proactor `Operation` | `io_waiter.py`; returned by most `ProactorIOManager` helpers |
| `IOWaiterSync` | already-resolved waitable (value or exception, no `Operation`) | `io_waiter.py`; e.g. create-only `sock_create` |
| `SocketIO` | `sock_recv`, `sock_connect`, `sock_create`, … | protocol in `io_manager.py`; `ProactorIOManager` |
| `PollIO` | `poll`, `poll_many` | protocol in `io_manager.py`; `ProactorIOManager` |
| `FileIO` | positioned `open` | protocol in `io_manager.py`; `ProactorIOManager` |
| `IOFile` | positioned binary read/write/seek/close on an opened handle | protocol in `files.py`; `ProactorFile` today |
| `ServerIO` | `SocketIO` + proactor submission (`accept_many`, …) | protocol in `io_manager.py`; stream servers |
| `StreamOpenIO` / `StreamWriterIO` | narrow slices for stream-pair buffer open | protocols in `streams/open.py` and `streams/writer.py`; **not** exported from `io_manager` or top-level `tealetio` — `ProactorIOManager` satisfies them structurally |
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

Multi-leg socket work (connect → send, and the connect/send legs of
`sock_create`) is composed in `ProactorIOManager` with `IOWaitGroup`, not
inside the proactor. Socket creation for `sock_create` is direct stdlib;
each async leg is a normal proactor `Operation`. The group wires advance
handlers and a single `CrossThreadEvent` park for the caller's `.wait()`.

```text
ProactorIOManager.sock_create(connect_to=…, initial_data=…)
        │
        ▼
direct socket.socket() + configure_scheduler_socket
        │
        ▼
IOWaitGroup
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
| `sock_create(…, connect_to=…)` | direct create → connect → optional send |
| `sock_accept(n=…)` | accept → optional recv |
| `sock_create_streams(…, connect_to=…)` | direct create → connect → optional send → `_open_streams` on advance (``recv_many`` armed before ``wait()`` returns) |

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

## Continuous operations and callback composition

Long-lived proactor operations (`accept_many`, `recv_many`, `poll_many`, …)
emit bare chunks through each `ContinuousOperation`'s `result_callback`. The
proactor does not shape delivery tuples, marshal onto the scheduler thread, or
compose accept-time reads — that lives in `ProactorIOManager` and
`continuous_callbacks.py`. See `OPERATION_CALLBACKS.md` for the full split.

| Layer | Responsibility |
|-------|----------------|
| `Proactor` | submit continuous ops; `_emit_result(chunk)` until finish/error/cancel |
| `ProactorIOManager` | worker-side accept mutation (preread, stream open), scheduler reorder and `finish_operation` |
| Application (`streams`, custom servers) | delivery disposition after shutdown or loss of interest |

### Accept-time pre-read

Built-in uring `receive_on_accept` was removed from the proactor. Accept-time
pre-read is wired in `ProactorIOManager._accept_preread_on_worker()` and exposed
via `accept_many(..., recv_size=…)` and `sock_accept(..., n=…)` only. The worker
schedules each accept-time `recv`; when it completes, one merged
`MultishotDelivery` (same leg index, `value=(conn, initial_data, recv_error)`)
is posted onto the scheduler reorder buffer. `accept_many_streams()` /
`start_server()` do not preread; they open streams on the worker delivery thread
and arm `recv_many` through `RecvIterBuffer` before posting `(reader, writer)`
to the scheduler. The proactor emits bare `socket` connections.

Each accept-time `recv` is a separate one-shot `Operation` registered with
`add_done_callback`. It is not linked to the parent `ContinuousOperation`;
cancelling the accept stream does not automatically cancel in-flight recvs. A
recv that completes with ``OSError(errno.ECANCELED)`` (see
``is_io_cancellation()``) is closed in the io_manager without calling the user
accept callback.

When multishot accept ends (`IORING_CQE_F_MORE` clears), the parent may finish
while a nested recv from the last accept is still in flight. That is expected:
the accept stream has ended, not that all per-connection work has completed.

### Delivery disposition

Whether a late delivery is processed, ignored, or discarded is **application
policy**, not enforced by `Operation` or the proactor.

`StreamServer` is the reference pattern. After `close()` synchronously cancels
the accept-loop tealet, `on_accept` still runs for accepts (and accept-time
recvs) already in flight. The server checks `_closed` and **discards** those
deliveries by closing the writer and returning without spawning a handler. The
accept-loop tealet wraps its main loop in ``try``/``finally`` so task
``CancelledError`` or IO ``OSError(ECANCELED)`` from ``accept_many().wait()``
sets `_closed` and closes listening sockets; `close()` does not close listeners
itself. In-flight handler tealets started before shutdown keep running
until they exit; `wait_closed()` blocks on the accept-loop ``Task`` and each
handler ``Task``.

Custom `accept_many` / `accept_many_streams` callbacks should apply the same
pattern when they need to reject work after shutdown: check a local flag, close
the connection or streams, and return. The io_manager marshals onto the
scheduler thread but does not implement server lifecycle.

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
blocked tealet while `CrossThreadEvent.swait()` is parked), the waiter cancels
pending backend work and re-raises — unless delivery already completed, in
which case the interrupt is swallowed and the result (or completion exception)
is returned. ``IOWaiter`` checks the underlying ``Operation``; ``IOWaiterSync``
is always ready; ``IOWaitGroup``
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
once on a single ``CrossThreadEvent`` until ``finish()`` or an error.
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
completion are ordered relative to the same ring, rather than racing
`Proactor.cancel()` teardown submission against worker delivery. Until that
exists, treat interrupted waits as best-effort abort, not atomic “keep bytes,
drop waiter only”.

## Open follow-ups

- **Waitable cancel for interrupted `IOWaiter` waits** — design and implement
  proactor/uring-integrated cancel so timeout and exceptional `wait()` exits
  race less with worker-thread delivery; revisit asyncio parity for buffered
  bytes on recv timeout. Current exceptional exits use
  `Proactor.cancel(operation).forget()`; pump `proactor.wait()` when
  `has_pending_operations()` must reach zero before ring close.
- Implement `SelectorIOManager` and wire `SelectorScheduler.io` when selector
  blocking IO should share the same capability gate as proactor schedulers.
- `SocketIO`, `PollIO`, and `FileIO` entry protocols are implemented; a future
  `SelectorIOManager` could implement the same slices for selector schedulers.
- Rename `ProactorFile` and review proactor-specific names in other protocol
  signatures (`RecvBufferPool`, and similar). `IOFile` and `ServerIO` are done.
- `StreamServer.serve_forever()` sugar (implemented); signal handling stays in
  `Runner`, not the server object.
- **Stream writer shutdown** — `StreamWriter.close()` is non-blocking; callers
  must `wait_closed()` to flush queued sends and release the socket via
  `sock_shutdown` / `sock_close`. `StreamServer` handler cleanup calls
  `wait_closed()` after `close()`.
- Stream endpoints live under `packages/tealetio/src/tealetio/streams/`
  (`reader`, `writer`, `open`, `connect`, `server`); IO bridge buffers remain
  in `io_buffers.py`. `open` is the leaf `io_manager` imports for stream-pair
  construction.

## References

- `packages/tealetio/src/tealetio/io_manager.py` — `ProactorIOManager`
- `packages/tealetio/src/tealetio/io_waiter.py` — `IOWaiter`, `IOWaiterSync`, `IOWaitGroup`, grouped composition
- `packages/tealetio/src/tealetio/continuous_callbacks.py` — helpers for io_manager accept paths
- `packages/tealetio/src/tealetio/proactor.py` — `Proactor`, `ProactorScheduler`
- `packages/tealetio/src/tealetio/files.py` — `ProactorFile`, `IOFile`
- `packages/tealetio/src/tealetio/streams/` — streams API
- `packages/tealetio/src/tealetio/io_buffers.py` — `RecvIterBuffer`, `SendBuffer`
- `packages/tealetio/docs/PYTHON_API.md` — user-facing API
- `packages/tealetio/docs/OPERATION_CALLBACKS.md` — io_manager continuous composition and delivery disposition
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
scheduler.io              ProactorIOManager — wait_operation, sock_*, poll, open
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

    def wait_operation(self, op: Operation[T]) -> T: ...
    def sock_recv(self, sock: socket.socket, n: int) -> bytes: ...
    # … remaining sock_*, poll, file helpers
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
| `OperationWaiter` | `wait_operation(op) -> T` | implemented in `files.py` |
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
protocols. `OperationWaiter` stays separate for `ProactorFile` even though the
manager also exposes `wait_operation`.

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

- `wait_operation`
- all `sock_*` helpers and `create_recv_buffer_pool` / `sock_recv_iter`
- `poll` / `poll_many`
- positioned file `open` → `IOFile` (`ProactorFile` on proactor schedulers)

Stream helpers (`open_connection`, `start_server`) remain module-level in
`streams`; they take an optional `scheduler=` and use `scheduler.io` internally.

`StreamServer` lifecycle stays in `streams`; it needs `scheduler.io` (for
`accept_many`), `spawn`, and `call_soon_threadsafe`.

## Operation chaining

Multi-leg socket work (create → connect → send, connect → send) is built in
`operation_chaining.py`, not inside the proactor. The proactor only accepts an
optional `operation_factory` on `create_socket` and `connect`; chaining policy
lives on `ProactorIOManager`.

```text
ProactorIOManager.sock_create(connect_to=…)
        │
        ▼
create_socket_chain_factory(proactor, …)   ← chained_fdclose_link root
        │
        └─ connect_send_chain_factory(proactor, initial, parent=root)
               └─ chained_send_link tail
```

Chain factories take `proactor` as their first argument. Delivery handlers,
advance hooks, and `next_operation` callbacks are nested closures built at
chain creation time — they bind the proactor, addresses, initial send buffers,
parent operations, cleanup policy, and result shaping before any `Operation` is
submitted. When a backend completion arrives on the worker thread, those
handlers start the next leg (`proactor.connect`, `proactor.send`) without the
scheduler blocking on intermediate results.

Production chains today:

| Entry point | Root factory | Connect → send leg |
|-------------|--------------|-------------------|
| `sock_connect(…, initial=…)` | `connect_send_chain_factory` | same factory |
| `sock_create(…, connect_to=…)` | `create_socket_chain_factory` + `chained_fdclose_link` | `connect_send_chain_factory` as child |

`chained_connect_link` is a composable building block used in tests; production
paths use `connect_send_chain_factory` directly.

Intermediate legs are not awaited by the scheduler task. Only the root
`Operation` is passed to `wait_operation`; child submissions happen from
delivery callbacks as completions arrive.

### Succeed or raise

Blocking IO helpers (`sock_create`, `sock_connect`, `sock_recv`, …) either
return the requested value or raise. There is no partial-success tuple,
hint-honour flag, or internal fallback inside `ProactorIOManager`.

In the create→connect→send chain, only the root ``create_socket`` operation
has a non-``None`` success result: the ``socket.socket``. Connect and send legs
complete with ``None``; their work is done when the chain advances without
error. ``sock_create(connect_to=…)`` blocks on that root and returns the
socket (already connected, initial data flushed when requested).
``sock_connect()`` returns ``None``.

### `proactor.connect` and `AF_UNIX`

`ProactorIOManager` always chains through `proactor.connect` (including the
connect leg of `sock_create(..., connect_to=…)`). Both proactor backends route
``AF_UNIX`` sockets through ``ProactorBase._sync_unix_connect()``: a brief
blocking ``sock.connect()`` followed by ``deliver()``. io_uring
``submit_connect`` does not accept UNIX sockaddr paths today, so this is not a
special-case deferral at the io_manager layer — the chained ``Operation`` may
simply complete synchronously before ``wait_operation`` returns. That is
acceptable in practice: Unix-domain connects are near-instant on the local
machine, so a brief blocking ``connect()`` on the completion thread does not
carry the same latency risk as a remote TCP handshake. Inet sockets still use
the backend async connect path. If io_uring gains Unix ``submit_connect``
support, ``UringProactor.connect()`` can switch ``AF_UNIX`` to the same async
completion path as inet; no io_manager or chaining changes are required beyond
that backend routing.

### Chain spine: cancel down, advance up

Each link carries two pointers in opposite directions:

```text
cancel:  root ──cancel_forward──► connect ──cancel_forward──► send (active leaf)
advance: root ◄──chain_parent──── connect ◄──chain_parent──── send
```

`cancel_forward` is a weak reference to the **active** child leg. The proactor
entry for that leg holds the strong reference while IO is live.

### Advance hooks (one shot per leg)

When a child leg completes, `Operation.advance()` walks **up** `chain_parent`
and dispatches the first `_advance_hook` it finds. The hook stays installed
while the handler runs; `advance_continue()` asserts it is still present, clears
it under the link lock, and bubbles toward the root. That single clear point is
the contract boundary for the link.

Today each leg has at most one hook invocation before one `advance_continue()`.
If links later gain DAG shape (multiple upstream arrivals per link), a hook may
run more than once on the same leg until the final arrival; only the last
`advance_continue()` clears the hook and propagates upward.

Intermediate legs often stay `_done=False` on the success path; only the root
future is awaited by the caller. `_done` on a child means that leg's own
`Operation` completed (cancel, explicit `complete()`, or a non-chain delivery
path), not merely that its backend work finished.

### Cancel vs dynamic chain extension

Worker threads extend the chain from delivery handlers (`proactor.connect`,
`proactor.send`) while the scheduler thread may call `cancel()` on the root at
the same time. Without synchronisation, cancel can miss a child that was not yet
linked on `cancel_forward`, leaving live proactor IO unreachable from
`cancel()`.

**Linking** — `Operation.attach_child()` installs `chain_parent` and
`cancel_forward` under the **parent** lock only when `not parent._done`.
`operation_factory` uses this path; a failed attach marks the orphan child
cancelled so the proactor skips submit. Delivery handlers and
`_chain_next_operation` call `may_extend_chain()` (same lock, same check) before
starting the next leg.

**Cancel snapshot** — `cancel()` reads `_done` and `cancel_forward` under the
node lock, recurses into the child **without** holding the parent lock across
the descent, then runs this node's `cancel_hook` and `_set_cancelled()` on the
unwind.

Invariant: once `parent._done`, no new leg may be linked; once a leg is
published on `cancel_forward`, cancel from the root must reach it (or a
descendant on the same spine).

### Cancel traversal and fd cleanup

`cancel()` walks **down** the active spine depth-first: `forward.cancel()` on
the child before local teardown on the parent. That stops outstanding IO at the
leaf before retiring links above it.

On the **leaf** unwind only (no `cancel_forward` at cancel entry), when the
node still has a `chain_parent`, call `advance(exception=CancelledError())`
**before** `cancel_hook` and `_set_cancelled()`. That bubbles up to advance
hooks such as `chained_fdclose_link`, which close captured sockets on error
paths. One leaf bubble is sufficient: `advance()` walks the full parent chain.
The root finishes with `cancelled=True`.

Order on each node during unwind:

1. `forward.cancel()` — recurse to the active child (already completed on the
   return path for non-leaf nodes).
2. At leaf only: `advance(CancelledError)` when chained.
3. `cancel_hook` — backend teardown (uring cancel, selector deregistration).
4. `_set_cancelled()` — terminal fallback when the hook did not finish the leg.

Intermediate nodes do **not** call `advance()`; the leaf bubble already
reached the root hook.

## Continuous callback composition

Long-lived proactor operations (`accept_many`, `recv_many`, `poll_many`, …)
emit results through a single result callback (or `callback_factory(parent)` when
composition needs the parent `ContinuousOperation`). Nested work started from
that callback — accept-time pre-read, echo sends in tests, and similar — is
**not** chained like `operation_chaining`; helpers live in
`continuous_callbacks.py`.

### Sub-operations: cancel propagation only

`ContinuousOperation._active_suboperations` exists so `cancel()` can reach
children submitted from result callbacks. It is **not** a drain barrier.

| Parent event | Attached children |
|--------------|-------------------|
| Normal finish (EOF, final multishot CQE, …) | Keep running; completion handlers may still run |
| Error finish (`_finish(exception=…)`) | Keep running — e.g. `ECONNRESET` on the parent socket must not cancel accepts/recvs already started from earlier callbacks |
| `cancel()` | Snapshot the set and `cancel()` each child |

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

1. **`wait_operation` on `IOManager`** — lives on `ProactorIOManager`; blocks via
   `ThreadsafeEvent.swait()` in the current tealet. `ProactorFile` depends on the
   `OperationWaiter` protocol.
2. **`BasicScheduler.io`** — property raises `RuntimeError`, not `None`.
3. **`AsyncProactorScheduler`** — shares the same `ProactorIOManager` instance as
   the sync proactor core on a given scheduler object.
4. **Stream helpers** — module-level only (`open_connection`, `start_server`);
   optional `scheduler=` for callers outside a running driver turn.

## Open follow-ups

- Implement `SelectorIOManager` and wire `SelectorScheduler.io` when selector
  blocking IO should share the same capability gate as proactor schedulers.
- `SocketIO`, `PollIO`, and `FileIO` entry protocols are implemented; a future
  `SelectorIOManager` could implement the same slices for selector schedulers.
- Rename `ProactorFile` and review proactor-specific names in other protocol
  signatures (`RecvBufferPool`, and similar). `IOFile` and `ServerIO` are done.
- `StreamServer.serve_forever()` sugar (implemented); signal handling stays in
  `Runner`, not the server object.

## References

- `packages/tealetio/src/tealetio/io_manager.py` — `ProactorIOManager`
- `packages/tealetio/src/tealetio/operation_chaining.py` — chain factories and link handlers
- `packages/tealetio/src/tealetio/continuous_callbacks.py` — continuous result-callback composition
- `packages/tealetio/src/tealetio/proactor.py` — `Proactor`, `ProactorScheduler`
- `packages/tealetio/src/tealetio/files.py` — `ProactorFile`, `OperationWaiter`
- `packages/tealetio/src/tealetio/streams.py` — streams API
- `packages/tealetio/docs/PYTHON_API.md` — user-facing API
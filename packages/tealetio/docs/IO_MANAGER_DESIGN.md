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

Delivery handlers run on the worker thread when a backend completion arrives.
They start the next leg (`proactor.connect`, `proactor.send`) and propagate
backend failures via `operation.advance(exception=…)`.

Child successes and errors bubble through `Operation.advance()`, which walks the
parent chain until it finds an advance hook. Hooks own link-local work: close a
created socket on error, shape the root result on success, then call
`advance_continue()`.

Chain factories take `proactor` as their first argument and close over it in
handlers. The proactor is not threaded through `advance()`.

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
- `packages/tealetio/src/tealetio/proactor.py` — `Proactor`, `ProactorScheduler`
- `packages/tealetio/src/tealetio/files.py` — `ProactorFile`, `OperationWaiter`
- `packages/tealetio/src/tealetio/streams.py` — streams API
- `packages/tealetio/docs/PYTHON_API.md` — user-facing API
# IO manager design (draft)

Status: Design note — not implemented. Intended for a follow-up PR after the
streams proof-of-concept branch merges.

## Motivation

`asyncio` folds scheduling, blocking socket helpers, file IO, DNS, transports,
and more into a single `loop` object. `tealetio` already does better at the
*submission* layer (`Proactor` with `SelectorProactor` and `UringProactor`), but
`BaseScheduler` and `ProactorScheduler` still expose a wide IO surface:

- `sock_recv`, `sock_connect`, `sock_create`, and related socket helpers
- `poll` / `poll_many`
- positioned file helpers (`open`, `read`, `write`, …)
- stream connect/server instance methods (`open_connection`, `start_server`, …)

Scheduling concerns (tasks, timers, `call_soon`, `spawn`, executor-backed DNS)
are mixed at the type level with IO orchestration stubs on `BaseScheduler`, even
when a scheduler has no IO backend (`BasicScheduler`).

The streams POC (`tealetio.streams`) currently depends on a concrete
`ProactorScheduler` type. That coupling is workable for a POC but repeats the
asyncio pattern of “the scheduler is the IO entry point.”

**Goal:** make IO capability an explicit, composable object bound to the
scheduler’s proactor backend, without breaking the public scheduler API during
migration.

## Current layering (implicit)

```text
tealetio.streams          open_connection, start_server, StreamReader/Writer
        │
        ▼
ProactorScheduler         wait_operation, sock_*, poll, open, stream helpers
        │
        ▼
Proactor (Protocol)       recv/send/accept/… → Operation[T]
        │
        ├── SelectorProactor
        └── UringProactor
```

Parallel path: `SelectorScheduler` + `SelectorMixin` provides fd readiness
(`add_reader` / `wait_readable`) for asyncio coexistence. That is a different
concern from the blocking `sock_*` facade used by `streams`.

## Proposed layering (explicit)

```text
Scheduler                 tasks, timers, call_soon, spawn, executor, DNS, driver
        │
        │ owns
        ▼
IOManager                 wait_operation + blocking IO facade over Proactor
        │
        │ wraps
        ▼
Proactor                  async submission → Operation[T]
```

### `IOManager` (concrete) vs protocols (interfaces)

Prefer **composition** over subclassing `Scheduler` per IO backend.

```python
class ProactorIOManager:
    def __init__(self, proactor: Proactor, *, wait: WaitFn) -> None: ...

    def wait_operation(self, op: Operation[T]) -> T: ...
    def sock_recv(self, sock: socket.socket, n: int) -> bytes: ...
    # … remaining sock_*, poll, file helpers
```

`ProactorScheduler` would hold `self._io: ProactorIOManager` and forward existing
methods for compatibility:

```python
@property
def io(self) -> IOManager: ...

def sock_recv(self, sock, n):
    return self._io.sock_recv(sock, n)
```

**Why composition?**

- Test IO without a full scheduler (mock `wait` + fake `Proactor`).
- Swap `SelectorProactor` vs `UringProactor` without touching task logic.
- `streams` and other callers depend on `scheduler.io` or a protocol, not
  `isinstance(scheduler, ProactorScheduler)`.
- Avoid growing `BaseScheduler` with more `NotImplementedError` stubs.

Mixin (`ProactorIOMixin`) is acceptable and matches existing `SelectorMixin`
style, but composition gives a clearer capability object (`scheduler.io`) and
simpler typing.

### Protocol split (narrow interfaces)

Avoid one giant `IOManager` protocol. Suggested slices:

| Protocol | Responsibility |
|----------|----------------|
| `Proactor` | Already exists — submit IO, return `Operation` |
| `OperationWaiter` | `wait_operation(op) -> T` (needs scheduler driver) |
| `SocketIO` | `sock_recv`, `sock_connect`, `sock_create`, … |
| `PollIO` | `poll`, `poll_many` |
| `FileIO` | positioned `open` / read / write |
| `StreamIO` (optional) | `open_connection`, `start_server` — may remain module-level helpers over `SocketIO` |

`ProactorIOManager` implements the blocking facade protocols by delegating to one
`Proactor` instance.

Module helpers:

```python
def open_connection(*, addr=..., path=..., io: SocketIO | None = None):
    io = io or get_running_scheduler().io
```

This replaces patterns such as `_running_proactor_scheduler()` with a capability
check on `scheduler.io`.

### Selector vs proactor *manager* flavours

Do **not** force a single inheritance tree for all IO styles.

- **`ProactorIOManager`** — one class; backend is any `Proactor` implementation.
  This is the primary path for `tealetio.streams` and blocking socket/file IO.
- **Selector readiness** — keep `SelectorMixin` / `add_reader` as the asyncio
  guest-loop seam. Optional `SelectorPollManager` only if fd-callback registration
  needs to be separated from `ProactorIOManager`; blocking sockets on the sync
  path still go through `SelectorProactor` inside `ProactorIOManager`.

`SelectorProactor` is a proactor *implementation*, not a separate manager type.

## What stays on `Scheduler`

Keep scheduling and “scheduler services” on `BaseScheduler` / concrete drivers:

- `spawn`, `call_soon`, timers, `run_forever` / `run_until_complete`
- `run_in_executor`, `getaddrinfo`, `ensure_resolved`
- `get_running_scheduler()` / thread-local binding (asyncio keeps this on the
  loop too)
- task queues, cancellation, `all_tasks`, and related introspection

## What moves to `IOManager`

- `wait_operation`
- all `sock_*` helpers and `create_recv_buffer_pool` / `sock_recv_iter`
- `poll` / `poll_many`
- positioned file helpers currently on `ProactorScheduler`
- optionally stream instance methods (`open_connection`, `start_server`) — these
  may remain on `streams` module + thin scheduler forwards

`StreamServer` lifecycle (`close`, `wait_closed`, handler counting) can stay in
`streams`; it needs `IOManager` (for `accept_many`), `spawn`, and
`call_soon_threadsafe`, not a fatter scheduler type.

## Comparison with asyncio

| Concern | asyncio | tealetio today | tealetio target |
|---------|---------|----------------|-----------------|
| IO submission | loop → proactor internally | `Proactor` protocol | unchanged |
| Blocking facade | `loop.sock_recv`, … | `ProactorScheduler.sock_*` | `scheduler.io.sock_*` |
| High-level streams | `asyncio.start_server` | `tealetio.streams` | depends on `SocketIO` / `io` |
| Capability gate | `get_running_loop()` | `get_running_scheduler()` | `get_running_scheduler().io` |

Asyncio cannot introduce `loop.io` without breaking callers. Tealetio can while
the API is still young.

## Streams POC alignment

The streams branch introduces:

- unified `open_connection(addr=… | path=…)` and `start_server(addr=… | path=…)`
- `StreamServer` context manager, `wait_closed()`, and handler-count shutdown
- `ensure_resolved` for TCP connects; module helpers narrowed to proactor
  schedulers for typing

None of that blocks the IO manager refactor. After the POC merges, a follow-up PR
should:

1. Introduce `ProactorIOManager` internally.
2. Add `scheduler.io` and forward existing `sock_*` / `wait_operation` methods.
3. Point `streams` at `scheduler.io` (or `SocketIO` protocol) instead of
   `ProactorScheduler`.
4. Trim `BaseScheduler` IO stubs in favour of `Optional` io capability or a
   `SupportsIO` marker protocol.
5. Optionally add `StreamServer.serve_forever()` as sugar over `run_forever()` +
   `close()` / `wait_closed()` (signal handling stays in `Runner`, not the
   server object — same split as asyncio `serve_forever` vs `asyncio.run`).

## Pragmatic migration (low churn)

| Phase | Work | User-visible break |
|-------|------|-------------------|
| 1 | `ProactorIOManager` + `scheduler.io` property; forward existing methods | none |
| 2 | `streams` uses `get_running_scheduler().io`; ty/protocol types | none |
| 3 | Document `scheduler.io`; deprecate direct `isinstance(..., ProactorScheduler)` in new code | docs only |
| 4 | Optional: move stream methods off scheduler surface to module-only | minor, later |

## Naming notes

- **`IOManager`** — concrete class managing blocking IO for one scheduler
  instance.
- **`SchedulerIO`** — alternative name emphasising the blocking facade rather than
  backend ownership.
- Avoid **`IOLoop`** — too asyncio-shaped.

Protocols: `SocketIO`, `FileIO`, `PollIO`; backend remains `Proactor`.

## Open questions

1. Should `wait_operation` live on `IOManager` or stay on `Scheduler` and be
   passed into `ProactorIOManager` as a callback? (Callback avoids a circular
   reference and keeps the driver on the scheduler.)
2. Should `BasicScheduler` expose `io: None` or omit the attribute entirely?
3. Does `AsyncProactorScheduler` share the same `ProactorIOManager` instance as
   the sync path, or a thin async variant that uses `await` instead of `swait`?
4. Should stream helpers remain module-level only, or also as methods on
   `IOManager` for asyncio-shaped symmetry?

## References

- `packages/tealetio/src/tealetio/proactor.py` — `Proactor` protocol,
  `ProactorScheduler`
- `packages/tealetio/src/tealetio/streams.py` — streams POC
- `packages/tealetio/docs/PYTHON_API.md` — user-facing streams section
- `packages/tealetio/docs/SCHEDULER_RUNTIME_API_SPEC.md` — scheduler runtime spec
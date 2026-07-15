# IO manager callback composition

Continuous proactor work is composed in `ProactorIOManager` and small helpers in
`continuous_callbacks.py`, not inside the proactor. One-shot multi-leg socket work
(create → connect → send) uses `IOWaitGroup` in the same layer; see
`IO_MANAGER_DESIGN.md`.

The proactor submits `accept_many`, `recv_many`, `poll_many`, and similar
operations and emits bare results through each operation's `result_callback`.
Tuple shaping, accept-time pre-read, scheduler-thread marshalling, and stream
pair construction live on `scheduler.io`.

## Two operation kinds

| Kind | Proactor completion path | Composition hook |
|------|--------------------------|------------------|
| One-shot (`connect`, `create_socket`, …) | `operation.deliver(proactor, result=…, exception=…)` | `ProactorIOManager` advance handlers via `IOWaitGroup` |
| Continuous (`accept_many`, `recv_many`, …) | `operation._emit_result(chunk)` | `result_callback` on the `ContinuousOperation`; io_manager wraps or extends it |

For one-shot ops the proactor calls `deliver()`, which finishes the operation
immediately. Multi-leg blocking helpers compose separate operations in
`io_waiter.IOWaitGroup` instead of delivery handlers on a single root operation.

For continuous ops the proactor requires a `result_callback` and emits chunks
until the operation finishes or errors. `ProactorIOManager` is the usual place
to adapt that callback (marshal onto the scheduler thread, attach accept-time
`recv`, build stream pairs, and similar).

## Proactor surface (thin)

`accept_many` and `recv_many` take a single `callback` argument. The proactor
does not know about tuple delivery shapes, nested `recv`, or thread affinity.

| Continuous op | Proactor delivers |
|---------------|-------------------|
| `accept_many` | accepted `socket.socket` per chunk |
| `recv_many` | `(bytes, is_eof)` chunks |
| `poll_many` | ready mask per chunk |

Nested work started from a result callback (for example accept-time `recv`) is
**independent** of the parent `ContinuousOperation`. Cancelling the parent does
not automatically cancel per-accept `recv` ops; each layer chooses its own
disposition (see below).

## `ProactorIOManager` continuous helpers

| Entry point | Composition |
|-------------|---------------|
| `accept_many(sock, callback, recv_size=…)` | worker mutates each leg (optional accept-time `recv`), then posts one merged `MultishotDelivery` per leg onto the scheduler; `TerminalReorderBuffer`, `deliver_wrapped`, user `callback`, and `finish_operation` run on the scheduler thread |
| `accept_many_streams(…)` | worker accepts, opens streams and arms ``recv_many`` there, then posts `(reader, writer)` onto the scheduler; user `callback` and `finish_operation` run on the scheduler thread |
| `poll_many(fd, mask, callback)` | worker posts each delivery unchanged; `TerminalReorderBuffer`, user `callback`, and `finish_operation` on the scheduler thread inside an `IOWaiter` |
| `sock_recv_iter` | `RecvIterBuffer`: `marshal_to_scheduler` + `ReorderBuffer` over `proactor.recv_many` chunks (not composed through `accept_many`) |

Worker-thread accept composition mutates the proactor delivery before the
scheduler sees it. Reorder, `finish_operation`, and user callbacks always run on
the scheduler thread via `_thread_reorder_helper` (one `call_soon_threadsafe` hop
per posted leg, with `immediate=True` when already on the owner thread).

Accept-time pre-read wiring (when `recv_size` is set):

```text
proactor.accept_many(sock, on_worker_delivery)     # worker thread
        │
        ▼  each accept (socket, index, more, …)
proactor.recv(conn, recv_size)                     # worker; independent one-shot Operation
        │
        ▼  recv done callback (worker)
post merged MultishotDelivery(index unchanged, value=(conn, data) or recv error)
        │
        ▼  marshal (one hop)
TerminalReorderBuffer → deliver_wrapped → user callback → finish_operation   # scheduler
```

Without `recv_size`, the worker posts `(conn, None, None)` in `value` after the
bare socket accept. Terminals (cancel, EOF, transport errors) post through
unchanged; reorder and `finish_continuous_delivery` still run on the scheduler.

`recv_op.add_done_callback(on_recv_complete)` registers preread completion; there
is no parent/child link on `Operation`. A cancelled recv closes the connection
with `abortive_close` and does not post to the scheduler or invoke the user
callback.

Helpers in `continuous_callbacks.py` support this layer:

- `ReorderBuffer` / `TerminalReorderBuffer` — index-ordered delivery on the scheduler thread (`RecvIterBuffer` uses full reorder; accept/poll defer only out-of-order terminals)
- `finish_continuous_delivery` — call `finish_operation` on terminal deliveries
- `marshal_to_scheduler` — one `call_soon_threadsafe` hop per worker-thread delivery (`RecvIterBuffer` and `start_server` paths); `ProactorIOManager._thread_reorder_helper` uses the same `immediate=True` marshal internally
- `normalize_accept_recv_size` — cap and validate `recv_size`
- `finalize_accept_recv_error` — optional `on_recv_error` hook, then close
- `wrap_accept_delivery` — adapt tuple delivery to bare-socket proactor callbacks

## Delivery disposition (application layer)

Late or unwanted deliveries are handled by the **application**, not by
`Operation` suboperation tracking or proactor callback factories.

A continuous op may finish (cancel, error, or natural EOF) while result
callbacks or nested work they started are still in flight. That is expected:
ending the accept **stream** does not mean all per-connection work has completed.

Callers choose how to treat deliveries that arrive after shutdown or after they
have lost interest:

| Disposition | Example |
|-------------|---------|
| **Discard** | close the socket or stream and return |
| **Ignore** | drop the delivery without further work |
| **Handle** | process anyway (for example drain already-accepted clients) |

`StreamServer` discards late accepts after `close()`:

- `on_accept` checks `_closed`; if set, it closes the writer and returns without
  spawning a handler.
- ``StreamServer._on_accept`` discards late deliveries when ``_closed`` and spawns
  the handler tealet directly (no deferred ``call_soon``). ``handler_eager_start``
  (default true) passes ``eager_start`` through to ``spawn()`` so the handler can
  begin on the same scheduler turn when the runtime allows it.
- `close()` synchronously cancels the accept-loop tealet; it does not close
  listening sockets. The accept-loop tealet wraps its main loop in ``try``/``finally``
  so ``CancelledError`` runs cleanup that sets `_closed` and closes listeners.
  In-flight handler tealets keep running until they finish.

The io_manager posts merged accept legs onto the scheduler thread (after worker
mutation when applicable) but does not enforce server shutdown policy —
`StreamServer` (or any custom `accept_many` callback) implements that.

Similarly, `IOWaitGroup` discards late `finish()` results after an interrupted
`wait()` sets `_closed` (for example `abortive_close` on a socket). That is
waiter-level disposition for one-shot composition, not continuous accept policy.

## Cancel vs in-flight completion

`Proactor.cancel(operation)` always races backend worker threads. Completions
arrive asynchronously; a waiter or scheduler task may cancel the same operation
while a CQE is already in flight.

### Current behaviour

Cancellation is backend-specific teardown (drop deferred resubmits, submit async
ring cancel or `poll_remove`, deregister selector interest, `break_wait()`, and
similar).

On **selector / emulated** paths, `ProactorBase._terminalise_cancelled()` runs
immediately after teardown is requested. Continuous ops emit a terminal
`MultishotDelivery` with `CancelledError` and `index=None` (best-effort: the
reorder buffer may deliver cancel before straggler legs still in flight).

On **uring** today, `UringProactor.cancel()` submits `submit_cancel` or
`poll_remove`, then calls the same synchronous `_terminalise_cancelled()` on
the target. The ring cancel completion only finishes the separate cancel
`Operation[None]`; it does not drive the continuous result callback. Late
multishot CQEs after the target is already `done()` are dropped in
`_deliver_uring_completion`.

Callers waiting on `IOWaiter.wait()` observe either a normal result or
`CancelledError`. Exceptional `wait()` exit routes through
`ProactorIOManager._cancel_operation(...).forget()` so teardown legs are not
blocked on.

For `IOWaitGroup`, exceptional `wait()` exit cancels all tracked legs; see
`IO_MANAGER_DESIGN.md`.

### Planned: uring completion-driven cancel

Defer continuous-op terminalisation on the uring path: `cancel()` should submit
teardown and return, but **not** call `_terminalise_cancelled()` on the target
immediately.

Instead, emit the cancel terminal from ring completions:

- target multishot handle: terminal CQE with `res < 0` (often `ECANCELED`),
  mapped to `CancelledError` with `index=None`;
- `poll_remove` completion for multishot `poll_many`;
- cancel-op completion as a fallback when the target is still active but no
  further target CQE arrives.

This matches io_uring semantics (cancel and success can race) and should
simplify `UringProactor.cancel()` by removing the synchronous front-run. Selector
and emulated backends keep immediate `_terminalise_cancelled()`.

Deferred resubmits and never-submitted legs still need a local terminal path
when the ring has nothing to complete.

## Module layout

| Module | Responsibility |
|--------|----------------|
| `operations.py` | `Operation`, `ContinuousOperation`, `ContinuousStepResult` |
| `io_manager.py` | `ProactorIOManager` — continuous and one-shot composition |
| `io_waiter.py` | `IOWaiter`, `IOWaitGroup` — blocking wait and one-shot multi-leg composition |
| `continuous_callbacks.py` | Small helpers used by `ProactorIOManager` accept paths |
| `proactor.py` | Submit ops; continuous backends call `_emit_result` / `_finish` |

## References

- `packages/tealetio/src/tealetio/io_manager.py`
- `packages/tealetio/src/tealetio/io_waiter.py`
- `packages/tealetio/src/tealetio/continuous_callbacks.py`
- `packages/tealetio/src/tealetio/operations.py`
- `packages/tealetio/src/tealetio/streams/server.py` — `StreamServer` late-delivery discard
- `packages/tealetio/docs/IO_MANAGER_DESIGN.md`
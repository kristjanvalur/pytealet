# IO manager callback composition

Continuous proactor work is composed in `ProactorIOManager` and small helpers in
`continuous_callbacks.py`, not inside the proactor. One-shot multi-leg socket work
(direct create, then connect → send) uses `IOWaitGroup` in the same layer; see
`IO_MANAGER_DESIGN.md`.

Before arming continuous `accept_many` / `recv_many`, the io_manager may drain
ready connections or data with non-blocking syscalls and continue numbering via
`base_sequence` (**Eager non-blocking first** in `IO_MANAGER_DESIGN.md`). The
proactor path below is the fallback when that drain would block, plus the
long-lived continuous stream after the drain.

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
| `accept_many(sock, callback, recv_size=…)` | worker mutates each leg (optional accept-time `recv`), then posts one merged `MultishotDelivery` per leg onto the scheduler; `ReorderBuffer`, `deliver_wrapped`, user `callback`, and `finish_operation` run on the scheduler thread |
| `accept_many_streams(…)` | worker accepts, opens streams and arms ``recv_many`` there, then posts `(reader, writer)` onto the scheduler; user `callback` and `finish_operation` run on the scheduler thread |
| `poll_many(fd, mask, callback)` | worker posts each delivery unchanged; `ReorderBuffer`, user `callback`, and `finish_operation` on the scheduler thread inside an `IOWaiter` (callback exceptions still finish terminal legs in `finally`) |
| `_recv_many` (internal) | thin wrap: eager non-blocking `recv` drain, then `proactor.recv_many` with the same `callback`; returns `ContinuousOperation` (no marshal/reorder); intermediate eager may use `operation=None`; pure-eager terminal uses a synthetic done op |
| `sock_recv_iter` | `RecvIterBuffer`: `marshal_to_scheduler` + `ReorderBuffer`; starts via `_recv_many`, cancels via proactor |

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
post merged MultishotDelivery(index unchanged,
    value=(conn, data, None) | (conn, None, recv_error))   # recv_error may be ECANCELED on timeout cancel
        │
        ▼  marshal (one hop)
ReorderBuffer → deliver_wrapped → user callback (if no recv_error)
        │                                    └─ try/finally: finish_operation on terminal legs
        └─ finalize_accept_recv_error when recv_error set (scheduler; no user callback)
```

Without `recv_size`, the worker posts `(conn, None, None)` in `value` after the
bare socket accept. Stream terminals (cancel, EOF, transport errors on the
continuous op) post through unchanged; reorder and `finish_continuous_delivery`
still run on the scheduler. Transport errors finish the operation then re-raise;
user accept callback exceptions propagate to the scheduler exception handler but
terminal legs still call `finish_continuous_delivery` in a `finally` block so
`IOWaiter.wait()` does not hang.

`recv_op.add_done_callback(on_recv_complete)` registers preread completion; there
is no parent/child link on `Operation`. Preread failures (including timeout
timeout cancel as ``OSError(ECANCELED)``) post `(conn, None, exc)` like other recv errors;
`finalize_accept_recv_error` closes the socket on the scheduler thread and does
not invoke the user accept callback unless `on_recv_error` is provided.

Helpers in `continuous_callbacks.py` support this layer:

- `ReorderBuffer` — scheduler-thread delivery ordering in strict index order (accept, poll, and `RecvIterBuffer` / `recv_many` chunks)
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
  so ``CancelledError`` or ``OSError(errno.ECANCELED)`` from IO cancel runs cleanup
  that sets `_closed` and closes listeners. In-flight handler tealets keep running
  until they finish.

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
ring cancel or `poll_remove`, deregister selector interest, scheduler
`wake_wait()`, and similar).

IO cancellation is distinct from task cancellation. Proactor cancel completes
operations with ``OSError(errno.ECANCELED)`` (see ``io_cancellation_error()``).
``is_cancellation_delivery()`` / ``is_io_cancellation()`` let ``io_manager``
treat that terminal as "no further chunks" rather than a transport failure to
surface to callers. ``CancelledError`` remains for ``Task.cancel()`` only.

On **selector / emulated** paths, `ProactorBase._terminalise_cancelled()` runs
immediately after teardown is requested. Continuous ops emit a terminal
`MultishotDelivery` with ``OSError(ECANCELED)`` and `index=None` (best-effort:
the reorder buffer may deliver cancel before straggler legs still in flight).

On **uring**, armed recv/accept (and similar) legs use `submit_cancel`; the
target finishes only from its own CQE, usually ``OSError(ECANCELED)``. The
cancel-op CQE completes only the teardown ``Operation[None]`` so callers can
``iomanager.cancel(...).wait()`` if they want; it does not terminalise or
otherwise complete the target. A successful cancel SQE post is trusted: there is
no synthetic target fallback if the ack arrives before the target CQE. Cancel may
lose the race to an in-flight success CQE; the target may never surface
``ECANCELED`` if the kernel already completed it.

The teardown ``wait()`` can surface cancel-ack outcome: ``res == 0`` on the
cancel CQE completes with ``None``; a negative ``res`` delivers ``OSError`` on
the teardown op (for example when the target already finished). That reports
whether the cancel *request* was accepted, not whether the target IO has stopped
yet — the target CQE remains authoritative for the original operation.

On uring multishot ``recv_many`` / ``accept_many``, a target ``-ECANCELED`` CQE
uses the leg index from ``completion.sequence``. Unlike selector/emulated
``index=None`` cancel terminals, uring cancel does not jump ahead of multileg
segments already in the reorder buffer; cancel is best-effort and may trail
straggler legs.

Multishot ``poll_many`` uses ``submit_poll_remove()`` (not ``submit_cancel``).
Once the stop posts successfully, the continuous poll op is terminalised
immediately (same as selector). The ``COMPLETION_KIND_POLL_REMOVE`` CQE only
finishes the teardown waitable; it does not re-terminalise the target. In-flight
poll CQEs may still race that terminal — cancel always races completions, and
we do not add extra gates just to shrink the window. One-shot ``poll_many``
fallback stops locally without ring cancel on the pending poll SQE.

This matches io_uring semantics for armed recv/accept legs: cancel and success
can race; a successful target CQE that arrives before teardown settles completes
normally. Selector and emulated backends keep immediate
`_terminalise_cancelled()`. Deferred resubmits and never-submitted legs still
terminalise locally when the ring has nothing to complete.

Late multishot CQEs still route through `entry.complete()` after the consumer
has marked the operation `done()`. `ContinuousOperation._emit_delivery` skips
the callback when already finished; out-of-order terminal ordering is handled on
the scheduler thread by `ReorderBuffer`, not in the uring completion worker.

Callers waiting on `IOWaiter.wait()` observe either a normal result or
``OSError(errno.ECANCELED)`` from proactor cancel (compare with
``is_io_cancellation()``; ``CancelledError`` remains for ``Task.cancel()``
only). Exceptional `wait()` exit routes through
`ProactorIOManager._cancel_operation(...).forget()` so teardown legs are not
blocked on.

For `IOWaitGroup`, exceptional `wait()` exit cancels all tracked legs; see
`IO_MANAGER_DESIGN.md`.

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
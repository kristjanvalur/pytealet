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
| `accept_many(sock, callback, recv_size=…)` | optional accept-time `recv` via `_accept_many_read_on_conn`; deliveries marshalled with `call_soon_threadsafe` before `callback((conn, initial_data))` |
| `accept_many_streams(…)` | `_open_streams` on the accept delivery thread (``recv_many`` starts there); user `callback((reader, writer))` marshalled with `call_soon_threadsafe` |
| `poll_many(fd, mask, callback)` | forwards to `proactor.poll_many` inside an `IOWaiter` |
| `sock_recv_iter` | blocking iterator over `proactor.recv_many` chunks |

Accept-time pre-read wiring (when `recv_size` is set):

```text
proactor.accept_many(sock, on_conn)
        │
        ▼  each accept
proactor.recv(conn, recv_size)     # independent one-shot Operation
        │
        ▼  recv done callback
marshal → deliver_wrapped → user callback
```

`on_conn` registers `recv_op.add_done_callback(on_recv_complete)`; there is no
parent/child link on `Operation`. A cancelled recv closes the connection with
`abortive_close` and does not invoke the user callback.

Helpers in `continuous_callbacks.py` support this layer:

- `normalize_accept_recv_size` — cap and validate `recv_size`
- `finalize_accept_recv_error` — optional `on_recv_error` hook, then close
- `wrap_accept_delivery` — adapt tuple delivery to bare-socket proactor callbacks
- `marshal_to_scheduler` — thread affinity for callbacks that must run on the
  scheduler thread (used by `start_server` paths via `_marshal_accept_callback`)

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
- `_dispatch_client` and `_dispatch_streams` perform the same check before
  tracking handler ``Task``s.
- `close()` synchronously cancels the accept-loop tealet; it does not close
  listening sockets. The accept-loop tealet wraps its main loop in ``try``/``finally``
  so ``CancelledError`` runs cleanup that sets `_closed` and closes listeners.
  In-flight handler tealets keep running until they finish.

The io_manager marshals accept deliveries onto the scheduler thread but does not
enforce server shutdown policy — `StreamServer` (or any custom `accept_many`
callback) implements that.

Similarly, `IOWaitGroup` discards late `finish()` results after an interrupted
`wait()` sets `_closed` (for example `abortive_close` on a socket). That is
waiter-level disposition for one-shot composition, not continuous accept policy.

## Cancel vs in-flight completion

`Proactor.cancel(operation)` always races backend worker threads. Completions
arrive asynchronously; a waiter or scheduler task may cancel the same operation
while a CQE is already in flight.

Cancellation is backend-specific teardown only (drop deferred resubmits, submit
async ring cancel or poll_remove, deregister selector interest, `break_wait()`,
and similar). The proactor terminalises the target operation immediately after
submitting teardown; it does not wait for the ring cancel CQE before marking the
target cancelled.

A late `deliver()` may therefore still succeed after cancel is submitted. That is
expected: whichever path reaches `_finish` first wins. Callers waiting on
`IOWaiter.wait()` observe either a normal result or `CancelledError`, not an
ambiguous in-between state. Exceptional `wait()` exit routes through
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
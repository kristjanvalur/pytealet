# IO manager callback composition

Continuous proactor work is composed in `ProactorIOManager` and small helpers in
`continuous_callbacks.py`, not inside the proactor. One-shot multi-leg socket work
(direct create, then connect → send) uses `IOWaitGroup` in the same layer; see
`IO_MANAGER_DESIGN.md`.

Continuous `accept_many` / `recv_many` always submit to the proactor (no
manager-side drain; see **Eager non-blocking first** in `IO_MANAGER_DESIGN.md`).
The proactor path below is the long-lived continuous stream.

The proactor submits `accept_many`, `recv_many`, `poll_many`, and similar
operations and emits bare results through each operation's `result_callback`.
Tuple shaping, accept-time pre-read, scheduler-thread marshalling, and stream
pair construction live on `scheduler.io`.

## Two operation kinds

| Kind | Proactor completion path | Composition hook |
|------|--------------------------|------------------|
| One-shot (`connect`, `create_socket`, …) | `operation.deliver(proactor, result=…, exception=…)` | `ProactorIOManager` advance handlers via `IOWaitGroup` |
| Continuous (`accept_many`, `recv_many`, `poll_many`) | shaper → user `callback(MultishotDelivery)` | io_manager wraps or extends that callback; poll returns `IOHandle` |

For one-shot ops the proactor calls `deliver()`, which finishes the operation
immediately. Multi-leg blocking helpers compose separate operations in
`io_waiter.IOWaitGroup` instead of delivery handlers on a single root operation.

For continuous ops the proactor requires a submit-time `callback` and emits
chunks until the stream ends or errors. `recv_many` / `accept_many` return
opaque `OpHandle`s, not waitables. `ProactorIOManager` is the usual place to adapt
that callback (marshal onto the scheduler thread, attach accept-time `recv`,
build stream pairs, and similar) and, for accept, to wrap stream-end in an
`IOWaiter`.

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
| `accept_many(sock, callback, recv_size=…)` | worker mutates each leg (optional accept-time `recv`), then posts one merged `MultishotDelivery` per leg onto the scheduler; `CountFinalizer` delivers immediately (completion/marshal order, not index order), runs `deliver_wrapped` / user `callback`, and settles the manager `IOWaiter` |
| `accept_many_streams(…)` | worker accepts, opens streams and arms ``recv_many`` there, then posts `(reader, writer)` onto the scheduler; `CountFinalizer` delivers immediately; user `callback` and waiter finish run on the scheduler thread |
| `poll_many(fd, mask, callback)` | returns `IOHandle` (not a waitable); worker posts each delivery unchanged; `ReorderBuffer` and user `callback` on the scheduler thread; terminal `!MORE` marks the handle closed; `handle.close()` → `stop_poll` |
| `_recv_many` (internal) | thin wrap of `proactor.recv_many` with the same `callback`; returns an opaque `OpHandle` (not waitable; no marshal/reorder, no manager-side drain) |
| `sock_recv_iter` | `RecvIterBuffer`: `marshal_to_scheduler` + `ReorderBuffer`; starts via `proactor.recv_many`, cancels via `cancel_nowait` |

Worker-thread accept composition mutates the proactor delivery before the
scheduler sees it. `CountFinalizer`, `finish_operation`, and user callbacks always
run on the scheduler thread via `_thread_count_finalizer_helper` (one
`call_soon_threadsafe` hop per posted leg, with `immediate=True` when already on
the owner thread). Poll and `RecvIterBuffer` still marshal through
`_thread_reorder_helper` / `ReorderBuffer`.

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
CountFinalizer → deliver_wrapped → user callback (if no recv_error)
        │
        └─ finalize_accept_recv_error when recv_error set (scheduler; no user callback)
           CountFinalizer settles the IOWaiter: numeric !MORE waits until
           delivered_count == terminal_index - start + 1
```

Without `recv_size`, the worker posts `(conn, None, None)` in `value` after the
bare socket accept. Stream terminals (cancel, EOF, transport errors on the
continuous op) post through unchanged; `CountFinalizer` still runs on the
scheduler and settles the waiter. Accept scheduler callbacks must **not**
call `finish_continuous_delivery`. Stream-end (cancel or accept `OSError`)
is not delivered to the user accept callback: `CountFinalizer` settles the
`IOWaiter` (`wait()` returns `None` or raises). `StreamServer` retries
transient accept errors. User accept
callback exceptions still propagate to the scheduler exception handler; the
helper counts in `finally` so `IOWaiter.wait()` cannot hang.

`recv_op.add_done_callback(on_recv_complete)` registers preread completion; there
is no parent/child link on `Operation`. Preread failures (including timeout
timeout cancel as ``OSError(ECANCELED)``) post `(conn, None, exc)` like other recv errors;
`finalize_accept_recv_error` closes the socket on the scheduler thread and does
not invoke the user accept callback unless `on_recv_error` is provided.

Helpers in `continuous_callbacks.py` support this layer:

- `CountFinalizer` — scheduler-thread accept delivery (immediate, unordered) and count-based waiter settle (`finish` callback; default `finish_operation`)
- `ReorderBuffer` — scheduler-thread delivery ordering in strict index order (`poll_many` and `RecvIterBuffer` / `recv_many` chunks)
- `finish_continuous_delivery` — call `finish_operation` on terminal deliveries (`CountFinalizer` and `ReorderBuffer` paths)
- `marshal_to_scheduler` — one `call_soon_threadsafe` hop per worker-thread delivery (`RecvIterBuffer` and `start_server` paths); `ProactorIOManager._thread_count_finalizer_helper` / `_thread_reorder_helper` use the same `immediate=True` marshal internally
- `normalize_accept_recv_size` — cap and validate `recv_size`
- `finalize_accept_recv_error` — optional `on_recv_error` hook, then close

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
  the handler tealet directly (no deferred ``call_soon``). ``spawn(..., eager_start=False)``
  is explicit so a scheduler-wide eager task factory cannot run the handler on
  the accept/CQE stack; delivery already opened streams and armed ``recv_many``.
  ``handler_eager_start=True`` opts back in.
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

Cancellation is backend-specific teardown (``ASYNC_CANCEL`` / ``stop_poll``
on uring, deregister selector interest, scheduler ``wake_wait()``, and
similar). There is no deferred SQ FIFO.

IO cancellation is distinct from task cancellation. Proactor cancel completes
operations with ``OSError(errno.ECANCELED)`` (see ``io_cancellation_error()``).
``is_io_cancellation()`` lets ``wait()`` / ``StreamServer`` treat
``OSError(ECANCELED)`` as shutdown rather than a transport failure.
``is_cancellation_delivery()`` is the same test on a ``MultishotDelivery``.
``CancelledError`` remains for ``Task.cancel()`` only.

On **selector / emulated** paths, `ProactorBase._terminalise_cancelled()` runs
immediately after teardown is requested. Continuous ops emit a terminal
`MultishotDelivery` with ``OSError(ECANCELED)`` at ``operation._next_index``
(oneshot accept: ``base_sequence``; selector ``poll_many``: the next
ordinal after any `more=True` events; selector recv-many:
``SelectorCancelHandle._next_index``). That matches uring `-ECANCELED`
CQEs, which also carry a numeric `completion.sequence`. Uring recv-multi
handles do not store ``_next_index``.

On **uring**, a waitable returned to the client is reverse-armed before the
public prepare method returns. Stream ``send`` constructs the ``send_all``
``Completion``, arms reverse, then ``prepare``s (no SQE until reverse exists).
Oneshot ``poll_many`` first/next-leg still replace reverse under
``_multi_leg_lock``. Cancel is issuer-thread only and never runs
on an incomplete client-held op with reverse still ``None``. Cancel behaviour:

- **`poll_many`**: stop with ``stop_poll()`` (native ``POLL_REMOVE``; oneshot
  abandon + ``ASYNC_CANCEL``; selector local). ``cancel()`` does not check
  handle kind.
- **Stream ``send`` (``send_all``)**: ``ASYNC_CANCEL`` the live reverse. C
  abandon stops further send_all legs. Finish from the target CQE
  (usually ``OSError(ECANCELED)``). Cancel may lose to an in-flight success
  CQE (full drain can still succeed).
- **Other oneshot / continuous multishot**: ``ASYNC_CANCEL`` the live reverse;
  the target finishes only from its own CQE (usually ``OSError(ECANCELED)``).

The cancel-op CQE invokes the ``cancel`` / ``stop_poll`` callback; it does
not terminalise the target. A successful cancel SQE post is trusted: there is
no synthetic target fallback if the ack arrives before the target CQE. Cancel
may lose the race to an in-flight success CQE; the target may never surface
``ECANCELED`` if the kernel already completed it.

``callback(None, None)`` means the cancel *request* was accepted
(``res == 0``); a negative ``res`` is ``callback(None, OSError)`` (for
example when the target already finished). That reports request outcome, not
whether the target IO has stopped — the target CQE remains authoritative.

On uring multishot ``recv_many`` / ``accept_many``, a target ``-ECANCELED`` CQE
uses the leg index from ``completion.sequence``. Selector cancel uses the same
numeric `!MORE` at ``ContinuousOperation._next_index`` or
``SelectorCancelHandle._next_index``. `CountFinalizer` defers settling the accept waiter
until every leg `start .. terminal_index` has been handed off. `recv_many`
still uses `ReorderBuffer`; cancel is best-effort and may trail straggler legs.

**``stop_poll``**: Multishot posts ``prepare_poll_remove()``; the target finishes
from its multishot CQE (typically ``res=-ECANCELED`` with ``!MORE``), delivered
through the result callback / `ReorderBuffer` like other continuous streams. The
``POLL_REMOVE`` CQE invokes the stop oneshot callback (request outcome / race,
not stream quiescence). One-shot ``poll_many`` stop abandons the reverse link,
emits stream-end, and posts ``ASYNC_CANCEL``; the poll CQE clears the sentinel.

This matches io_uring semantics for armed legs: cancel and success can race.
Selector backends keep immediate ``_terminalise_cancelled()`` after deregister.

Late multishot CQEs still route through `entry.complete()` after the consumer
has marked the operation `done()`. The result callback may still run for those
stragglers; consumers and `finish_operation` must tolerate idempotent / late
legs. Out-of-order **accept** terminals are handled on the scheduler thread by
`CountFinalizer` (immediate callback, finish when the delivered count matches
`terminal_index - start + 1`), not in the uring completion worker. `recv_many`
and `poll_many` still use `ReorderBuffer`. `MultishotDelivery.index` is always
the stream ordinal. Backend cancel is a numeric `!MORE`. `RecvIterBuffer.close`
with a live unfinished leg uses `cancel_nowait`; with no live op the buffer
holds a complete prefix or is empty, so close posts `ECANCELED` at
`ReorderBuffer.next_index`. Close while `recv_many` is still installing sets
`_closed` and cancels the returned op if it is still open (or posts the same
sequenced terminal if that op already finished). Accept has no heap: a numeric
cancel finishes when the count matches. Accept stream-end (cancel or
transport error) settles the `IOWaiter` only.

Callers waiting on `IOWaiter.wait()` observe either a normal result or
``OSError(errno.ECANCELED)`` from proactor cancel (compare with
``is_io_cancellation()``; ``CancelledError`` remains for ``Task.cancel()``
only). Exceptional `wait()` exit routes through
`io.cancel_nowait(...)` so teardown legs are not blocked on and no cancel
waitable is allocated. Continuous ``poll_many`` is stopped with
``IOHandle.close()`` (``stop_poll``), not through that cancel path.

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
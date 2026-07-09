# Operation callback composition

Multi-leg proactor work is composed in callback helpers, not inside the
proactor. One-shot operations (`connect`, `create_socket`, …) and continuous
operations (`accept_many`, `recv_many`, …) share the same suboperation model for
cancel propagation; they differ in where the proactor invokes composition hooks.

See `IO_MANAGER_DESIGN.md` for how `ProactorIOManager` wires factories into
`sock_connect` / `sock_create`. This document covers the callback modules and
completion contracts.

## Two operation kinds

| Kind | Proactor completion path | Composition hook |
|------|--------------------------|------------------|
| One-shot (`connect`, `create_socket`, …) | `operation.deliver(proactor, result=…, exception=…)` | Optional `delivery` handler; else `_finish` immediately |
| Continuous (`accept_many`, `recv_many`, …) | `operation._emit_result(chunk)` | `result_callback` or `callback_factory(parent)` |

For one-shot ops the proactor calls `deliver()`. That is the right place to spawn
nested work before the parent completes.

`Operation.complete(result)` / `complete_error(exc)` finish the parent **after**
local composition. Delivery handlers and child `on_complete` callbacks must not
raise without expecting `complete_error` from the suboperation wrapper.

## Shared primitive: `chain_suboperation`

`chain_suboperation(parent, spawn, on_complete)` in `operation_callbacks.py`:

- Spawns the child under `parent._lock` and registers it in
  `_active_suboperations`
- Runs `on_complete` when the child finishes; failures in `on_complete` call
  `parent.complete_error(exc)`
- Returns `False` only when the parent is already `_done` (the attach path
  re-checks the same condition under the lock)

Callers need not finish the parent on `False`. Local cleanup (for example
closing a created socket that will not be returned) is the caller's
responsibility when composition cannot start.

`spawn()` runs while holding `parent._lock`, which serialises attach against
`cancel()` but can defer another thread's `cancel()` until a synchronous backend
path (for example `AF_UNIX` connect) returns from `spawn()`.

## One-shot flows

### Connect + initial send

```text
proactor.connect(sock, addr, operation_factory=connect_initial_send_operation_factory(...))
  └─ connect Operation (root; scheduler waits on this)
       deliver(connect success)
         └─ chain_suboperation → proactor.send(sock, initial)
       on_send_complete:
         └─ send error  → connect_op.complete_error(exc)
         └─ send success → connect_op.complete(None)
```

Empty `initial` payload: `deliver` calls `connect_op.complete(None)` immediately
without spawning a send. When `chain_suboperation` returns `False`, no extra
cleanup is needed on the connect socket (`operation.fileobj`).

### Create → connect → send

```text
proactor.create_socket(..., operation_factory=create_connect_operation_factory(...))
  └─ create Operation (root)
       deliver(create success)
         └─ chain_suboperation → proactor.connect(sock, connect_to)
       on_connect_complete:
         └─ connect error → close(sock), complete_error(exc)
         └─ connect success, no initial → complete(sock)
         └─ connect success, initial → chain_suboperation → send → complete(sock)
```

When `chain_suboperation` returns `False`, close the created socket; `cancel()`
finishes the root.

## Continuous flows

Long-lived operations compose through `continuous_callbacks.py`:

- `accept_read_delivery` — accept-time pre-read via nested `recv` +
  `chain_suboperation`
- `marshal_to_scheduler` — thread affinity for `start_server` delivery
- `wrap_accept_delivery` — bare `(conn, None, None)` tuples without pre-read

The proactor emits bare sockets from `accept_many`; tuple shaping and pre-read
live in the callback/io_manager layer. See `IO_MANAGER_DESIGN.md` (continuous
callback composition).

## Module layout

| Module | Responsibility |
|--------|----------------|
| `operations.py` | `Operation`, `ContinuousOperation`, `OperationFactory` type, suboperation tracking |
| `operation_callbacks.py` | `operation_factory`, `chain_suboperation`, one-shot delivery handlers, named factories |
| `continuous_callbacks.py` | Continuous-specific helpers; imports `chain_suboperation` for nested work |

Named factories (thin `operation_factory(delivery=…)` wrappers):

- `sock_connect(..., initial=…)` → `connect_initial_send_operation_factory`
- `sock_create(..., connect_to=…)` → `create_connect_operation_factory`

## Semantics

| Event | One-shot parent | Continuous parent |
|-------|-----------------|-------------------|
| Parent `complete()` / normal `_finish` | Children keep running | Same |
| Parent error finish | Children keep running | Same |
| Parent `cancel()` | `_finish(cancelled=True)`: backend hook, terminal state, children, callbacks | Same |
| Child completion | `on_complete` may call `parent.complete(…)` | Handlers may run after `parent.done()` when handed off while active |

### Cancel vs in-flight completion

`Operation.cancel()` always races backend worker threads. Proactor completions
arrive asynchronously; the scheduler or a waiter may call `cancel()` on the
same operation while a CQE is already in flight.

`cancel_hook` is **best-effort IO teardown** only (drop deferred resubmits,
submit async cancel, deregister selector interest, `break_wait()`, and similar).
It does not own terminal state. Hooks do not call `_finish()`; `cancel()` routes
through `_finish(cancelled=True)`, which runs the hook and then terminalises
unless `_done` is already set.

A late `deliver()` / `complete()` may therefore still succeed after
`cancel_hook` runs. That is expected: whichever path reaches `_finish` first
wins. Callers waiting on `wait_operation` observe either a normal result or
`CancelledError`, not an ambiguous in-between state.

Only the root one-shot `Operation` is passed to `wait_operation`. Child
operations complete independently; the parent finishes when the final
`on_complete` calls `parent.complete(…)` or `parent.complete_error(…)`.

## References

- `packages/tealetio/src/tealetio/operation_callbacks.py`
- `packages/tealetio/src/tealetio/continuous_callbacks.py`
- `packages/tealetio/src/tealetio/operations.py`
- `packages/tealetio/docs/IO_MANAGER_DESIGN.md`
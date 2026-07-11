# Operation callback composition

Nested proactor work from continuous operations is composed in callback helpers,
not inside the proactor. One-shot multi-leg socket work (create → connect → send)
lives in `ProactorIOManager` via `IOWaitGroup`; see `IO_MANAGER_DESIGN.md`.

This document covers `chain_suboperation`, continuous callback modules, and
completion contracts on `Operation` / `ContinuousOperation`.

## Two operation kinds

| Kind | Proactor completion path | Composition hook |
|------|--------------------------|------------------|
| One-shot (`connect`, `create_socket`, …) | `operation.deliver(proactor, result=…, exception=…)` | `ProactorIOManager` advance handlers via `IOWaitGroup` |
| Continuous (`accept_many`, `recv_many`, …) | `operation._emit_result(chunk)` | `result_callback` or `callback_factory(parent)` |

For one-shot ops the proactor calls `deliver()`, which finishes the operation
immediately. Multi-leg blocking helpers compose separate operations in
`io_waiter.IOWaitGroup` instead of delivery handlers on a single root operation.

`Operation.complete(result)` / `complete_error(exc)` finish a one-shot parent
from a chained suboperation callback. Handlers must not raise without expecting
`complete_error` from the `chain_suboperation` wrapper.

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
path (for example `AF_UNIX` connect) returns from `spawn()`. The done callback is
registered after releasing `parent._lock` so a synchronously completing child
does not deadlock when `on_complete` finishes the parent.

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
| `operations.py` | `Operation`, `ContinuousOperation`, suboperation tracking |
| `io_waiter.py` | `IOWaiter`, `IOWaitGroup` — one-shot multi-leg composition |
| `operation_callbacks.py` | `chain_suboperation` for nested work under continuous parents |
| `continuous_callbacks.py` | Continuous-specific helpers; imports `chain_suboperation` |

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

For `IOWaitGroup`, exceptional `wait()` exit cancels all tracked legs; see
`IO_MANAGER_DESIGN.md`.

## References

- `packages/tealetio/src/tealetio/io_waiter.py`
- `packages/tealetio/src/tealetio/operation_callbacks.py`
- `packages/tealetio/src/tealetio/continuous_callbacks.py`
- `packages/tealetio/src/tealetio/operations.py`
- `packages/tealetio/docs/IO_MANAGER_DESIGN.md`
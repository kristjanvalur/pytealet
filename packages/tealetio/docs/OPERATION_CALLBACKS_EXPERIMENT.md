# Unified operation callback composition

Status: **Implemented** (PR #52 → `main`).

## Summary

One-shot multi-leg socket work (create → connect → send, connect → send) uses
the same callback composition model as continuous operations:

- Parent keeps a **set** of in-flight children (`_active_suboperations`) for
  **cancel propagation only**
- Composition happens in **delivery handlers** and child **done callbacks**, not
  in a chain spine
- Parent finish/error finish does not cancel children; only `cancel()` walks the
  set
- `chain_suboperation(parent, spawn, on_complete)` spawns under `parent._lock`,
  attaches the child, and runs `on_complete` when the child finishes

`operation_chaining.py` and the old `chain_parent` / `cancel_forward` /
`advance()` model have been removed.

## Two operation kinds

| Kind | Proactor completion path | Composition hook |
|------|--------------------------|------------------|
| One-shot (`connect`, `create_socket`, …) | `operation.deliver(proactor, result=…, exception=…)` | Optional `delivery` handler; else `_finish` immediately |
| Continuous (`accept_many`, `recv_many`, …) | `operation._emit_result(chunk)` | `result_callback` or `callback_factory(parent)` |

For one-shot ops the proactor calls `deliver()`. That is the right place to spawn
nested work before the parent completes.

`Operation.complete(result)` / `complete_error(exc)` finish the parent **after**
local composition (instead of bubbling via `advance()`).

## Connect + initial send

```text
proactor.connect(sock, addr, operation_factory=connect_initial_send_operation_factory(...))
  └─ connect Operation (root; scheduler waits on this)
       deliver(connect success)
         └─ chain_suboperation(connect_op, lambda: proactor.send(sock, initial), on_send_complete)
       on_send_complete(send_op):
         └─ send error  → connect_op.complete_error(exc)
         └─ send success → connect_op.complete(None)
```

Empty `initial` payload: `deliver` calls `connect_op.complete(None)` immediately
without spawning a send.

## Create → connect → send

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

Error cleanup (close socket) lives in `on_complete` handlers and `complete_error`
paths inside `create_connect_delivery`.

## Module layout

| Module | Responsibility |
|--------|----------------|
| `operations.py` | `Operation`, `ContinuousOperation`, `OperationFactory` type, suboperation tracking |
| `operation_callbacks.py` | `operation_factory`, `chain_suboperation`, delivery handlers, named factories |
| `continuous_callbacks.py` | Continuous-specific helpers (`marshal_to_scheduler`, `accept_read_delivery`, …) |

`chain_suboperation` lives in `operation_callbacks.py` and accepts any
`Operation[Any]` parent. `continuous_callbacks` re-exports or imports it for
continuous composition paths.

## Wiring

`ProactorIOManager` passes named factories into proactor `operation_factory`
hooks:

- `sock_connect(..., initial=…)` → `connect_initial_send_operation_factory`
- `sock_create(..., connect_to=…)` → `create_connect_operation_factory`

Each factory is a thin `operation_factory(delivery=…)` wrapper that attaches the
delivery handler before the proactor submits backend work.

## Semantics (aligned with continuous ops)

| Event | One-shot parent | Continuous parent |
|-------|-----------------|-------------------|
| Parent `complete()` / normal `_finish` | Children keep running | Same |
| Parent error finish | Children keep running | Same |
| Parent `cancel()` | Snapshot set, cancel children, `_cancelling` blocks late attach | Same |
| Child completion | `on_complete` may call `parent.complete(…)` | `deliver()` may run after `parent.done()` when handed off while active |

## References

- `packages/tealetio/src/tealetio/operation_callbacks.py` — one-shot composition
- `packages/tealetio/src/tealetio/continuous_callbacks.py` — continuous composition
- `packages/tealetio/src/tealetio/operations.py` — `deliver`, `complete`, `complete_error`
- `packages/tealetio/docs/IO_MANAGER_DESIGN.md` — IO manager and callback sections

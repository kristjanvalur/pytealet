# Experiment: unified operation callback composition

Status: **Exploration** on branch `experiment/unified-operation-callbacks`.

## Motivation

`operation_chaining.py` implements one-shot multi-leg work (create → connect →
send) with an explicit spine:

- `chain_parent` / `cancel_forward` (single active child weakref)
- `delivery` handlers that spawn the next leg via `operation_factory(parent=…)`
- `advance()` / `advance_hook` bubble completions toward the root
- `advance_continue()` one-shot hook clearing

It is correct and well-tested, but heavy for linear sequences. The continuous
callback experiment (`continuous_callbacks.py`) showed a simpler pattern:

- Parent keeps a **set** of in-flight children for **cancel propagation only**
- Composition happens in **completion callbacks**, not factory wiring
- Parent finish/error finish does not cancel children; only `cancel()` walks the set
- `chain_suboperation(parent, child, on_complete)` attaches, registers
  `on_complete` on the child's done callback, detaches on completion

This document explores using the same callback composition model for one-shot
operations, starting with **connect → initial send**.

## Two entry points today

| Kind | Proactor completion path | Composition hook |
|------|--------------------------|------------------|
| One-shot (`connect`, `create_socket`, …) | `operation.deliver(proactor, result=…, exception=…)` | Optional `delivery` handler; else `_finish` immediately |
| Continuous (`accept_many`, `recv_many`, …) | `operation._emit_result(chunk)` | `result_callback` or `callback_factory(parent)` |

For one-shot ops the proactor already calls `deliver()`. That is the analogue of
a continuous result callback: the right place to spawn nested work before the
parent completes.

`Operation.complete(result)` / `complete_error(exc)` are the intended way for a
delivery handler to finish the parent **after** local composition (instead of
calling `_finish` directly or bubbling via `advance()`).

## Target pattern: connect + initial send

Today (`connect_send_chain_factory` + `chained_send_link`):

```text
proactor.connect(sock, addr, operation_factory=connect_send_chain_factory(...))
  └─ connect Operation with delivery=chained_send_link
       deliver(connect success)
         └─ proactor.send(..., operation_factory=child factory with parent=connect)
              deliver(send success) → send.advance(result) → bubble to connect hook → root None
```

Proposed (`connect_initial_send_delivery` — name TBD):

```text
proactor.connect(sock, addr, delivery=connect_initial_send_delivery(proactor, initial))
  └─ connect Operation (root; scheduler waits on this)
       deliver(connect success)
         └─ send_op = proactor.send(sock, initial)   # plain child, no operation_factory
         └─ chain_suboperation(connect_op, send_op, on_send_complete)
       on_send_complete(send_op):
         └─ send error  → connect_op.complete_error(exc)
         └─ send success → connect_op.complete(None)
```

No `chain_parent`, no `advance_hook`, no `operation_factory(parent=…)` on the
send leg. Cancel on the connect op propagates to the attached send via the
suboperation set (same semantics as continuous ops).

### Empty initial payload

If `initial` is empty, `deliver` calls `connect_op.complete(None)` immediately
without spawning a send.

## Prerequisite: suboperation tracking on base `Operation`

`attach_suboperation` / `chain_suboperation` / `_cancelling` currently live on
`ContinuousOperation` only. One-shot parents need the same cancel set:

- Move suboperation set + `_cancelling` cancel wave to base `Operation` (or a
  small shared mixin used by both classes).
- `ContinuousOperation` keeps `_emit_result` / `callback_factory`; it inherits
  suboperation tracking from the base.
- Deprecate `attach_child` / `cancel_forward` / `chain_parent` / `advance()` for
  new composition paths (migrate incrementally).

`chain_suboperation` in `continuous_callbacks.py` likely moves to a neutral
module (e.g. `operation_callbacks.py`) and accepts `Operation[Any]` parents.

## Wiring options for connect + send

**Option A — `set_delivery` on the connect operation (preferred first slice)**

`ProactorIOManager.sock_connect(..., initial=…)` submits:

```python
operation = proactor.connect(sock, address)
operation.set_delivery(connect_initial_send_delivery(proactor, initial))
```

Requires proactor `connect` to return a bare `Operation` and allow attaching
delivery before the completion arrives (already true if delivery is set before
submit completes synchronously — verify for all backends).

**Option B — `operation_factory` that only sets delivery**

Thin factory wrapper, no `parent`/`advance_hook` spine — keeps current proactor
`operation_factory` parameter but drops chain factories.

**Option C — proactor `connect(..., delivery=…)` parameter**

Surface delivery on the proactor API; likely redundant if `set_delivery` after
spawn is sufficient.

Start with **A or B**; avoid expanding proactor signatures until the pattern
settles.

## Phase 2: create → connect → send

`create_socket_chain_factory` adds fd-close on error via `advance_hook`. In the
unified model:

```python
def create_connect_send_delivery(proactor, connect_to, initial_data):
    sock_holder: list[socket.socket] = []

    def delivery(_proactor, operation, result, exception):
        if exception is not None:
            operation.complete_error(exception)
            return
        sock = cast(socket.socket, result)
        sock_holder.append(sock)
        connect_op = proactor.connect(sock, connect_to)
        chain_suboperation(
            operation,
            connect_op,
            lambda op: _on_connect_complete(operation, sock_holder, op, initial_data),
        )
    ...
```

Error cleanup (close socket) belongs in the connect/send completion handlers and
in `complete_error` paths, not in `advance_hook`. Shape root result as the
socket in the final `operation.complete(sock)` after connect+send succeed.

## Semantics alignment with continuous ops

| Event | One-shot parent (unified) | Continuous parent (existing) |
|-------|---------------------------|------------------------------|
| Parent `complete()` / normal `_finish` | Children keep running | Same |
| Parent error finish | Children keep running | Same |
| Parent `cancel()` | Snapshot set, cancel children, `_cancelling` blocks late attach | Same |
| Child completion | `on_complete` may call `parent.complete(…)` even if … | `deliver()` may run after `parent.done()` when handed off while active |

One-shot ops complete once; there is no streaming `done()` before children
finish except the brief window between last `deliver` spawning a child and
`complete()` from `on_complete`. That window is where cancel propagation
matters.

## Migration plan

1. **Lift suboperation tracking** to base `Operation`; keep `chain_suboperation`.
2. **PoC** `connect_initial_send_delivery` + switch `sock_connect(..., initial=…)`.
3. **Tests** — parity with existing connect-send chain tests; cancel-while-send
   pending.
4. **PoC** `create_connect_send_delivery`; switch `sock_create(connect_to=…)`.
5. **Remove** unused chain links (`chained_send_link`, `connect_send_chain_factory`, …) once parity holds.
6. **Document** in `IO_MANAGER_DESIGN.md`; trim `operation_chaining.py` to only
   what still needs `advance()` (if anything).

## Open questions

- Should `deliver()` on one-shot ops reject spawning when `_cancelling` (mirror
  `_emit_result` on continuous ops)?
- Do we still need `operation_factory` on proactor methods at all, or only bare
  `Operation` + `set_delivery` / caller-side composition?
- How do we type the root result when create returns a socket but connect/send
  legs complete with `None`? (Today: advance hook shapes socket at root.)
- Free-threaded / threaded proactor delivery: same `_cancelling` race fixes as
  `ContinuousOperation.cancel()`.

## References

- `packages/tealetio/src/tealetio/operation_chaining.py` — current chain model
- `packages/tealetio/src/tealetio/continuous_callbacks.py` — continuous composition
- `packages/tealetio/src/tealetio/operations.py` — `deliver`, `complete`, `complete_error`
- `packages/tealetio/docs/IO_MANAGER_DESIGN.md` — continuous callback section
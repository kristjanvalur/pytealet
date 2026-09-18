# Agent Instructions for tealetio

## Scope

`tealetio` is the synchronous, asyncio-like runtime built on core `tealet`:
schedulers, tasks, futures, locks, queues, selector helpers, proactor IO
(`UringProactor` / `SelectorProactor`), streams, and asyncio coexistence.

Workspace root `AGENTS.md` covers monorepo tooling. Prefer this file for
package-local conventions when working under `packages/tealetio/`.

## Quality checks before push / PR

**Always run the workspace quality suite before pushing commits or updating a
PR** that touches `packages/tealetio/` (or shared roots that tealetio CI
watches). Root CI runs `make check` on every PR; unused imports and format
failures fail the quality job before tests.

From the workspace root:

```bash
make check    # ruff format --check, ruff check, ty check
# or fix first:
make fix
make check
```

Do not rely on tealetio package tests alone — they can pass while `ruff check`
still fails.

## Dev and test

```bash
uv sync --active --locked --dev --package tealetio
uv run --active --package tealetio python -m pytest packages/tealetio/tests/ -v
```

`UringProactor` paths need Linux and a working `uring-api` native build (see
`packages/uring_api/AGENTS.md`). Selector-backed tests cover the non-uring
matrix.

## Large files — grep, then `offset`/`limit`

Do **not** read these files whole. Grep for the symbol, then read a slice.

- **`src/tealetio/proactor.py`** (~4k lines): CQE helpers and
  `RecvBufferPool` at the top; `Proactor` protocol and `ProactorBase`; then
  `SelectorProactor` / `ThreadedSelectorProactor`; then `UringProactor`;
  proactor schedulers at the bottom. `recv_many` / `accept_many` /
  `poll_many` / `send` / cancel are methods on each concrete proactor.
- **`src/tealetio/scheduler.py`** (~2k lines): runnable queues
  (Fifo / Priority), driving mixins, `Channel`,
  `BaseScheduler` (run loop, `call_soon` / `call_soon_threadsafe`, callback
  drain), `BasicScheduler` at the end.
- **`tests/test_proactor.py`** (~6k lines): grep by test class or name.

Related, smaller files (prefer these over re-reading the proactor):

- `delivery.py` — `MultishotDelivery`, reorder/count finalizers, marshal
- `io_manager.py` — `ProactorIOManager` composition (not inside the proactor)
- `io_waiter.py` — `IOWaiter`, `IOWaitGroup`
- `docs/OPERATION_CALLBACKS.md`, `docs/IO_MANAGER_DESIGN.md`

## Package boundaries

- Keep scheduler/IO/asyncio coexistence here, not in core `tealet`.
- Prefer narrow changes: proactor delivery, io_manager, streams — avoid
  unrelated package edits when fixing a single component.
- Follow root `AGENTS.md` coding guidelines (internal contracts, British English
  in docs, assert for structural invariants).

## Light API calls

API contracts are not aggressively enforced. Keep public methods light and
rely on deeper breakage to detect misuse.

Do not add `_check_open()` / `if self.closed()` on every `UringProactor`
method. Use after `close()` is misuse; the owned ring raises
`RuntimeError("ring is closed")`. Selector backends may still check at
`_check_open()` because the selector has no equivalent inner failure.

Validate at the boundary that owns the resource, and only when the real call
would otherwise succeed silently or corrupt state. Feature combinations and
arguments with no deeper failure still deserve a clear error at the API that
accepted them.

## Typing

Workspace policy is in **`docs/TYPING.md`** (and root `AGENTS.md`).

For tealetio specifically:

- **Annotate** public scheduler, proactor, io_manager, stream, and asyncio
  coexistence APIs thoroughly — that is what package users and `py.typed`
  consumers rely on.
- **Private** helpers (CQE complete/deliver, freelist, cargo) may omit
  annotations. Prefer untyped or `Any` cargo over hot-path `typing.cast`.
- Uring ring legs: one-shot submits call the ring directly; complete handlers
  trust cq cargo; use `assert isinstance` at CQE trust boundaries rather than
  cast helpers.
- Do not turn on mypy-style “all defs annotated” requirements; CI uses **ty**
  with gradual typing.
- Assigning a bound method onto an instance attribute
  (`self.recv_multishot = self._recv_multishot_fallback`) is valid Python. If
  **ty** rejects it (`invalid-assignment` / `invalid-method-override`), fix
  the types or ignore that diagnostic — do not “fix” the override by
  renaming it away.

## Tests that cannot run

`pytest.skip` means the *environment* cannot run the test (no uring, wrong
OS, missing feature). If a test is impossible to write reliably, **delete it
or comment it out with a reason**. Do not skip it.

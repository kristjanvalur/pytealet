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

## Package boundaries

- Keep scheduler/IO/asyncio coexistence here, not in core `tealet`.
- Prefer narrow changes: proactor delivery, io_manager, streams — avoid
  unrelated package edits when fixing a single component.
- Follow root `AGENTS.md` coding guidelines (internal contracts, British English
  in docs, assert for structural invariants).

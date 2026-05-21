# Thread Hardening Session Notes (threads branch)

Date: 2026-05-20
Scope: Cross-thread ownership hardening for critical pytealet operations, plus related docs/tests.

## Current branch state
- Branch: `threads`
- Runtime integration now uses vendored `libtealet` 0.7.2.
- Locking mode now uses `TEALET_LOCK_AUTO`.

## What is already implemented
- Owner thread recorded on tealet objects (`owner_tid`).
- Public API helper added: `tealet.belongs_to_current()`.
- Cross-thread guards exist for:
  - `stub()` from non-owner thread (rejects with `InvalidError`).
  - `run()` / `switch()` path via `CheckTarget` ownership checks.
- Volatile traversal APIs are owner-thread only:
  - `current()` from non-owner thread rejects with `InvalidError`.
  - `main()` from non-owner thread rejects with `InvalidError`.
  - `previous()` from non-owner thread rejects with `InvalidError`.
- Cross-thread duplicate/dealloc are explicitly permitted:
  - `_tealet.tealet(existing_stub)` can duplicate a foreign-thread stub.
  - foreign-thread `pytealet_dealloc()` can call `tealet_delete()`.
- Documentation updated in `docs/ARCHITECTURE.md` with ownership model and constraints.
- Free-threaded lock callback wiring in `GetMain()` uses `tealet_configure_set_locking(..., TEALET_LOCK_AUTO)`.
- `_tealet.tealet` now supports weak references on supported runtimes.
- Legacy `lock_state_key` scaffolding is removed from module state.

## Test status snapshot (2026-05-20)
- Focused thread ownership check passes:
  - `tests/test_tealet.py::TestThreadOwnership::test_new_tealet_has_owner_tid_and_belongs`
- Thread ownership class currently: **7 passed, 0 failed**.
- Coverage now includes:
  - foreign-thread `stub()` rejected
  - foreign-thread `run()` rejected
  - foreign-thread `switch()` rejected
  - foreign-thread `current()` / `main()` / `previous()` rejected
  - foreign-thread stub duplicate allowed
  - foreign-thread deallocation path allowed
- Full suite snapshot:
  - `tests/`: **26 passed, 1 skipped** (py313-debug env)
- Greenlet thread-focused tests were not run to completion in this env due to missing `greenlet` module.

## Policy Decisions (Current)

1. Cross-thread duplicate is **allowed**.
2. Cross-thread deallocation is **allowed**.
3. Volatile traversal methods (`current()`, `main()`, `previous()`) are **owner-thread only**.
4. Other non-auto-locked APIs are allowed/rejected explicitly per API semantics; no blanket cross-thread allow rule is implied.

## Resolved Items

1. Cross-thread duplicate policy is now explicit and tested.
2. Cross-thread deallocation policy is now explicit and tested.
3. Traversal volatility policy is implemented and tested.
4. `lock_state_key` concern is obsolete (field removed).
5. Locking mode docs in this note are updated to `TEALET_LOCK_AUTO`.

## Remaining Follow-up

1. Keep architecture docs synchronized with the new per-API cross-thread contract.
2. Decide whether any additional readonly APIs should become owner-thread-only for consistency.

## API Inventory (Current Behavior)

- Owner-thread restricted:
  - `stub()`, `run()`, `switch()`, `current()`, `main()`, `previous()`.
- Explicitly permitted cross-thread:
  - duplicate-from-stub via `_tealet.tealet(existing_stub)`.
  - wrapper deallocation path (`pytealet_dealloc()` -> `tealet_delete()`).
- Always-safe metadata/inspection:
  - `thread_id`, `state`, `belongs_to_current()`.

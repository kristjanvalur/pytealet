# PyTealet Issue Log

**Status:** Historical fixes plus remaining hardening items.

## Overview

The pytealet C extension has been modernised for Python 3.10+ and currently runs a stable in-repo baseline. This document tracks historical issues, resolved fixes, and remaining hardening work. Use the repository test and CI results as the source of truth for current pass counts; older counts below are retained only as validation evidence for specific fixes.

## Validation Notes

- Historical P0 runtime crashes documented below are fixed in current code.
- Remaining items are mainly hardening and modernisation follow-ups.
- Run the current checks from the workspace root when validating a release:
    - `make check`
    - `uv run --active python -m pytest tests/`
    - `uv run --active --package tealetio python -m pytest packages/tealetio/tests/`
    - `uv run --active --package tealet-greenlet python -m pytest packages/tealet-greenlet/tests/`

---

## P0 - Historical Critical Issues (Resolved)

### ✅ Issue #0: No-GIL Critical-Section tstate Segfault (Resolved)

**Location:** `src/_tealet/tstate_state.c`, Python 3.13+ free-threaded builds

**Problem:**
No-GIL CPython stores a `PyThreadState.critical_section` chain whose entries are
stack-allocated C structures. `_tealet` saved/restored frame and datastack state
across stack switches, but did not move or clear this critical-section head.
After switching to another tealet stack, CPython could later try to suspend or
resume a stale critical-section chain during import, thread startup, or executor
shutdown, causing a native segfault.

**Observed crash paths:**
- `concurrent.futures.__getattr__` lazy-loading `ThreadPoolExecutor` from inside
    a tealet task.
- `threading.Thread.start()` / `Condition.wait()` while starting executor work
    from inside a tealet task.

**Fix:**
Treat `critical_section` as frame-like execution state on `Py_GIL_DISABLED`
builds: save it with the owning tealet, restore it only with that tealet's C
stack, and clear it for fresh tealet branches.

**Validation:**
- Runtime-runner executor/shutdown crash subset repeated 20x.
- Scheduler executor/to-thread crash subset repeated 20x.
- Full root suite: `161 passed`.
- Full `tealetio` suite: `327 passed`.

### ✅ Issue #1: Segfault in pytealet_get_main() (Resolved)

**Location:** `src/_tealet/pytealet.c` line 438

**Problem:** 
When a new PyTealetObject is created (STATE_NEW), it has no underlying tealet (`tealet=NULL`). Accessing the `.main` property dereferences this NULL pointer, causing a segfault.

**Historical Code (pre-fix):**
```c
static PyObject *
pytealet_get_main(PyObject *_self, void *_closure)
{
    PyTealetObject *self = (PyTealetObject *)_self;
    PyTealetObject *main = TEALET_PYOBJECT(self->tealet->main);  // ❌ self->tealet is NULL!
    Py_INCREF(main);
    return (PyObject*)main;
}
```

**Reproduction:**
```python
import _tealet
t = _tealet.tealet()  # STATE_NEW, tealet=NULL
print(t.main)         # SEGFAULT
```

**Fix:**
```c
static PyObject *
pytealet_get_main(PyObject *_self, void *_closure)
{
    PyTealetObject *self = (PyTealetObject *)_self;
    
    if (!self->tealet) {
        // New tealet not yet initialized, return thread's main
        PyTealetObject *main = GetMain();
        if (!main)
            return NULL;
        Py_INCREF(main);
        return (PyObject*)main;
    }
    
    PyTealetObject *main = TEALET_PYOBJECT(self->tealet->main);
    Py_INCREF(main);
    return (PyObject*)main;
}
```

**Impact:** Historical only. This no longer blocks normal usage.

**Design Concern:**
The NULL tealet pointer occurs in two scenarios:
1. **STATE_NEW:** PyTealetObject created but no underlying tealet yet (lazy creation)
2. **STATE_EXIT:** Tealet exited and auto-deleted, but PyTealetObject still exists

Should review if lazy creation (STATE_NEW without tealet) is intentional design or should be changed. The NULL fallback is necessary for STATE_EXIT case at minimum.

---

### ✅ Issue #2: Wrong tealet_exit() Flags (Resolved)

**Location:** `src/_tealet/pytealet.c` line 625

**Problem:**
Code expects libtealet to auto-delete the C-level tealet on exit (as evidenced by comment "will be auto-deleted on return" and setting `tealet->tealet = NULL`), but uses `TEALET_EXIT_DEFAULT` which means "don't delete".

**Historical Code (pre-fix):**
```c
/* clear the old tealet */
tealet->state = STATE_EXIT;
tealet->tealet = NULL; /* will be auto-deleted on return */
TEALET_SET_PYOBJECT(t_current, NULL);
t_return = return_to->tealet;

/* decref the objects after the switch */
dustbin_fill(t_return, func, (PyObject*)tealet, result);

Py_INCREF(return_arg);
if (tealet_exit(t_return, (void*)return_arg, TEALET_EXIT_DEFAULT))  // ❌ Wrong flag
    tealet_exit(t_return->main, (void *)return_arg, TEALET_EXIT_DEFAULT);
```

**Why This Is Wrong:**
1. Comment says "will be auto-deleted on return"
2. Code sets `tealet->tealet = NULL` expecting libtealet to own deletion
3. `pytealet_dealloc()` has `if (tealet->tealet)` check before calling `tealet_delete()`
4. This design only works if libtealet deletes the C object

**API Definition:**
```c
#define TEALET_EXIT_DEFAULT 0  /* Don't auto-delete */
#define TEALET_EXIT_DELETE  1  /* Auto-delete on exit */
#define TEALET_EXIT_DEFER   2  /* Defer exit to return statement */
```

**Fix:**
```c
Py_INCREF(return_arg);
if (tealet_exit(t_return, (void*)return_arg, TEALET_EXIT_DELETE))
    tealet_exit(t_return->main, (void *)return_arg, TEALET_EXIT_DELETE);
```

**Impact:** Historical only. Current runtime uses corrected exit behavior.

**Historical Context:** 
Original code was written when libtealet's default behavior was to auto-delete. The API evolved to require explicit `TEALET_EXIT_DELETE` flag.

---

## P1 - High (Incorrect Behavior)

### ✅ Issue #3: Missing NULL Checks in Other Property Getters (Resolved)

**Location:** `src/_tealet/pytealet.c` lines 460-480 (approximate)

**Problem (historical):**
Similar to Issue #1, other property getters were suspected to lack NULL checks for STATE_NEW tealets.

**Properties to Check:**
- `pytealet_get_tid()` - Thread ID getter
- `pytealet_get_frame()` - Frame getter (likely already handles NULL)

**Current Status:**
Audited against current code. Remaining property getters are safe for STATE_NEW/STATE_EXIT paths:
- `thread_id` reads `owner_tid` only.
- `frame` falls back to `None`/current frame and does not dereference a missing native tealet.
- `context` uses running-state checks and stored tstate context with lock discipline.

**Impact:** Historical only; no active crash issue identified in current implementation.

---

## P2 - Medium (Enhancements)

### ✅ Issue #4: tealet_create() Migration Target (Closed)

**Location:** `src/_tealet/pytealet.c` (`pytealet_stub_impl`, `pytealet_run_dispatch`)

**Current Approach:**
Current wrapper uses supported libtealet APIs (`tealet_new`, `tealet_run`, `tealet_spawn`, `tealet_switch`).

```c
/* stubs are spawned with pytealet_primed_main and TEALET_START_DEFAULT */
```

**Status:** Closed as not applicable.

**Reason:**
In the vendored libtealet version, `tealet_create()` is no longer a public migration target (see libtealet changelog notes about removal/rework of older `tealet_create()` flows). The wrapper already uses the current supported creation APIs.

---

### ✅ Issue #5: Deprecated PyThread TLS API (Resolved)

**Location:** `src/_tealet/pytealet.c`, `src/_tealet/pytealet_module.c`

**Problem (historical):**
Older code used deprecated `PyThread_*` key-value TLS APIs:
```c
static int tls_key;
PyThread_get_key_value(tls_key);
PyThread_set_key_value(tls_key, ...);
```

**Current Status:**
TLS handling has been migrated to `PyThread_tss_*` APIs (`PyThread_tss_create/get/set/delete`).

**Impact:** Resolved for TLS deprecation concern.

---

### ✅ Addressed: Thread Cleanup + Module Teardown Handling

**Location:** `src/_tealet/` (module teardown, thread-state save/restore, deferred cleanup)

**Implemented:**
- Added explicit per-thread lineage tracking in module state using a lock-protected circular list of `PyTealetMainData`.
- Layered thread cleanup into:
    - a Python-facing validated wrapper (`thread_reap()`), and
    - an internal teardown-safe cleanup path for module shutdown.
- Added module teardown ring draining in `pytealet_module_free()` to walk remaining thread lineages and clean them best-effort.
- Updated cleanup invalidation semantics so cleaned wrappers are set to `STATE_EXIT` and reject further `switch()/run()` operations via normal state checks.

**Current Status:**
- Thread and module teardown paths now have explicit cleanup behavior and are no longer TODO.
- Remaining hardening (optional): add explicit `Py_IsFinalizing()` guards for additional non-essential paths if future shutdown edge cases are observed.

**Priority:** Completed (with optional follow-up hardening)

---

### ✅ Scheduler Task Factories, Eager Startup, and Stub-Backed Tasks (Resolved)

**Location:** `packages/tealetio/src/tealetio/scheduler.py`, `packages/tealetio/src/tealetio/tasks.py`

**Implemented:**
- `BaseScheduler.set_task_factory(...)` and `get_task_factory()` configure
    scheduler-local task construction policy.
- `DefaultTaskFactory` preserves direct `TealetTask.prepare(...)` task creation.
- `StubTaskFactory` creates scheduler tasks from a reusable prepared tealet stub;
    `stub_here()` creates the reusable stub at the caller's current tealet stack
    point, and the factory lazily creates one on first use if needed.
- Factories receive the owning scheduler, target callable, selected
    `contextvars.Context`, and optional per-spawn `eager_start` override.
- `BaseScheduler.spawn(..., eager_start=...)` and factory defaults follow
    asyncio naming. Eager startup runs immediately only while the scheduler is
    already running, matching asyncio's `eager_start` condition.
- Task bookkeeping via `all_tasks()` covers default, eager, and stub-backed
    tasks consistently, including tasks that complete during eager startup.

**Current Design Choice:**
The scheduler uses a factory-based task construction model rather than a
dedicated scheduler-level `set_stub(...)` API. Stub usage is expressed by
installing `StubTaskFactory`, which keeps stub-backed task creation aligned with
the same task factory path as direct and eager task creation.

**Priority:** Completed

---

### ✅ Source Distribution Includes Vendored libtealet Archives (Resolved)

**Location:** `MANIFEST.in`, `src/_tealet/libtealet/`, source-distribution build flow

**Historical Problem:**
Building an sdist currently includes Python sources, tests, and typing files, but not the vendored prebuilt `libtealet` archives required by the default build path. A wheel built directly from the workspace succeeds, but a wheel built from the generated sdist fails because `setup.py` cannot find `src/_tealet/libtealet/lib/<abi>/libtealet.a`.

**Observed Failure:**
```text
RuntimeError: Pre-built libraries not found for ABI: sysv_amd64 at src/_tealet/libtealet/lib/sysv_amd64
```

**Fix:**
- `MANIFEST.in` now grafts `src/_tealet/libtealet`, including the vendored headers and prebuilt archives used by the default build path.
- `MANIFEST.in` also includes local `_tealet` C headers required to compile extension sources from an extracted sdist.
- Validated by building an sdist, checking for representative Unix and Windows static archives, and building a wheel from the extracted sdist.

**Priority:** Completed

---

## Testing Recommendations

### Critical Path Tests (Must Pass Before Release)

```python
def test_new_tealet_properties():
    """Test Issue #1 fix"""
    import _tealet
    t = _tealet.tealet()
    main = t.main  # Should not segfault
    assert main is not None
    assert t.state == _tealet.STATE_NEW

def test_tealet_run_and_exit():
    """Test Issue #2 fix"""
    import _tealet
    
    def worker(current, arg):
        return arg * 2
    
    t = _tealet.tealet()
    result = t.run(worker, 21)
    # After return, tealet should be cleaned up
    assert result == 42
    assert t.state == _tealet.STATE_EXIT
    # No memory leak should occur

def test_state_transitions():
    """Test state machine"""
    import _tealet
    t = _tealet.tealet()
    assert t.state == _tealet.STATE_NEW
    
    t.stub()
    assert t.state == _tealet.STATE_STUB
    
    # Should be able to duplicate
    t2 = t.duplicate()
    assert t2.state == _tealet.STATE_STUB

def test_thread_isolation():
    """Test threading model"""
    import _tealet
    import threading
    
    mains = []
    def thread_func():
        main = _tealet.main()
        mains.append(main)
    
    threads = [threading.Thread(target=thread_func) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    
    # Each thread should have its own main
    assert len(set(id(m) for m in mains)) == 4
```

---

## Fix Checklist

- [x] **P0-1:** Fix `pytealet_get_main()` segfault
- [x] **P0-2:** Fix `tealet_exit()` flags (DEFAULT → DELETE)
- [x] **P1-3:** Add NULL checks to other property getters (audited/resolved)
- [x] **Test:** Run full test suite for the original fix baseline
- [x] **Test:** Fix segfault in TestRandom1 and TestRandom2 (no longer reproducing in current suite)
- [ ] **Test:** Verify no memory leaks with valgrind
- [ ] **Test:** Multi-threaded stress test
- [x] **P2-4:** (Closed) `tealet_create()` migration target is not applicable in current vendored libtealet API
- [x] **P2-5:** (Resolved) Migrated away from deprecated TLS key-value PyThread API to `PyThread_tss_*`

Historical validation counts for individual fixes are recorded in their issue
sections. Current pass counts should come from the test commands above or CI.

---

## Resolution Status

**Status:** Historical summary; current validation lives in CI and the test commands above.

| Issue | Status | Assignee | Notes |
|-------|--------|----------|-------|
| #1 Segfault in get_main | ✅ Fixed | - | Committed |
| #2 Wrong exit flags | ✅ Fixed | - | Committed |
| #3 Other NULL checks | ✅ Fixed | - | Audited current getters; no active NULL-deref issue |
| #4 tealet_create() | ✅ Closed | - | Migration target not applicable for current vendored libtealet API |
| #5 PyThread API | ✅ Fixed | - | TLS migrated to `PyThread_tss_*` APIs |
| Scheduler task factories | ✅ Fixed | - | Factory-based default/stub/eager_start task creation implemented |

---

## Notes

The code architecture is sound. Current open items are primarily hardening and modernization tasks rather than release-blocking runtime failures.

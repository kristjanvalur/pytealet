# PyTealet Current Issues

**Date:** November 30, 2025  
**Status:** Code compiles but has runtime bugs preventing operation

## Overview

The pytealet C extension has been successfully modernized for Python 3.10+ but has critical runtime issues that prevent it from working correctly. This document tracks known issues and their fixes.

## Recent Validation (March 31, 2026)

- Debug rebuild succeeds with strict defaults (`-std=c17 -pedantic-errors`) via `./scripts/fast_build.sh debug`.
- `_tealet` smoke import/creation checks in the fast build script pass.
- `tests/test_tealet.py` passes fully with stub tests enabled:
    - `PYTEALET_ENABLE_STUB_TESTS=1 uv run pytest tests/test_tealet.py -q`
    - Result: `12 passed`.
- Recent stabilization included:
    - Including libtealet stub helper declarations (`tools.h`) in `pytealet.c`.
    - Removing local `TEALET_EXTRA` macro redefinition in favor of upstream `TEALET_EXTRA(t, tp)`.
    - Refactoring dustbin handling to a list-backed mechanism with preallocation intent and deferred tstate decref routing only on the `pytealet_main()` exit path.

---

## P0 - Critical (Prevents Operation)

### 🔴 Issue #1: Segfault in pytealet_get_main()

**Location:** `src/_tealet/pytealet.c` line 438

**Problem:** 
When a new PyTealetObject is created (STATE_NEW), it has no underlying tealet (`tealet=NULL`). Accessing the `.main` property dereferences this NULL pointer, causing a segfault.

**Current Code:**
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

**Impact:** Prevents basic usage - the test case immediately segfaults.

**Design Concern:**
The NULL tealet pointer occurs in two scenarios:
1. **STATE_NEW:** PyTealetObject created but no underlying tealet yet (lazy creation)
2. **STATE_EXIT:** Tealet exited and auto-deleted, but PyTealetObject still exists

Should review if lazy creation (STATE_NEW without tealet) is intentional design or should be changed. The NULL fallback is necessary for STATE_EXIT case at minimum.

---

### 🔴 Issue #2: Wrong tealet_exit() Flags

**Location:** `src/_tealet/pytealet.c` line 625

**Problem:**
Code expects libtealet to auto-delete the C-level tealet on exit (as evidenced by comment "will be auto-deleted on return" and setting `tealet->tealet = NULL`), but uses `TEALET_EXIT_DEFAULT` which means "don't delete".

**Current Code:**
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

**Impact:** Memory leak - C-level tealet structures are not freed when functions return.

**Historical Context:** 
Original code was written when libtealet's default behavior was to auto-delete. The API evolved to require explicit `TEALET_EXIT_DELETE` flag.

---

## P1 - High (Incorrect Behavior)

### ⚠️ Issue #3: Missing NULL Checks in Other Property Getters

**Location:** `src/_tealet/pytealet.c` lines 460-480 (approximate)

**Problem:**
Similar to Issue #1, other property getters may also lack NULL checks for STATE_NEW tealets.

**Properties to Check:**
- `pytealet_get_tid()` - Thread ID getter
- `pytealet_get_frame()` - Frame getter (likely already handles NULL)

**Fix Pattern:**
Add defensive NULL checks similar to the `.main` property fix.

**Impact:** Potential segfaults when accessing properties on STATE_NEW tealets.

---

## P2 - Medium (Enhancements)

### 💡 Issue #4: Could Use tealet_create() API

**Location:** `src/_tealet/pytealet.c` line 40 (stub_new), line 344 (pytealet_run)

**Current Approach:**
Code uses `tealet_new()` which creates a tealet and immediately switches to it:

```c
static tealet_t *stub_new(tealet_t *t) {
    void *arg = (void*)tealet_current(t);
    return tealet_new(t, stub_main, &arg);  // Creates and switches
}
```

**Status:** ✅ This works correctly - the immediate switch-back in `stub_main` is intentional.

**Enhancement Opportunity:**
`tealet_create()` was added to libtealet later and provides a cleaner API:

```c
static tealet_t *stub_new(tealet_t *t) {
    tealet_t *stub = tealet_create(t, stub_main);
    if (!stub) return NULL;
    
    // Now manually switch to initialize it
    void *arg = (void*)tealet_current(t);
    int err = tealet_switch(stub, &arg);
    if (err) {
        tealet_delete(stub);
        return NULL;
    }
    return stub;
}
```

**Benefits:**
- More explicit about when switching happens
- Better error handling between create and switch
- Clearer code intent

**Trade-offs:**
- Current code works fine and is more concise
- Migration requires testing
- Negligible performance difference

**Priority:** P2 (Enhancement) - not critical, improves code clarity.

---

### 📝 Issue #5: Deprecated PyThread API

**Location:** Throughout `src/_tealet/pytealet.c`

**Problem:**
Uses deprecated `PyThread_*` API for thread-local storage:
```c
static int tls_key;
PyThread_get_key_value(tls_key);
PyThread_set_key_value(tls_key, ...);
```

**Impact:** Works but uses deprecated API (Python 3.7+).

**Recommendation:** Consider migrating to modern Python threading API in future, but low priority since it still works.

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
    t2 = _tealet.tealet(t)
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
- [ ] **P1-3:** Add NULL checks to other property getters
- [x] **Test:** Run basic test suite (via fast_build.sh) - 10/12 tests pass
- [ ] **Test:** Fix segfault in TestRandom1 and TestRandom2
- [ ] **Test:** Verify no memory leaks with valgrind
- [ ] **Test:** Multi-threaded stress test
- [ ] **P2-4:** (Optional) Migrate to `tealet_create()` API
- [ ] **P2-5:** (Optional) Migrate away from deprecated PyThread API

**Test Results (after P0 fixes):**
```
tests/test_tealet.py::TestModule::test_main PASSED
tests/test_tealet.py::TestModule::test_main2 PASSED
tests/test_tealet.py::TestModule::test_main3 PASSED
tests/test_tealet.py::TestSimple::test_simple PASSED
tests/test_tealet.py::TestStatus::test_status_run PASSED
tests/test_tealet.py::TestStatus::test_status_stub PASSED
tests/test_tealet.py::TestSubclass::test_subclass PASSED
tests/test_tealet.py::TestSwitch::test_switch PASSED
tests/test_tealet.py::TestSwitch::test_switch_new PASSED
tests/test_tealet.py::TestSwitch::test_switch_arg PASSED
tests/test_tealet.py::TestRandom1::test_random SEGFAULT ❌
tests/test_tealet.py::TestRandom2::test_random SEGFAULT ❌
```

---

## Resolution Status

**Last Updated:** November 30, 2025

| Issue | Status | Assignee | Notes |
|-------|--------|----------|-------|
| #1 Segfault in get_main | ✅ Fixed | - | Committed |
| #2 Wrong exit flags | ✅ Fixed | - | Committed |
| #3 Other NULL checks | 🔴 Open | - | Needs investigation |
| #4 tealet_create() | 💡 Enhancement | - | Low priority |
| #5 PyThread API | 📝 Tracked | - | Low priority |

---

## Notes

The code architecture is sound - these are primarily API evolution issues where libtealet changed behavior over the past 12 years. Once the P0 issues are fixed, the code should work correctly.

# PyTealet C Extension Analysis - Historical Document

**Analysis Date:** November 30, 2025  
**Original Code Date:** ~2013 (12 years old)  
**libtealet Version:** 0.3.2 (modern)  
**Python Version Target:** 3.10+

**⚠️ Note:** This was the initial analysis document used during code modernization. It has been split into:
- **`ARCHITECTURE.md`** - Architecture and design documentation
- **`ISSUES.md`** - Current issues and bug tracking

This file is kept for historical reference.

---

## Python API Reference

### C Extension Module: `_tealet`

**Module-Level Functions:**

```python
_tealet.current() -> tealet
```
Returns the currently executing tealet in this thread.

```python
_tealet.main() -> tealet
```
Returns the main tealet for this thread (the root of the tealet tree).

```python
_tealet.hide_frame(func, *args, **kwds) -> result
```
Calls `func(*args, **kwds)` with the current frame hidden from the stack trace. Used internally to clean up tracebacks.

**Module-Level Classes:**

```python
class _tealet.tealet([source_tealet])
```
The core tealet class. If `source_tealet` is provided and is a STUB, creates a duplicate.

**Tealet Object Methods:**

```python
tealet.stub() -> tealet
```
Converts a NEW tealet into a STUB (duplicatable template). Returns self.
- **State requirement:** Must be STATE_NEW
- **Effect:** Changes state to STATE_STUB, creates suspended tealet that can be duplicated
- **Uses:** `stub_new()` internally (currently uses `tealet_new()` - issue!)

```python
tealet.run(function, arg=None) -> result
```
Executes a function in the tealet context.
- **State requirement:** Must be STATE_NEW or STATE_STUB
- **Parameters:**
  - `function`: Callable that receives `(current_tealet, arg)`
  - `arg`: Optional argument passed to function
- **Returns:** Whatever the function returns (or switches to)
- **Effect:** Changes state to STATE_RUN, executes function

```python
tealet.switch(arg=None) -> result
```
Switches execution to this tealet, passing an optional argument.
- **State requirement:** Must be STATE_RUN (active tealet)
- **Parameters:**
  - `arg`: Optional value to pass to the target tealet
- **Returns:** Value passed back when someone switches to us
- **Thread-safe:** Only within same thread family

**Tealet Object Properties (read-only):**

```python
tealet.main -> tealet
```
The main tealet for this tealet's thread.
- **Current Bug:** Segfaults if accessed on STATE_NEW tealet (tealet pointer is NULL)

```python
tealet.state -> int
```
Current state of the tealet:
- `STATE_NEW` (0): Created but not initialized
- `STATE_STUB` (1): Duplicatable template
- `STATE_RUN` (2): Currently active/runnable
- `STATE_EXIT` (3): Exited/finished

```python
tealet.frame -> frame | None
```
The current Python frame for this tealet, or None if not active.

```python
tealet.thread_id -> int
```
The OS thread ID that owns this tealet's family.

**Module Constants:**

```python
_tealet.STATE_NEW = 0
_tealet.STATE_STUB = 1
_tealet.STATE_RUN = 2
_tealet.STATE_EXIT = 3
```

**Exception Classes:**

```python
_tealet.TealetError
```
Base exception for all tealet errors.

```python
_tealet.DefunctError(TealetError)
```
Raised when trying to switch to a defunct (corrupted) tealet.

```python
_tealet.InvalidError(TealetError)
```
Raised when trying to switch to a tealet from a different thread family.

```python
_tealet.StateError(TealetError)
```
Raised when an operation is invalid for the tealet's current state.

---

### High-Level Python API: `tealet.greenlet`

The `tealet.greenlet` module provides a greenlet-compatible interface:

```python
from tealet import greenlet

greenlet.getcurrent() -> greenlet
```
Returns the current greenlet.

```python
class greenlet.greenlet(run=None, parent=None)
```
Greenlet-compatible wrapper around tealets.

**Methods:**
- `switch(*args, **kwds)` - Switch to this greenlet
- `throw(type, value=None, traceback=None)` - Throw exception in greenlet
- `gr_frame` - Property: current frame
- `dead` - Property: True if greenlet has exited

**Exceptions:**
- `greenlet.error` - Greenlet error
- `greenlet.GreenletExit` - Raised to exit a greenlet

---

## Critical Issues Found (Detailed)

### 💡 ENHANCEMENT: Could Use tealet_create() API

**Location:** `stub_new()` line 40, `pytealet_run()` line 344

**Python API Affected:** `tealet.stub()` method (creates duplicatable templates)

**Current Approach:** The code uses `tealet_new()` which creates a tealet and immediately switches to it:

```c
/* Allocate and switch to a new tealet.
 * This is semantically equivalent to
 * tealet_create() followed by tealet_switch(), but may be slightly faster.
 * The return value is the new tealet, or NULL if memory allocation failed.
 * Note that this tealet may have been already freed should run(g) have
 * returned by the time this function returns.
 */
tealet_t *tealet_new(tealet_t *tealet, tealet_run_t run, void **parg);
```

**Current Code:**
```c
static tealet_t *stub_new(tealet_t *t) {
    void *arg = (void*)tealet_current(t);
    return tealet_new(t, stub_main, &arg);  // Creates and switches
}
```

**How It Works:**
1. `tealet_new()` creates a new tealet with `stub_main` as entry point
2. **Immediately switches to it** (calling `stub_main`)
3. `stub_main` immediately switches back to the caller (intentional!)
4. Returns the tealet pointer, now suspended at the switch point

**Status:** ✅ **This works correctly.** The immediate switch-back is intentional - it creates a suspended tealet ready for duplication.

**Enhancement Opportunity:** `tealet_create()` was added to libtealet later and provides a cleaner API for this pattern:

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

**Benefits of Migration:**
- More explicit about the switch happening
- Better error handling between create and switch
- Clearer code intent

**Trade-offs:**
- Current code works fine and is more concise
- Migration requires testing to ensure behavior unchanged
- Negligible performance difference

**Priority:** P2 (Enhancement) - not critical, but could improve code clarity if migrating to newer libtealet patterns.

---

### 🔴 CRITICAL: Segfault in pytealet_get_main()

**Location:** Line 438

**Problem:** When a new PyTealetObject is created without an underlying tealet (STATE_NEW), accessing `t.main` causes a segfault:

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

**Current Behavior:**
```python
t = _tealet.tealet()  # STATE_NEW, tealet=NULL
print(t.main)  # SEGFAULT - dereferencing NULL->main
```

**Fix:** Must check if tealet exists:

```c
static PyObject *
pytealet_get_main(PyObject *_self, void *_closure)
{
    PyTealetObject *self = (PyTealetObject *)_self;
    
    if (!self->tealet) {
        // New tealet not yet initialized
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

---

### 🔴 CRITICAL: tealet_exit() Flag Mismatch

**Location:** `pytealet_main()` line 623-624

**Problem:** Code expects auto-delete but uses `TEALET_EXIT_DEFAULT` (no delete):

```c
/* clear the old tealet */
tealet->state = STATE_EXIT;
tealet->tealet = NULL; /* will be auto-deleted on return */
TEALET_SET_PYOBJECT(t_current, NULL);
t_return = return_to->tealet;

/* decref the objects after the switch */
dustbin_fill(t_return, func, (PyObject*)tealet, result);

Py_INCREF(return_arg);
if (tealet_exit(t_return, (void*)return_arg, TEALET_EXIT_DEFAULT))
    tealet_exit(t_return->main, (void *)return_arg, TEALET_EXIT_DEFAULT);
```

**API Documentation:**
```c
/* Exit flags */
#define TEALET_EXIT_DEFAULT 0  /* Don't auto-delete */
#define TEALET_EXIT_DELETE  1  /* Auto-delete on exit */
#define TEALET_EXIT_DEFER   2  /* Defer exit to return statement */
```

**Code Intent (Lines 617-624):**
1. **Line 617:** Comment says "will be auto-deleted on return"
2. **Line 617:** Sets `tealet->tealet = NULL` expecting libtealet to free it
3. **Line 272 (dealloc):** Only calls `tealet_delete()` if tealet pointer still exists
4. **Intent:** Exit tries to return to target tealet, or falls back to main if that fails

**The Bug:**
Using `TEALET_EXIT_DEFAULT` means the C-level tealet is **not** deleted by libtealet. This violates the design:
- The comment promises auto-delete
- Setting `tealet->tealet = NULL` assumes libtealet freed it
- `pytealet_dealloc()` won't clean it up (NULL check fails)
- **Result:** C-level tealet leaks memory

**Historical Context:** Original code was written when the default was auto-delete. The libtealet API evolved to require explicit `TEALET_EXIT_DELETE` flag.

**Fix:** Use `TEALET_EXIT_DELETE` to match the design intent:

```c
Py_INCREF(return_arg);
if (tealet_exit(t_return, (void*)return_arg, TEALET_EXIT_DELETE))
    tealet_exit(t_return->main, (void *)return_arg, TEALET_EXIT_DELETE);
```

**Why This Is Correct:**
- Matches the comment: "will be auto-deleted on return"
- Justifies setting `tealet->tealet = NULL` (libtealet owns deletion)
- Prevents memory leak of C-level tealet structure
- The fallback to main uses same deletion behavior (consistent)

---

### ✅ CORRECT: Resilient Cleanup in pytealet_dealloc()

**Location:** Line 256-265

**Design:** If a STATE_RUN tealet is deallocated (bad programming), issue a warning but continue cleanup:

```c
static void
pytealet_dealloc(PyObject *obj)
{
    PyTealetObject *tealet = (PyTealetObject *)obj;
    if (tealet->state == STATE_RUN) {
        int err = PyErr_WarnEx(PyExc_RuntimeWarning, 
                               "freeing an active tealet leaks memory", 1);
        if (err) {
            PyErr_WriteUnraisable(Py_None);
        }
    }
    // ... continues to free anyway
    if (tealet->tealet)
        tealet_delete(tealet->tealet);  // Best-effort cleanup
```

**Why This Is Correct:**

1. **Defensive Programming:** Like asyncio's warning when a coroutine isn't awaited, this handles programmer error gracefully
2. **Resilience over Perfection:** A warning is better than a crash or assertion failure in production
3. **Best-Effort Cleanup:** Calling `tealet_delete()` cleans up what it can, even if Python stack frames leak
4. **Clear Error Message:** The warning alerts the programmer to fix their code

**What Happens:**
- Warning alerts developer that the tealet's Python stack wasn't properly unwound
- `tealet_delete()` frees the C-level tealet structure and stack memory
- Python objects on the suspended stack may leak (their refcounts won't be decremented)
- The program continues running, avoiding a crash

**Analysis:** ✅ This is **intentional and correct** defensive programming. The alternative (fatal error) would make debugging harder and crash production code unnecessarily.

**Similar Patterns in Python:**
```python
# asyncio does this:
# RuntimeWarning: coroutine 'func' was never awaited

# pytealet does this:
# RuntimeWarning: freeing an active tealet leaks memory
```

---

## Memory Management Analysis

### Dustbin Pattern ✅

**Location:** Lines 193-211

**Purpose:** Defer Python object cleanup until after context switches.

**Design:** Brilliant! The "dustbin" pattern solves a critical problem:

1. **Problem:** When switching contexts, we can't decref Python objects before the switch (they might trigger code that uses the old stack) or during (stack is inconsistent)
2. **Solution:** Store objects to be decref'd in the main tealet's `main_data` structure
3. **Cleanup:** After switching and restoring the new context, safely decref the old objects

```c
static void dustbin_fill(tealet_t *tealet, PyObject *a, PyObject *b, PyObject *c)
{
    main_data *mdata = (main_data*)*tealet_main_userpointer(tealet);
    assert(!mdata->dustbin[0]);  // ✓ Detects double-fill bugs
    assert(!mdata->dustbin[1]);
    assert(!mdata->dustbin[2]);
    mdata->dustbin[0] = a;
    mdata->dustbin[1] = b;
    mdata->dustbin[2] = c;
}
```

**Analysis:**
- ✅ Solves the problem correctly
- ✅ Assertions detect misuse
- ⚠️ Limited to 3 objects - could be an issue if more needed
- ✅ Used consistently in `pytealet_run()` and `pytealet_switch()`

**Potential Improvement:** Use a dynamic array or free-list instead of fixed 3 slots.

---

### Reference Counting Strategy ✅

**Location:** `pytealet_main()` lines 555-557

**Design Pattern:**

```c
/* We only have borrowed references from the calling tealet.
 * the argument to the function will get their own reference, but
 * anything we need after the function we keep our own references
 * for, because when the function returns, the calling tealet
 * may have exited and dropped the references we borrowed.
 */
Py_INCREF(func);
Py_INCREF(tealet);
```

**Analysis:**
- ✅ **Excellent:** Recognizes that the caller might exit before we return
- ✅ Takes ownership of references needed after the call
- ✅ Puts them in the dustbin for cleanup after switch
- ✅ This shows deep understanding of the problem domain

**Correctness:** This is exactly right for a continuation-based system.

---

### Main Tealet Initialization ⚠️

**Location:** `GetMain()` lines 632-672

**Current Flow:**
1. Check TLS for existing main tealet
2. If none, call `tealet_initialize()` with `sizeof(tealet_extra_t)`
3. Allocate `main_data` separately
4. Store `main_data*` in `tealet_main_userpointer()`
5. Create PyTealetObject wrapper
6. Store PyTealetObject* in `tealet->extra` (via `TEALET_SET_PYOBJECT`)

**Design (Correct):**

1. **Two Separate Storage Mechanisms (By Design):**
   - `tealet->extra` → Per-tealet data: `tealet_extra_t{PyTealetObject*}`
     - Each tealet (main and children) gets this
     - Stores the back-pointer to the Python wrapper
   - `tealet_main_userpointer()` → Per-thread data: `main_data*`
     - **One instance per thread** (stored in the main tealet only)
     - Shared by all tealets in the thread's family
     - Contains thread ID and dustbin for deferred cleanup

2. **Why This Makes Sense:**
   ```c
   tmain = tealet_initialize(&talloc, sizeof(tealet_extra_t));
   ```
   This is correct! `sizeof(tealet_extra_t)` is for the per-tealet data.
   
   ```c
   mdata = (main_data*)PyMem_Malloc(sizeof(*mdata));
   *tealet_main_userpointer(tmain) = (void*)mdata;
   ```
   This separate allocation is also correct - it's thread-global data.

3. **Access Patterns:**
   - **Per-tealet access:** `TEALET_PYOBJECT(tealet)` gets the PyTealetObject for any tealet
   - **Thread-global access:** `*(main_data**)tealet_main_userpointer(any_tealet)` gets the shared dustbin

**Analysis:** ✅ This design is **correct and elegant**:
- `extra` scales with number of tealets (minimal overhead)
- `main_userpointer` is singleton per thread (shared state)
- Dustbin is accessible from any tealet in the family without searching
- Clean separation of per-tealet vs per-thread data

---

## State Machine Analysis

### States Defined:

```c
#define STATE_NEW 0    // Created but not initialized
#define STATE_STUB 1   // Duplicatable template
#define STATE_RUN 2    // Currently active
#define STATE_EXIT 3   // Exited/finished
```

### State Transitions:

```
NEW ──────────────┐
│                 │
│  .stub()        │  .run() with NEW
│                 │
▼                 ▼
STUB ────────► RUN ────► EXIT
      .run()        returns
```

### State Validation ✅

**pytealet_new():**
```c
if (src->state != STATE_NEW && src->state != STATE_STUB) {
    PyErr_SetString(StateError, "state must be new or stub");
    return NULL;
}
```
✅ Correct: Can only duplicate NEW or STUB

**pytealet_stub():**
```c
if (pytealet->state != STATE_NEW) {
    PyErr_SetString(StateError, "must be new");
    return NULL;
}
```
✅ Correct: Can only create stub from NEW

**pytealet_run():**
```c
if (target->state != STATE_NEW && target->state != STATE_STUB) {
    PyErr_SetString(StateError, "must be new or stub");
    return NULL;
}
```
✅ Correct: Can only run NEW or STUB

**pytealet_switch():**
```c
if (self->state != STATE_RUN) {
    PyErr_SetString(StateError, "must be active");
    return NULL;
}
```
✅ Correct: Can only switch to RUN

### Missing State Transition: EXIT → NEW?

Currently, once a tealet reaches EXIT, it cannot be reused. This might be intentional, but could be worth considering whether stubs should be resettable.

---

## Python Thread State Management

### Save/Restore Pattern ✅

**Location:** Lines 137-184

**Design:**
```c
save_tstate(current, tstate);
// ... do tealet operation ...
restore_tstate(current, tstate);
```

**What's Saved:**
- `frame` - Current Python stack frame
- `recursion_depth` - Python recursion counter
- `curexc_type/value/traceback` - Current exception (Python 3.7+)

**Analysis:**
- ✅ Uses correct Python 3.7+ fields (`curexc_*` not `exc_*`)
- ✅ Clears old state to prevent contamination
- ✅ Assertions verify invariants
- ⚠️ **frame is a weak reference** - comment says "leave tealet->frame alone, it's a weakref" but code doesn't actually increment refcount. This is correct for frames but worth documenting better.

**Potential Issue:** If a frame outlives its tealet, the weak reference becomes dangling. But since frames hold references to their code, this should be OK as long as the frame is cleared before tealet deletion.

---

## Stub Mechanism Analysis

### Purpose:
Create a template tealet that can be duplicated multiple times for reuse.

### Implementation:

```c
static tealet_t *stub_main(tealet_t *current, void *arg)
{
    void *myarg = 0;
    tealet_switch((tealet_t*)arg, &myarg);  // Switch back to caller
    
    // Now we're running for real (after duplication)
    struct stub_arg sarg = *(struct stub_arg*)myarg;
    tealet_free(sarg.current, myarg);
    return (sarg.run)(sarg.current, sarg.runarg);
}
```

**Flow:**
1. `stub_new()` creates a tealet with `stub_main` as run function
2. Immediately switches back to caller (creating a suspended tealet)
3. Later, someone calls `stub_run()` which switches to the stub
4. Stub receives arguments and calls the real run function
5. Real function executes

**Analysis:**
- ✅ Clever trampoline pattern
- ✅ Allows argument passing through heap allocation
- ⚠️ **But: Uses `tealet_new()` which switches** - should use `tealet_create()`

**Memory Management:**
```c
struct stub_arg *psarg = (struct stub_arg*)tealet_malloc(stub, sizeof(struct stub_arg));
```
✅ Correct: Uses tealet's allocator for cross-context data  
✅ Freed after use: `tealet_free(sarg.current, myarg);`

---

## API Usage Correctness

### Comparing Current Usage vs libtealet v0.3.2 API

| Function | Current Usage | API Expectation | Status |
|----------|---------------|-----------------|--------|
| `tealet_initialize()` | ✅ Called with correct signature | `(alloc, extrasize)` | ✅ CORRECT |
| `tealet_new()` | ❌ Expects create-only | Actually creates+switches | ❌ WRONG |
| `tealet_create()` | ⚠️ NOT USED | Create without switch | Should use this |
| `tealet_switch()` | ✅ Used correctly | `(target, **parg)` | ✅ CORRECT |
| `tealet_exit()` | ❌ Wrong flags | Should use TEALET_EXIT_DELETE | ❌ WRONG |
| `tealet_delete()` | ✅ Used in dealloc | Manual cleanup | ✅ CORRECT |
| `tealet_duplicate()` | ✅ Used for stubs | Duplicates tealet+stack | ✅ CORRECT |
| `tealet_current()` | ✅ Get current tealet | Returns current | ✅ CORRECT |
| `tealet_malloc/free()` | ✅ For cross-context data | Tealet allocator | ✅ CORRECT |

---

## Architectural Design Decisions

### 1. Two-Level Structure ✅

**Design:**
- `PyTealetObject` (Python-visible) wraps `tealet_t` (C-level)
- Bidirectional link via `tealet->extra->pytealet`

**Benefits:**
- ✅ Clean separation of concerns
- ✅ Python object lifecycle independent of tealet lifecycle
- ✅ Can create Python objects before/after tealet exists

**Potential Issue:**
- ⚠️ Complexity in keeping both sides synchronized
- ⚠️ State can get out of sync (e.g., STATE_EXIT but tealet still exists)

### 2. Stub Pattern for Reusability ✅

**Design:**
- STATE_STUB represents a template
- Can be duplicated multiple times
- Each duplicate gets fresh arguments

**Benefits:**
- ✅ Efficient: Create expensive setup once, duplicate cheaply
- ✅ Matches greenlet semantics
- ✅ Clever use of stack duplication

**Drawback:**
- ⚠️ Complexity: trampoline indirection makes it harder to understand
- ⚠️ Uses API incorrectly (`tealet_new` vs `tealet_create`)

### 3. Main Tealet per Thread ✅

**Design:**
```c
static int tls_key;  // Thread-local storage
PyTealetObject *t_main = (PyTealetObject*)PyThread_get_key_value(tls_key);
```

**Benefits:**
- ✅ Correct: Each OS thread gets its own tealet hierarchy
- ✅ Prevents cross-thread switching (which would segfault)
- ✅ Matches libtealet's threading model

**Issue:**
- ⚠️ Uses deprecated `PyThread_*` API (Python 3.7+)
- Should use Python 3.7+ threading API, but this still works

---

## Design Improvement Suggestions

### 1. ~~Unified Extra Data Structure~~ (Not Needed - Design is Correct)

The current design correctly uses two separate storage mechanisms:
- `tealet->extra`: Per-tealet data (PyTealetObject pointer)
- `main_userpointer()`: Per-thread shared data (dustbin, thread ID)

This is the right approach and should not be changed.

### 2. Use tealet_create() Instead of tealet_new()

**Current:**
```c
static tealet_t *stub_new(tealet_t *t) {
    void *arg = (void*)tealet_current(t);
    return tealet_new(t, stub_main, &arg);  // Switches!
}
```

**Proposed:**
```c
static tealet_t *stub_new(tealet_t *t) {
    tealet_t *stub = tealet_create(t, stub_main);
    if (!stub) return NULL;
    
    // Initialize and switch once to set up
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
- Explicit about when switching happens
- Easier to understand control flow
- Allows error handling between create and switch

### 3. Explicit Tealet Cleanup

**Current:**
```c
if (tealet_exit(t_return, (void*)return_arg, TEALET_EXIT_DEFAULT))
    tealet_exit(t_return->main, (void *)return_arg, TEALET_EXIT_DEFAULT);
```

**Proposed:**
```c
// Use TEALET_EXIT_DELETE to auto-cleanup
if (tealet_exit(t_return, (void*)return_arg, TEALET_EXIT_DELETE))
    tealet_exit(t_return->main, (void *)return_arg, TEALET_EXIT_DELETE);
```

**Benefits:**
- Automatic cleanup
- Matches expected behavior (comment says "auto-deleted on return")
- No manual tracking needed

### 4. Safer Property Access

**Current:**
```c
static PyObject *pytealet_get_main(PyObject *_self, void *_closure)
{
    PyTealetObject *self = (PyTealetObject *)_self;
    PyTealetObject *main = TEALET_PYOBJECT(self->tealet->main);  // SEGFAULT if tealet==NULL
    Py_INCREF(main);
    return (PyObject*)main;
}
```

**Proposed:**
```c
static PyObject *pytealet_get_main(PyObject *_self, void *_closure)
{
    PyTealetObject *self = (PyTealetObject *)_self;
    PyTealetObject *main;
    
    if (!self->tealet) {
        // Not yet initialized, return thread's main
        main = GetMain();
        if (!main) return NULL;
    } else {
        main = TEALET_PYOBJECT(self->tealet->main);
    }
    
    Py_INCREF(main);
    return (PyObject*)main;
}
```

### 5. Dynamic Dustbin

**Current:**
```c
PyObject *dustbin[3];  // Fixed size
```

**Proposed:**
```c
typedef struct dustbin_t {
    PyObject **objects;
    size_t count;
    size_t capacity;
} dustbin_t;
```

**Benefits:**
- No arbitrary limit
- Can handle complex cleanup scenarios
- Trade-off: More complexity, allocations

**Counter-argument:** Fixed size of 3 is probably enough for 99% of cases, keep it simple.

---

## Testing Recommendations

### 1. State Transition Tests
```python
def test_state_transitions():
    t = tealet.tealet()
    assert t.state == STATE_NEW
    
    t.stub()
    assert t.state == STATE_STUB
    
    # Should be able to duplicate
    t2 = tealet.tealet(t)
    assert t2.state == STATE_STUB
```

### 2. Null Pointer Tests
```python
def test_new_tealet_properties():
    t = tealet.tealet()
    # Should not segfault:
    main = t.main  # Currently segfaults!
    assert main is not None
```

### 3. Memory Leak Tests
```python
def test_cleanup():
    import gc
    
    # Create and abandon active tealet
    def worker(t, arg):
        pass
    
    t = tealet.tealet()
    # Don't store t, let it get GC'd while active
    # Should warn, not crash
```

### 4. Thread Safety Tests
```python
def test_thread_isolation():
    import threading
    
    def thread_func():
        t = tealet.tealet()
        # Each thread should have its own main
        return tealet.main()
    
    mains = []
    threads = [threading.Thread(target=lambda: mains.append(thread_func())) 
               for _ in range(4)]
    # All should succeed, no crashes
```

---

## Priority Fix List

### P0 - Critical (Causes Crashes/Corruption)
1. 🔴 **Fix pytealet_get_main() segfault** - Check for NULL tealet
2. 🔴 **Fix tealet_exit() flags** - Use TEALET_EXIT_DELETE

### P1 - High (Incorrect Behavior)
4. **Add NULL checks in property getters** - Prevent segfaults on STATE_NEW tealets

### P2 - Medium (Improvements)
5. **Migrate to tealet_create() API** - Use newer, more explicit API for stub creation
6. Replace deprecated PyThread_* API with modern alternatives
7. Add comprehensive state validation
8. Improve error messages

### P3 - Low (Nice to Have)
9. Dynamic dustbin
10. Better documentation
11. Add debug logging

---

## Conclusion

The original design (from 2013) is **architecturally sound** and shows sophisticated understanding of the problem domain:
- ✅ Dustbin pattern for deferred cleanup
- ✅ Careful reference counting across contexts
- ✅ Stub mechanism for reusability
- ✅ Thread-local main tealets

However, there are **critical bugs** that need immediate attention:
- 🔴 Segfault in property access
- 🔴 API misuse (tealet_new vs tealet_create)
- 🔴 Wrong exit flags (no auto-delete)
- 🔴 Memory layout confusion (dual extra data)

The code has "aged" in that libtealet's API has evolved:
- `tealet_new()` now switches immediately
- Exit flags changed meanings
- Extra data mechanism formalized

**Recommendation:** Fix the P0 issues before any further testing or deployment. The code cannot run correctly in its current state.

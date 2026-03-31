# PyTealet Architecture Documentation

**Original Code Date:** ~2013  
**libtealet Version:** 0.3.2  
**Python Version Target:** 3.10+

## Overview

The pytealet.c code is a Python C extension that wraps libtealet to provide stack-slicing coroutines to Python. This document describes the design, architecture, and intended operation of the system.

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

## Core Architecture

### Two-Level Object Structure

**Design:**
- `PyTealetObject` (Python-visible) wraps `tealet_t` (C-level)
- Bidirectional link via `tealet->extra->pytealet`

**Benefits:**
- Clean separation of concerns
- Python object lifecycle independent of tealet lifecycle
- Can create Python objects before/after tealet exists

**Independent Lifecycles:**

```python
# Example lifecycle:
t = _tealet.tealet()              # PyTealetObject created, tealet=NULL, state=NEW
t.run(some_function, arg)         # Creates tealet_t, state=RUN
# Function returns...              # tealet_t auto-deleted, tealet=NULL, state=EXIT
print(t.state)                    # Still works! Returns STATE_EXIT
del t                             # PyTealetObject deallocated
```

The Python object persists even after the C-level tealet is deleted, allowing safe access to state information.

---

## Memory Management

### Dustbin Pattern

**Location:** Lines 193-211

**Purpose:** Defer Python object cleanup until after context switches.

**The Problem:**
When switching contexts, we can't safely decref Python objects:
- **Before switch:** Decref might trigger code that uses the old stack
- **During switch:** Stack is in inconsistent state
- **After switch:** Old stack is gone, can't safely run cleanup code

**The Solution:**
Store objects to be decref'd in the main tealet's `main_data` structure (the "dustbin"), then safely decref them after the context switch completes and the new stack is established.

```c
static void dustbin_fill(tealet_t *tealet, PyObject *a, PyObject *b, PyObject *c)
{
    main_data *mdata = (main_data*)*tealet_main_userpointer(tealet);
    assert(!mdata->dustbin[0]);  // Detects double-fill bugs
    assert(!mdata->dustbin[1]);
    assert(!mdata->dustbin[2]);
    mdata->dustbin[0] = a;
    mdata->dustbin[1] = b;
    mdata->dustbin[2] = c;
}

static void dustbin_empty(tealet_t *tealet)
{
    main_data *mdata = (main_data*)*tealet_main_userpointer(tealet);
    Py_XDECREF(mdata->dustbin[0]);
    Py_XDECREF(mdata->dustbin[1]);
    Py_XDECREF(mdata->dustbin[2]);
    mdata->dustbin[0] = mdata->dustbin[1] = mdata->dustbin[2] = NULL;
}
```

**Usage Pattern:**
1. Before context switch: Fill dustbin with objects to cleanup
2. Perform context switch
3. After switch completes: Empty dustbin to decref objects

**Design Trade-offs:**
- ✅ Solves the problem correctly
- ✅ Assertions detect misuse
- ⚠️ Limited to 3 objects (sufficient for current usage)
- ✅ Used consistently in `pytealet_run()` and `pytealet_switch()`

---

### Reference Counting Strategy

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

**Why This Matters:**
In a continuation-based system, the caller might exit before we return. Therefore:
- Take ownership of references needed after the call
- Put them in the dustbin for cleanup after switch
- Never rely on borrowed references surviving a context switch

This shows deep understanding of the coroutine problem domain.

---

### Storage Architecture

**Location:** `GetMain()` lines 632-672

The design uses two separate storage mechanisms:

**1. Per-Tealet Data: `tealet->extra`**
```c
typedef struct tealet_extra {
    PyTealetObject *pytealet;
} tealet_extra_t;

#define TEALET_PYOBJECT(t) (((tealet_extra_t*)tealet_extra(t))->pytealet)
#define TEALET_SET_PYOBJECT(t, obj) (((tealet_extra_t*)tealet_extra(t))->pytealet = (obj))
```

- Each tealet (main and children) gets this
- Stores the back-pointer to the Python wrapper
- Allocated as part of tealet creation via `sizeof(tealet_extra_t)`

**2. Per-Thread Data: `main_userpointer()`**
```c
typedef struct main_data {
    long tid;               // OS thread ID
    PyObject *dustbin[3];   // Deferred cleanup
} main_data;
```

- **One instance per thread** (stored in the main tealet only)
- Shared by all tealets in the thread's family
- Contains thread ID and dustbin for deferred cleanup
- Allocated separately via `PyMem_Malloc()`

**Access Patterns:**
- **Per-tealet access:** `TEALET_PYOBJECT(tealet)` gets the PyTealetObject for any tealet
- **Thread-global access:** `*(main_data**)tealet_main_userpointer(any_tealet)` gets the shared dustbin

**Why This Design:**
- `extra` scales with number of tealets (minimal overhead per tealet)
- `main_userpointer` is singleton per thread (shared state)
- Dustbin is accessible from any tealet in the family without searching
- Clean separation of per-tealet vs per-thread concerns

---

## State Machine

### States Defined:

```c
#define STATE_NEW 0    // Created but not initialized
#define STATE_STUB 1   // Duplicatable template
#define STATE_RUN 2    // Currently active/runnable
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

### State Validation

The code enforces valid state transitions:

**Creating duplicates (pytealet_new):**
```c
if (src->state != STATE_NEW && src->state != STATE_STUB) {
    PyErr_SetString(StateError, "state must be new or stub");
    return NULL;
}
```
Can only duplicate NEW or STUB tealets.

**Creating stubs (pytealet_stub):**
```c
if (pytealet->state != STATE_NEW) {
    PyErr_SetString(StateError, "must be new");
    return NULL;
}
```
Can only create stub from NEW.

**Running (pytealet_run):**
```c
if (target->state != STATE_NEW && target->state != STATE_STUB) {
    PyErr_SetString(StateError, "must be new or stub");
    return NULL;
}
```
Can only run NEW or STUB.

**Switching (pytealet_switch):**
```c
if (self->state != STATE_RUN) {
    PyErr_SetString(StateError, "must be active");
    return NULL;
}
```
Can only switch to RUN.

---

## Python Thread State Management

### Save/Restore Pattern

**Location:** Lines 137-184

**What's Saved Across Context Switches:**
- `frame` - Current Python stack frame
- `recursion_depth` - Python recursion counter
- `curexc_type/value/traceback` - Current exception (Python 3.7+)

**Design:**
```c
save_tstate(current, tstate);
// ... do tealet operation ...
restore_tstate(current, tstate);
```

**Key Details:**
- Uses correct Python 3.7+ fields (`curexc_*` not `exc_*`)
- Clears old state to prevent contamination
- Assertions verify invariants
- Frame is a weak reference (not refcounted)

---

## Stub Mechanism

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

### Flow:
1. `stub_new()` creates a tealet with `stub_main` as run function
2. Immediately switches back to caller (creating a suspended tealet)
3. Later, someone calls `stub_run()` which switches to the stub
4. Stub receives arguments and calls the real run function
5. Real function executes

### Benefits:
- Efficient: Create expensive setup once, duplicate cheaply
- Matches greenlet semantics
- Clever use of stack duplication via `tealet_duplicate()`

### Memory Management:
```c
struct stub_arg *psarg = (struct stub_arg*)tealet_malloc(stub, sizeof(struct stub_arg));
```
Uses tealet's allocator for cross-context data, freed after use:
```c
tealet_free(sarg.current, myarg);
```

---

## Threading Model

### Main Tealet per Thread

**Design:**
```c
typedef struct PyTealetModuleState {
    Py_tss_t tls_key;
    ...
} PyTealetModuleState;

PyTealetObject *t_main = (PyTealetObject*)PyThread_tss_get(&mstate->tls_key);
```

**Properties:**
- Each module instance (and therefore each interpreter) has its own TLS key
- Each OS thread gets its own tealet hierarchy
- Prevents cross-thread switching (which would corrupt stacks)
- Matches libtealet's threading model
- Thread ID stored in `main_data` for validation

**Thread Safety:**
Tealets can only switch within the same thread family. Cross-thread switches are detected and raise `InvalidError`.

### API Direction: Tealet-Centric Introspection

Design choice: keep the explicit tealet callback hook (libtealet-style) in `run` rather than relying only on module-level lookups.

Planned direction:
- Prefer obtaining family/context relationships from a tealet object itself.
- Add more tealet-facing methods/properties mirroring libtealet concepts (`current`, `prev`, and related family/navigation hooks).
- Reduce dependence on TLS/global/module lookups for routine relationship queries.

Rationale:
- Keeps behavior aligned with libtealet semantics.
- Makes control-flow relationships explicit at call sites.
- Improves portability to multi-interpreter scenarios by favoring object-local navigation over ambient globals.

### Thread Shutdown Considerations (Deferred)

Current direction is **TLS-per-module-state** (`mstate->tls_key`) so main-tealet ownership is scoped per interpreter/module state.

⚠️ **Known deferred work:**
- We currently do not run full per-thread teardown of the TLS-stored `main` tealet object.
- `PyThread_tss_delete()` in module free deletes the key, but does not automatically decref Python objects that may still be stored for live/terminated threads.
- Final design should define explicit ownership and thread-exit cleanup behavior **per mstate** (not globally), including when and where `PyThread_tss_set(&mstate->tls_key, NULL)` and final decref happen.

This is intentionally deferred while we complete mstate-plumbing and interpreter-isolation work.

---

## Tealet Lifecycle and Cleanup

### Normal Exit Path

When a tealet's run function returns:

1. **State transition:** `tealet->state = STATE_EXIT`
2. **C-level cleanup:** `tealet->tealet = NULL` (libtealet auto-deletes the C object)
3. **Dustbin:** Python objects put in dustbin for deferred cleanup
4. **Exit switch:** `tealet_exit()` switches to return target with `TEALET_EXIT_DELETE` flag
5. **Python object survives:** PyTealetObject remains alive for Python to query state

### Deallocation

```c
static void pytealet_dealloc(PyObject *obj)
{
    PyTealetObject *tealet = (PyTealetObject *)obj;
    
    // Defensive: warn if deallocating active tealet
    if (tealet->state == STATE_RUN) {
        PyErr_WarnEx(PyExc_RuntimeWarning, 
                     "freeing an active tealet leaks memory", 1);
    }
    
    // Clean up Python-level state
    Py_XDECREF(tealet->exc_type);
    Py_XDECREF(tealet->exc_val);
    Py_XDECREF(tealet->exc_tb);
    
    // Clean up C-level tealet if still exists
    if (tealet->tealet)
        tealet_delete(tealet->tealet);
    
    Py_TYPE(obj)->tp_free(obj);
}
```

**Defensive Programming:**
- Warning alerts developer if tealet wasn't properly exited
- Best-effort cleanup even in error case
- Resilient: doesn't crash, just warns and leaks Python stack objects
- Similar to asyncio's "coroutine was never awaited" warning

---

## libtealet API Usage

### Core Functions Used

| Function | Purpose | Usage Pattern |
|----------|---------|---------------|
| `tealet_initialize()` | Initialize main tealet | Called once per thread |
| `tealet_new()` | Create and switch to new tealet | Used in stub creation |
| `tealet_switch()` | Switch to target tealet | Core switching primitive |
| `tealet_exit()` | Exit and switch | Used when run function returns |
| `tealet_delete()` | Manual cleanup | Used in error paths and dealloc |
| `tealet_duplicate()` | Duplicate stub | Used for creating from STUB |
| `tealet_current()` | Get current tealet | Used throughout |
| `tealet_malloc/free()` | Cross-context allocation | Used for stub arguments |

### Exit Flags

```c
#define TEALET_EXIT_DEFAULT 0  /* Don't auto-delete */
#define TEALET_EXIT_DELETE  1  /* Auto-delete on exit */
#define TEALET_EXIT_DEFER   2  /* Defer exit to return statement */
```

The code uses `TEALET_EXIT_DELETE` to have libtealet automatically clean up the C-level tealet structure when exiting, which justifies setting `tealet->tealet = NULL` in the exit path.

---

## Design Patterns and Best Practices

### 1. Separation of Lifecycles

Python objects outlive C objects, allowing safe access to state even after the underlying tealet is deleted.

### 2. Deferred Cleanup (Dustbin)

Never decref Python objects during a context switch - defer until the switch completes.

### 3. Reference Ownership

Take ownership of references that must survive context switches; never rely on borrowed references.

### 4. State Machine Enforcement

Explicit state validation prevents invalid operations and provides clear error messages.

### 5. Defensive Programming

Warn on programmer errors but remain resilient; don't crash production code unnecessarily.

### 6. Thread Isolation

Each thread gets its own tealet family; cross-thread operations are explicitly forbidden.

---

## Comparison with greenlet

PyTealet provides a greenlet-compatible interface while using libtealet as the underlying implementation:

**Similarities:**
- Cooperative coroutines without async/await keywords
- Stack switching and preservation
- Greenlet-compatible API via `tealet.greenlet` module

**Differences:**
- Built on libtealet instead of direct assembly/platform code
- May have different performance characteristics
- Uses tealet's memory allocator interface
- Supports custom allocators via `tealet_alloc_t`

---

## Performance Characteristics

From libtealet (targets for Python wrapper):
- Context switch: ~100-500 CPU cycles
- Memory per coroutine: ~2-16 KB (incremental growth)
- No kernel involvement (pure user-space)
- Manual scheduling (deterministic)

---

## Summary

The pytealet architecture demonstrates sophisticated understanding of coroutine implementation:

✅ **Dustbin pattern** - Elegant solution to deferred cleanup problem  
✅ **Dual lifecycle** - Python objects survive C object deletion  
✅ **Reference discipline** - Careful ownership across context switches  
✅ **Stub mechanism** - Efficient template-based duplication  
✅ **Thread safety** - Per-thread isolation with clear boundaries  
✅ **State validation** - Explicit state machine with helpful errors  
✅ **Defensive design** - Resilient to programmer errors

The design is architecturally sound and shows deep understanding of the stack-slicing problem domain.

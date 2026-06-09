# PyTealet Architecture Documentation

**Original Code Date:** ~2013  
**libtealet Version:** 0.7.5  
**Python Version Target:** 3.10+

## Overview

The `_tealet` extension is a Python C extension that wraps libtealet to provide stack-slicing coroutines to Python. Its implementation is split across several C files and shared headers. This document describes the design, architecture, and intended operation of the system.

### Current C Source Layout

- `src/_tealet/pytealet.c`: Core runtime logic for tealet objects (run/switch paths and active runtime helpers)
- `src/_tealet/pytealet_module.c`: CPython module lifecycle (module functions and init/exec/traverse/clear/free)
- `src/_tealet/tstate_state.c`: Save/restore helpers for `PyThreadState` fields across switches
- `src/_tealet/frame_info.c`: Dormant-frame capture and 3.12+ rewrite/restore support
- `src/_tealet/pytealet_common.h`: Shared compile-time version/feature macros and compatibility typedefs
- `src/_tealet/pytealet.h`: Shared internal API declarations exported between extension translation units
- `src/_tealet/pytealet_module.h`: Per-module state layout shared by module/runtime sources

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
_tealet.previous() -> tealet | None
```
Returns the tealet that most recently switched to the current tealet, or
`None` when there is no previous tealet.

```python
_tealet.thread_reap() -> list[tealet]
```
Explicitly tears down this thread's tealet lineage and returns wrappers whose
native tealet handles were forcibly invalidated. Intended for optional worker
thread shutdown cleanup.
- Must be called from the thread's main tealet context.
- Return order is unspecified.
- Idempotent for already-cleaned thread state (returns an empty list).

```python
_tealet.frame_introspection([enabled]) -> bool
```
Gets or sets dormant-tealet frame introspection at runtime.
- With no argument, returns current setting.
- With one argument, sets and returns the new setting.
- Enabling can raise `RuntimeError` when compile-time support is disabled.

```python
_tealet.error_was_remote() -> bool
```
Returns whether the most recently raised exception from this thread's switching
APIs (`run()`, `switch()`, `throw()`) was remotely delivered through queued
exception injection (`set_exception`/`throw`), rather than raised locally as an
operational/runtime error.

Behavior notes:
- The flag is thread-local and lineage-scoped.
- The flag is cleared at entry to each switching API (`run`, `switch`, `throw`).
- If there is no active lineage (no thread main), result is `False`.

**Module-Level Classes:**

```python
class _tealet.tealet()
```
The core tealet class. Creates a NEW tealet wrapper in the current lineage.

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

```python
tealet.set_exception(exception, fallback=None) -> None
```
Queues an exception instance to be delivered on the next switch boundary into
this tealet lineage.
- `exception` must be an exception instance.
- `fallback` (optional) is a tealet to resume if the injected exception is not
    caught by the target flow.

```python
tealet.throw(exception) -> result
```
Convenience API that combines injection and transfer.
- `STATE_RUN` target: equivalent to `set_exception(exception, fallback=current)`
    followed by `switch()`.
- `STATE_NEW`/`STATE_STUB` target: equivalent to
    `set_exception(exception, fallback=current)` followed by `run()`.
- `run()` accepts missing callable arguments in this case because the queued
    exception is delivered before worker-call dispatch.

```python
tealet.is_foreign() -> bool
```
Returns whether this tealet object is owned by a different thread.

**Tealet Object Properties:**

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

For suspended tealets, frame exposure is best-effort and version-dependent:
- Python 3.10 uses saved `PyThreadState` frame references.
- Python 3.11+ captures frame information via the dedicated frame-info path.
- Python 3.12+ may temporarily rewrite frame links to hide unsafe internal frames while a dormant tealet is being introspected.

Runtime policy can additionally disable dormant-frame exposure through
`_tealet.frame_introspection(False)`. In that mode, suspended tealets report
`None` for `.frame` while active/current-frame lookups still behave normally.

```python
tealet.thread_id -> int
```
The owning thread ID for this tealet object. This is set when the object is
created and is used for cross-thread API guards.

```python
tealet.context -> contextvars.Context | None
```
Execution context for the tealet. This is a first-class, assignable property.

Context semantics:
- New tealets start with `context = None`.
- A tealet in `STATE_NEW` or `STATE_STUB` can have `context` assigned before
    `run()`. That context is then used when the tealet starts executing.
- A suspended tealet (`STATE_RUN` but not currently executing) can have
    `context` replaced; the updated context is used when it is resumed.
- Setting `context = None` clears the tealet context.
- The value must be `contextvars.Context` or `None`; other values raise
    `TypeError`.
- Cross-thread context access is allowed for non-running tealets
    (`STATE_NEW`, `STATE_STUB`, or suspended `STATE_RUN`) under the lineage
    domain lock.
- Cross-thread context access to a currently running tealet raises
    `InvalidError`.

Thread ownership rules enforced by the C API:
- Creating a tealet object ensures a thread-main tealet exists for that thread.
- `run()` and `switch()` do not auto-create a main tealet in the caller thread;
    they validate ownership and reject foreign-thread use.
- `stub()` is only allowed from the owning thread.
- Volatile traversal methods (`current()`, `main()`, `previous()`) are only
    allowed from the owning thread.
- Cross-thread duplication via `existing_tealet.duplicate()` is allowed
    when `existing_tealet` is in `STATE_STUB` or `STATE_NEW`.
- Duplicating a `STATE_STUB` tealet preserves the existing dormant/stub
    execution context for use by the new wrapper.
- Duplicating a `STATE_NEW` tealet is also supported; it duplicates an
    unstarted tealet wrapper and preserves its not-yet-run status until it is
    run from its owning thread.
- Wrapper deallocation is allowed across threads.

**Module Constants:**

```python
_tealet.STATE_NEW = 0
_tealet.STATE_STUB = 1
_tealet.STATE_RUN = 2
_tealet.STATE_EXIT = 3
_tealet.PYTEALET_WITH_PENDING_FRAME_INTROSPECTION = 0 | 1
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
Raised when a thread-restricted operation is called from a different thread
family.

```python
_tealet.StateError(TealetError)
```
Raised when an operation is invalid for the tealet's current state.

```python
_tealet.PanicError(TealetError)
```
Raised for panic-transfer failures (for example `switch(..., panic=True)`).

`PanicError` payload methods:
- `.result()`: returns panic transfer payload if no injected exception was
    attached; raises that injected exception otherwise.
- `.exception()`: returns the pending injected exception consumed from the
    throw queue when panic translation occurs, or `None`.

Operational-vs-remote distinction:
- Remote delivered exception (from pending throw queue): raised as the injected
    exception instance itself, and `_tealet.error_was_remote()` becomes `True`
    until the next switching API call.
- Local operational/runtime failure (state/invalid/defunct/panic translation):
    raised as `_tealet` operational exception classes, and
    `_tealet.error_was_remote()` remains `False`.

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

### API Layering Policy (Python and Capsule C API)

The runtime follows a two-front-door pattern:
- Python-bound methods (for example, tealet.run, tealet.prepare) are responsible for Python call-shape parsing and Python-facing argument errors.
- Capsule C API entrypoints (for example, PyTealetApi_Run, PyTealetApi_Prepare) are responsible for C-facing validation at the exported boundary.
- Shared runtime state transitions and execution behavior live in internal implementation helpers (for example, operation-specific impl/dispatch helpers).

This keeps core semantics centralized while allowing each public boundary to retain its own contract and diagnostics.

Practical rule:
- Avoid implementing runtime behavior in both public entrypoints.
- Keep boundary checks at the boundary, and keep stateful behavior in one shared internal path.

### Physical Layout Guidance

Preferred source layout is operation-local grouping, keeping related variants near each other.

For a given operation, place code in this order when practical:
1. Shared internal implementation helper (impl/dispatch).
2. Python method wrapper.
3. Capsule C API wrapper.

Rationale:
- Reduces navigation cost during review and debugging.
- Makes it obvious which logic is shared vs boundary-specific.
- Lowers regression risk when changing state transitions or refcount-sensitive behavior.

Allow exceptions for very large helpers (for example, common dispatch used by several operations), but preserve clear references and naming so call flow remains easy to follow.

### Capsule C API Policy

The capsule C API is intended to be complementary to the Python API, with one
shared behavioral core.

Implementation ownership:
- `src/_tealet/pytealet.c` owns canonical runtime behavior and shared dispatch/impl helpers.
- `src/_tealet/pytealet_module.c` owns capsule export and thin forwarders only.
- `src/tealet/include/pytealet_capi.h` defines the public ABI shape.

Runtime consistency rules:
- Avoid duplicated state-machine logic between Python wrappers and C API wrappers.
- Wrapper-specific argument parsing and error wording may differ, but state
    transitions and transfer behavior must match.
- Features intentionally Python-only (for example, `hide_frame`) should remain
    explicitly documented as non-parity scope.

ABI policy:
- Pre-0.1.0: `PYTEALET_CAPI_ABI_VERSION` remains `1`, and table layout can
    evolve while the API is unreleased.
- At and after 0.1.0 API freeze: preserve slot ordering and evolve with
    append-only growth for compatible ABI updates.

Review and validation gates:
- Every new C API entrypoint should include capi-client coverage.
- New flags should include negative tests for unknown bits.
- Any table/struct reordering must update header and export table in lockstep.

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

**Location:** `src/_tealet/pytealet.c` (runtime helpers used by run/switch and tealet exit)

**Purpose:** Defer Python object cleanup until after context switches.

**The Problem:**
When switching contexts, we can't safely decref Python objects:
- **Before switch:** Decref might trigger code that uses the old stack
- **During switch:** Stack is in inconsistent state
- **After switch:** Old stack is gone, can't safely run cleanup code

**The Solution:**
Store objects to be decref'd in the main tealet's per-thread dustbin list, then safely decref them after the context switch completes and the new stack is established.

```c
void PyTealet_dustbin_push(tealet_t *tealet, PyObject *obj) {
    PyTealetMainData *mdata;
    if (!obj)
        return;
    if (!tealet) {
        Py_DECREF(obj);
        return;
    }
    mdata = (PyTealetMainData *)*tealet_main_userpointer(tealet);
    if (!mdata || !mdata->dustbin) {
        Py_DECREF(obj);
        return;
    }
    if (PyList_Append(mdata->dustbin, obj) < 0) {
        PyErr_WriteUnraisable(Py_None);
        PyErr_Clear();
    }
    Py_DECREF(obj);
}

static void dustbin_clear(tealet_t *tealet) {
    PyTealetMainData *mdata = (PyTealetMainData *)*tealet_main_userpointer(tealet);
    Py_ssize_t n = PyList_GET_SIZE(mdata->dustbin);
    if (n == 0)
        return;
    if (PyList_SetSlice(mdata->dustbin, 0, n, NULL) < 0) {
        PyErr_WriteUnraisable(Py_None);
        PyErr_Clear();
    }
}
```

**Usage Pattern:**
1. Before/around context switch: Push objects into dustbin as ownership is transferred
2. Perform context switch
3. After switch completes: Clear dustbin list to decref queued objects

**Design Trade-offs:**
- ✅ Solves the problem correctly
- ✅ Handles variable number of deferred decrefs
- ✅ Handles error paths with write-unraisable fallback
- ✅ Used consistently in `pytealet_run()` and `pytealet_switch()`

---

### Reference Counting Strategy

**Location:** `pytealet_main()` in `src/_tealet/pytealet.c`

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

---

## Shutdown and Teardown Ordering

### Main-tealet lifetime vs Python object lifetime

`tealet_finalize()` tears down the native main tealet and its runtime structures. In Python, however, `PyTealetObject` instances may outlive module shutdown because GC/refcount destruction order is not deterministic at interpreter teardown.

This creates a potential ordering hazard:

- Native layer may be finalized first (`tealet_finalize` / module teardown)
- Individual Python tealet wrappers may be deallocated later
- Late deallocation may still try to interact with native tealet pointers

### Current implication

pytealet must defensively handle stale-or-unavailable runtime state during late object cleanup. This is especially important for experimental modes that defer native tealet deletion to Python object teardown.

### Follow-up design note

If late wrapper cleanup after main-tealet finalization is required, pytealet may need an explicit per-wrapper invalidation/deletion path that does not depend on a live main tealet runtime. This likely requires additional ownership/invalidation bookkeeping in pytealet, and possibly small lifecycle API support in libtealet.

This shows deep understanding of the coroutine problem domain.

---

### Storage Architecture

**Location:** `GetMain()` in `src/_tealet/pytealet.c`

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
    PyTealetNewArg new_arg; // Staging area for tealet entry handoff
    PyObject *dustbin;      // Deferred cleanup list
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

**Location:** `src/_tealet/tstate_state.c` and `src/_tealet/tstate_state.h`

**What's Saved Across Context Switches:**
- Frame pointers / frame references (version-dependent)
- Recursion counters (version-dependent fields)
- Current exception and error-stack state
- Context vars and selected cframe/datastack fields where applicable

**Design:**
```c
PyTealetTstate_Save(&current->tstate, tstate);
// ... do tealet operation ...
PyTealetTstate_Restore(&current->tstate, tstate);
```

**Key Details:**
- Uses Python-version-gated field handling in one place (`tstate_state.c`)
- Keeps save/restore logic centralized to reduce cross-version drift
- Separates pure state capture from deferred decref/drop behavior

### Dormant Frame Query and Chain Isolation

**Locations:**
- `src/_tealet/frame_info.c` and `src/_tealet/frame_info.h`
- `src/_tealet/pytealet.c` (`pytealet_get_frame()` and tealet entry setup in `pytealet_main()`)

#### Why this exists

When a tealet is suspended, its execution stack may be stored in heap-backed slices and parts of the interpreter frame chain may be incomplete or C-stack-owned. If we expose frame links naively, Python's frame machinery can attempt to traverse or materialize frames that are not safe to walk in that state.

The architecture therefore separates:
- Frame capture for dormant introspection
- Runtime thread-state save/restore for switching
- Temporary frame-link sanitization for safe traversal in newer Python versions

#### Dormant frame capture model

On non-`PY_HAS_TSTATE_FRAME` builds, the frame-info path captures the currently visible frame with `PyEval_GetFrame()` and stores a strong reference in `PyTealetFrameInfo`.

On release, links are restored first, then the stored frame reference is dropped (or deferred via dustbin when appropriate).

#### Python 3.12+ frame-chain rewiring

For 3.12+, frame traversal safety requires additional filtering:
- The chain is scanned through `_PyInterpreterFrame->previous`.
- Incomplete frames (`_PyFrame_IsIncomplete`) are skipped.
- Frames owned by the C stack (`FRAME_OWNED_BY_CSTACK`) are skipped.
- Link rewrites are recorded as reversible edits (`location`, `old_value`).

This produces a temporary "safe" chain for introspection. Rewrites are always unwound in reverse order before releasing frame info.

If rewrite recording fails (for example, OOM), the implementation restores already-edited links and degrades to best-effort behavior rather than leaving a partially rewritten chain.

#### Tealet entry isolation (prevent walking outside function-rooted tealets)

At tealet entry, runtime setup intentionally detaches frame-walking roots from the parent context:
- On 3.11+, it installs a fresh top cframe view and clears `current_frame` plus datastack links.
- On 3.10, it clears `tstate->frame` (`Py_CLEAR(tstate->frame)`) before entering tealet code.

Rationale: frame walking from a function-rooted tealet must not "escape" into the caller's external stack chain. Entering with a clean top-of-chain boundary keeps introspection local to the tealet lineage and avoids cross-stack leakage.

---

## Stub Mechanism

### Purpose:
Create a paused tealet template that can be duplicated and then run with a chosen function.

### Implementation:

```c
/* libtealet (tealet_extras.c) */
tealet_t *tealet_stub_new(tealet_t *t, void *stack_far) {
    return tealet_create(t, _tealet_stub_main, stack_far);
}
```

The pytealet wrapper uses this API directly:
- `pytealet_stub()` calls `tealet_stub_new(main->tealet, stack_far)` and marks the wrapper as `STATE_STUB`.
- `pytealet_run()` uses `tealet_stub_run(target->tealet, pytealet_main, &switch_arg)` when the target is a stub.
- Duplicating a stub wrapper (`existing_stub.duplicate()`) duplicates native state with `tealet_duplicate()` and duplicates saved thread state.

### Flow:
1. `tealet_stub_new()` creates a paused tealet using `tealet_create(..., _tealet_stub_main, ...)`.
2. The stub can be duplicated (`tealet_duplicate`) before use.
3. `tealet_stub_run()` allocates a small `stub_arg`, stores `(current, run, runarg)`, and switches to the stub.
4. `_tealet_stub_main` receives `stub_arg`, frees it with tealet allocator, and tail-calls the requested run function.
5. In pytealet, that run function is `pytealet_main`, which performs Python-level run/switch semantics.

### Benefits:
- Efficient paused-template creation with explicit run dispatch.
- Supports duplicate-from-stub workflows naturally.
- Keeps stub-specific trampoline logic in libtealet helper APIs.

### Memory Management:
```c
struct stub_arg *psarg = (struct stub_arg*)tealet_malloc(stub, sizeof(struct stub_arg));
```
Uses tealet's allocator for cross-context data, freed after use:
```c
tealet_free(sarg.current, myarg);
```
If switching fails, `tealet_stub_run()` frees the allocation on the failure path as well.

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

### Domain Lock and Cross-Thread Consistency

Each lineage has one shared domain lock object stored in main-lineage data
(`mdata->domain_lock_obj`). Every `PyTealetObject` stores a strong reference to
that same lock (`tealet->domain_lock_obj`), including duplicated wrappers.

This gives per-wrapper access to the lineage lock and allows pytealet code to
guard wrapper state changes that may be observed across threads.

The same lock is also configured into libtealet via
`tealet_configure_set_locking(..., TEALET_LOCK_AUTO, ...)`, so internal
libtealet switching and pytealet-level state checks synchronize on the same
critical section.

Current usage includes guarding:
- Wrapper linkage publication/rollback (`wrapper->tealet` / `TEALET_SET_PYOBJECT`)
    in run/stub transitions.
- Wrapper linkage teardown during exit and thread cleanup.
- Wrapper tracking set mutation (`mdata->wrappers`).
- Context property reads/writes and running-state detection via
    `tealet_current(...) == wrapper->tealet`.

When this domain lock is held, `tealet_current`/GetCurrent-style relationship
queries are stable with respect to concurrent switches, so API checks that rely
on that identity are reliable.

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

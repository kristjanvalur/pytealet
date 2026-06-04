# pytealet C API Exploration (c-api branch)

Date: 2026-06-02

## Goal

Expose a native C API from _tealet so that other C extensions can call pytealet directly (for example, a stackless emulation layer), without routing through Python-level attribute lookups or Python-callable wrappers.

## Executive Summary

Recommended approach:

1. Export a versioned function-table C API through a PyCapsule named _tealet._C_API.
2. Keep per-interpreter/module state private to _tealet.
3. Expose opaque provider context handles (per interpreter) instead of exposing PyTealetModuleState.
4. Have client modules import the capsule at their module init and store API pointers/context in their own module state.
5. Avoid hard-linking client extensions against _tealet symbols.

This is the modern CPython-compatible pattern, works with multi-phase module init, and is the closest match to how major projects expose binary APIs.

## What We Have Today in pytealet

Current _tealet architecture already uses per-module state via multi-phase init:

- module state struct: src/_tealet/pytealet_module.h
- module m_size > 0 and Py_mod_exec: src/_tealet/pytealet_module.c
- frequent PyModule_GetState and PyType_GetModuleState usage: src/_tealet/pytealet_module.c and src/_tealet/pytealet.c

Key implication:

- The internal state type PyTealetModuleState is interpreter/module-instance specific and should remain internal.
- It is not a stable cross-extension ABI surface.

## Main Question: How to Handle mstate for External Callers?

Short answer: do not expose raw mstate to clients.

Instead:

1. _tealet owns mstate and resolves it internally.
2. Clients hold an opaque provider context (for their interpreter) obtained from _tealet API.
3. API entry points accept either:
   - ordinary Python objects (tealet instances, exceptions), and/or
   - an opaque context token when no object is available.

This avoids ABI breakage when internal state layout changes.

## Why Not Query mstate Directly From Clients?

Possible but not recommended.

Reasons:

1. Internal layout lock-in: exposing PyTealetModuleState freezes internals.
2. Multi-phase init details leak into clients.
3. Future changes (new fields, lock strategy, free-threaded support changes) become ABI hazards.
4. Safer layering is provider-owned state + opaque handles.

Also important:

- PyState_FindModule is for legacy single-phase patterns and is not the right primitive for multi-phase modules.

## Linking Strategy

### Recommended: Runtime capsule import (no hard link)

Use PyCapsule_Import("_tealet._C_API", 0) from client module init.

Pros:

1. Portable across platforms and extension loader visibility models.
2. Avoids symbol-visibility and shared-library link complications.
3. Lets provider perform runtime ABI/version checks.
4. Matches CPython guidance for "Providing a C API for an Extension Module".

### Not Recommended: Direct hard linking against _tealet symbols

Problems:

1. Platform-specific symbol export/visibility pitfalls.
2. Wheel/distribution fragility.
3. Tighter coupling to build/link settings.
4. Harder compatibility story across package upgrades.

## Ecosystem Patterns (Modern and Common)

### CPython recommended pattern: Capsules

The official extending guide section "Providing a C API for an Extension Module" explicitly recommends capsules, often exporting a function-pointer table in one capsule.

### CPython datetime C API

datetime uses a capsule-backed API imported by macro (PyDateTime_IMPORT). This demonstrates the pointer-table pattern used in stdlib. Note that docs warn current macro style is not subinterpreter-friendly if used as a process-global static in clients.

### NumPy C API

NumPy exposes a pointer-table C API imported at runtime. This is the dominant third-party pattern for a binary extension API used by many downstream modules.

Conclusion: capsule + function-table import is the de facto standard.

## Proposed pytealet C API Design

## 1) Public header for clients

Create a header such as:

- src/_tealet/pytealet_capi.h (or installed include/pytealet/capi.h)

Responsibilities:

1. Define API ABI version constants.
2. Define exported function-table struct with reserved slots.
3. Provide import helper macro/function for clients.
4. Keep all provider internals opaque.

Example sketch:

```c
#ifndef PYTEALET_CAPI_H
#define PYTEALET_CAPI_H

#include <Python.h>

#define PYTEALET_CAPI_ABI_VERSION 1u
#define PYTEALET_CAPI_CAPSULE_NAME "_tealet._C_API"

typedef struct PyTealet_API PyTealet_API;
typedef struct PyTealet_Context PyTealet_Context; /* opaque */

struct PyTealet_API {
    uint32_t abi_version;
    uint32_t struct_size;
    uint64_t feature_flags;

    /* Context lifecycle (per-interpreter) */
    PyTealet_Context *(*ctx_new)(void);
    void (*ctx_free)(PyTealet_Context *ctx);

    /* Core operations */
    PyObject *(*current)(PyTealet_Context *ctx);          /* new ref */
    PyObject *(*main)(PyTealet_Context *ctx);             /* new ref */
    PyObject *(*thread_sweep)(PyTealet_Context *ctx);     /* new ref */

    PyObject *(*switch_)(PyTealet_Context *ctx, PyObject *target, PyObject *arg); /* new ref */
    PyObject *(*set_exception)(PyTealet_Context *ctx, PyObject *target, PyObject *exc); /* new ref */

    int (*check_tealet)(PyTealet_Context *ctx, PyObject *obj);

    void *reserved[16];
};

static inline const PyTealet_API *
PyTealet_ImportAPI(void)
{
    return (const PyTealet_API *)PyCapsule_Import(PYTEALET_CAPI_CAPSULE_NAME, 0);
}

#endif
```

Notes:

- Function names in table can be PyTealet_Switch-style by exposing thin wrappers around your internals.
- Keep reserved entries for ABI growth.

## 2) Provider-side export in _tealet

In pytealet_module_exec:

1. Create/populate a static const PyTealet_API table.
2. Wrap table pointer in PyCapsule_New(..., "_tealet._C_API", ...).
3. Add capsule as module attribute _C_API.

Optional:

- Put ABI/version metadata into capsule context or table fields.
- Validate internal invariants before publishing.

## 3) Per-interpreter context model

Recommended context model:

1. ctx_new imports/gets _tealet module instance in current interpreter.
2. ctx_new stores a strong PyObject *module reference inside PyTealet_Context.
3. API calls use PyModule_GetState(ctx->module) internally each call.
4. ctx_free decrefs module and frees context.

This gives correct lifetime and interpreter separation without exposing mstate.

## 4) Error model

Use normal CPython conventions:

1. Pointer-returning calls return NULL on error and set PyErr.
2. int-returning calls return -1 on error with PyErr set.
3. Ownership conventions documented per function (new ref / borrowed / stolen).

## 5) Client module integration

Client module (for example stackless shim) should:

1. Import API in its module init.
2. Check abi_version and struct_size.
3. Create/store PyTealet_Context in client module state.
4. Use only table functions, never _tealet internals.

## Stackless Emulation Use Case Mapping

For a stackless emulation module, this design supports:

1. Fast direct switch/throw/set_exception operations from C.
2. No Python-level dynamic dispatch overhead for core control transfers.
3. Clean layering: stackless shim has its own C API and can internally depend on pytealet API table.
4. Future evolvability: both APIs can be independently versioned.

Recommended layering:

1. _tealet publishes pytealet C API capsule.
2. _stackless_emu imports pytealet API and exposes its own capsule API.
3. Third-party modules can choose either API depending on abstraction level.

## Free-threaded and Subinterpreter Considerations

The capsule+context design aligns with modern isolation guidance:

1. No process-global mutable provider internals exposed.
2. Context bound to current interpreter/module object.
3. Client must not cache interpreter-bound context across interpreters.
4. Client should store API/context in its own per-module state (not process-global statics).

## Compatibility and Versioning Policy

Use explicit ABI policy from day one:

1. abi_version: bump on breaking table/signature changes.
2. struct_size: allows backward-compatible extension of table tail.
3. feature_flags: advertise optional capabilities.
4. reserved slots: future function growth without immediate ABI bump.

Import-time checks should reject incompatible providers with clear ImportError.

Pre-release development policy (current c-api branch):

1. Keep abi_version pinned until first external release candidate.
2. Use a single base feature bit for table presence instead of per-function flags.
3. Treat table shape changes as in-flight branch evolution, then lock and version strictly before release.

## Suggested Implementation Plan

Phase 1 (minimal viable C API):

1. Add capsule export and import header.
2. Expose read-only identification/check functions and current/main retrieval.
3. Add one switch-like call path needed by stackless prototype.

Phase 2 (operational API):

1. Add native worker dispatch API for C callbacks (run_c) with the same (current, arg) and return semantics as Python run callables.
2. Add exception routing APIs (set_exception/throw helpers).
3. Add thread cleanup hooks needed by stress scenarios.
4. Add explicit docs for thread ownership and safety contracts.

Phase 3 (stackless integration):

1. Build stackless emulation extension consuming pytealet API.
2. Measure call-path overhead against Python-level fallback.
3. Harden ABI/version checks and compatibility tests.

## Test Strategy

Minimum tests to add once implementation starts:

1. Provider exports valid capsule and version metadata.
2. Client import succeeds/fails correctly on version mismatch.
3. Multiple subinterpreters each obtain independent contexts.
4. Re-import and teardown do not leak contexts.
5. Cross-thread misuse surfaces deterministic Python exceptions.

## Practical Recommendation

Proceed with capsule-based, versioned API and opaque per-interpreter context.

This is the safest and most portable design for modern CPython extension modules with per-module state, and it is the right base for building a stackless emulation C API on top.

## References Used

1. CPython C API: Capsules
2. CPython Extending Guide: Providing a C API for an Extension Module
3. CPython HOWTO: Isolating Extension Modules (per-module state, subinterpreters)
4. CPython datetime C API (capsule-backed table pattern)
5. NumPy C API import and pointer-table model

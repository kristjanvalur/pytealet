# C API Reference

This document describes tealet's capsule-based C API.

## Header and Include Path

Public header:
- `pytealet_capi.h`

Installed include path helper:
- Python: `tealet.get_include()`

Typical downstream build usage:
1. Add `tealet.get_include()` to your extension include directories.
2. Include `pytealet_capi.h`.
3. Import the capsule table with `PyTealetApi_Import()`.

## Capsule Contract

Capsule name:
- `PYTEALET_CAPI_CAPSULE_NAME` = `"_tealet._C_API"`

Import helper:
- `const PyTealet_CAPI *PyTealetApi_Import(void)`

ABI/version fields:
- `abi_version`
- `struct_size`
- `feature_flags`

Current feature flag:
- `PYTEALET_CAPI_FEATURE_BASE`

## Flags and Enums

Switch flags:
- `PYTEALET_SWITCH_FLAGS_DEFAULT`
- `PYTEALET_SWITCH_PANIC`

Throw flags:
- `PYTEALET_THROW_FLAGS_DEFAULT`
- `PYTEALET_THROW_PANIC`

State enum:
- `PyTealet_State`
  - `PYTEALET_STATE_NEW`
  - `PYTEALET_STATE_STUB`
  - `PYTEALET_STATE_RUN`
  - `PYTEALET_STATE_EXIT`
  - `PYTEALET_STATE_PRIMED`

## Context Lifecycle

`PyTealet_CAPI` is context-based.

- `ctx_new() -> PyTealet_CAPI_Context *`
- `ctx_free(ctx)`

Callers should create a context once per usage scope and release it with `ctx_free`.

## API Function Table

Module-level operations:
- `current(ctx) -> PyObject *`
- `main(ctx) -> PyObject *`
- `previous(ctx) -> PyObject *`
- `thread_active(ctx) -> PyObject *`
- `thread_kill(ctx, cleanup_passes, kill_exc_spec) -> PyObject *`
- `thread_reap(ctx, cleanup_passes, kill_exc_spec) -> PyObject *`
- `thread_sweep(ctx) -> PyObject *`
- `error_was_remote(ctx) -> int`
- `frame_introspection_get(ctx) -> int`
- `frame_introspection_set(ctx, enabled) -> int`
- `check_tealet(ctx, obj) -> int`

Tealet/object operations:
- `create(ctx) -> PyObject *`
- `duplicate(ctx, source) -> PyObject *`
- `stub(ctx, target) -> int`
- `set_stub(ctx, target, source, duplicate) -> int`
- `prepare(ctx, target, function_py, function_c) -> int`
- `run(ctx, target, function_py, function_c, arg) -> PyObject *`
- `switch_(ctx, target, arg, flags) -> PyObject *`
- `throw_(ctx, target, exception, return_target, flags) -> PyObject *`
- `set_pending_exception(ctx, target, exception, fallback) -> int`

`prepare()` primes the target immediately and leaves it in active `RUN` state;
subsequent `switch_()`/`throw_()` and Python `resolve_target()` exit routing use
the normal active-target path.

`throw_` return-target semantics:
- `return_target == NULL`: use current tealet as default return target
- `return_target == Py_None`: no default return target
- `return_target` is tealet object: use that explicit default return target

Metadata helpers:
- `is_foreign(ctx, target) -> int`
- `state_get(ctx, target, state_out) -> int`
- `thread_id_get(ctx, target, thread_id_out) -> int`

## Return/Ownership Conventions

General conventions:
- Functions returning `PyObject *` follow CPython conventions (new reference on success, `NULL` on error).
- Integer return codes generally use:
  - `0`/`1` for boolean-like success values where documented
  - `0` for success and `-1` for error for setter/getter-style helpers
- On error, Python exception state is set.

## Minimal Usage Sketch

```c
#include <Python.h>
#include "pytealet_capi.h"

static PyObject *demo(PyObject *self, PyObject *noargs) {
    const PyTealet_CAPI *api = PyTealetApi_Import();
    PyTealet_CAPI_Context *ctx = NULL;
    PyObject *cur = NULL;

    (void)self;
    (void)noargs;

    if (!api) {
        return NULL;
    }

    ctx = api->ctx_new();
    if (!ctx) {
        return NULL;
    }

    cur = api->current(ctx);
    api->ctx_free(ctx);
    return cur;
}
```

## Related Design Notes

For architecture and runtime context:
- `docs/ARCHITECTURE.md`
- `docs/ISSUES.md`

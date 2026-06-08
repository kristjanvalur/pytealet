/* pytealet_capi.h - public C API declarations for the _tealet extension.
 *
 * Client extensions should import this API via PyCapsule_Import() using the
 * capsule name below, then call function pointers from the returned table.
 */

#ifndef PYTEALET_CAPI_H
#define PYTEALET_CAPI_H

#include <Python.h>

#include <stdint.h>

#define PYTEALET_CAPI_ABI_VERSION 1u
#define PYTEALET_CAPI_CAPSULE_NAME "_tealet._C_API"

/* Feature flags published in PyTealet_CAPI.feature_flags. */
#define PYTEALET_CAPI_FEATURE_BASE (1ull << 0)

/* Transfer flags for switch_ and throw_.
 * Bit values are intentionally aligned with libtealet transfer semantics.
 */
#define PYTEALET_SWITCH_FLAGS_DEFAULT 0u
#define PYTEALET_SWITCH_PANIC (1u << 0)

#define PYTEALET_THROW_FLAGS_DEFAULT 0u
#define PYTEALET_THROW_PANIC (1u << 0)

typedef struct PyTealet_CAPI_Context PyTealet_CAPI_Context;
typedef PyObject *(*PyTealetApi_RunCFunc)(PyObject *current, PyObject *arg);

typedef struct PyTealet_CAPI {
    uint32_t abi_version;
    uint32_t struct_size;
    uint64_t feature_flags;

    /* Context lifetime is per-interpreter and requires an attached thread state. */
    PyTealet_CAPI_Context *(*ctx_new)(void);
    void (*ctx_free)(PyTealet_CAPI_Context *ctx);

    /* Module-level operations (not bound to a specific tealet method call). */

    /* Return new references (or NULL with exception set on failure). */
    PyObject *(*current)(PyTealet_CAPI_Context *ctx);
    PyObject *(*main)(PyTealet_CAPI_Context *ctx);
    PyObject *(*thread_sweep)(PyTealet_CAPI_Context *ctx);

    /* Returns 1 if obj is tealet-compatible, 0 if not, -1 on API misuse/error. */
    int (*check_tealet)(PyTealet_CAPI_Context *ctx, PyObject *obj);

    /* Equivalent to _tealet.tealet(). */
    PyObject *(*create)(PyTealet_CAPI_Context *ctx);

    /* Equivalent to source.duplicate(). */
    PyObject *(*duplicate)(PyTealet_CAPI_Context *ctx, PyObject *source);

    /* Tealet-method style operations (conceptual target.method(...)). */

    /* Equivalent to target.stub(). */
    int (*stub)(PyTealet_CAPI_Context *ctx, PyObject *target);

    /* Equivalent to target.prepare(function), but accepts exactly one callable mode.
     * Provide either function_py or function_c (not both). Returns 0 on success, -1 on error.
     */
    int (*prepare)(PyTealet_CAPI_Context *ctx, PyObject *target, PyObject *function_py,
                   PyTealetApi_RunCFunc function_c);

    /* Equivalent to target.run(...), with unified callable mode.
     * Provide either function_py or function_c (not both).
     */
    PyObject *(*run)(PyTealet_CAPI_Context *ctx, PyObject *target, PyObject *function_py,
                     PyTealetApi_RunCFunc function_c, PyObject *arg);

    /* Equivalent to target.switch(arg, panic=...) using C flags. */
    PyObject *(*switch_)(PyTealet_CAPI_Context *ctx, PyObject *target, PyObject *arg, uint32_t flags);

    /* Equivalent to target.throw(exception), with optional transfer flags. */
    PyObject *(*throw_)(PyTealet_CAPI_Context *ctx, PyObject *target, PyObject *exception, uint32_t flags);

    void *reserved[16];
} PyTealet_CAPI;

/* Import helper for clients. Returns NULL and sets exception on failure. */
static inline const PyTealet_CAPI *PyTealetApi_Import(void) {
    return (const PyTealet_CAPI *)PyCapsule_Import(PYTEALET_CAPI_CAPSULE_NAME, 0);
}

#endif

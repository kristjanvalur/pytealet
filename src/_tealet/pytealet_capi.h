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
#define PYTEALET_CAPI_FEATURE_SWITCH (1ull << 0)

typedef struct PyTealet_CAPI_Context PyTealet_CAPI_Context;

typedef struct PyTealet_CAPI {
    uint32_t abi_version;
    uint32_t struct_size;
    uint64_t feature_flags;

    /* Context lifetime is per-interpreter and requires the GIL. */
    PyTealet_CAPI_Context *(*ctx_new)(void);
    void (*ctx_free)(PyTealet_CAPI_Context *ctx);

    /* Return new references (or NULL with exception set on failure). */
    PyObject *(*current)(PyTealet_CAPI_Context *ctx);
    PyObject *(*main)(PyTealet_CAPI_Context *ctx);
    PyObject *(*thread_sweep)(PyTealet_CAPI_Context *ctx);

    /* Returns 1 if obj is tealet-compatible, 0 if not, -1 on API misuse/error. */
    int (*check_tealet)(PyTealet_CAPI_Context *ctx, PyObject *obj);

    /* Equivalent to target.switch(arg) if arg != NULL, else target.switch(). */
    PyObject *(*switch_)(PyTealet_CAPI_Context *ctx, PyObject *target, PyObject *arg);

    void *reserved[16];
} PyTealet_CAPI;

/* Import helper for clients. Returns NULL and sets exception on failure. */
static inline const PyTealet_CAPI *PyTealet_ImportCAPI(void) {
    return (const PyTealet_CAPI *)PyCapsule_Import(PYTEALET_CAPI_CAPSULE_NAME, 0);
}

#endif

/* pytealet_runtime.h - internal runtime object/state layout.
 *
 * Shared by pytealet runtime translation units that need concrete access to
 * tealet wrapper and lineage structures.
 */

#ifndef PYTEALET_RUNTIME_H
#define PYTEALET_RUNTIME_H

#include "frame_info.h"
#include "pytealet_capi.h"
#include "pytealet_module.h"
#include "tstate_state.h"

#include <stdint.h>

typedef struct PyTealetNewArg {
    struct PyTealetObject *dest;
    PyTealetModuleState *mstate;
    PyObject *func;
    PyTealetApi_RunCFunc cfunc;
    PyObject *arg;
} PyTealetNewArg;

/* the structure we associate with the main tealet */
struct PyTealetMainData {
    long tid;
    struct PyTealetMainData *ring_prev;
    struct PyTealetMainData *ring_next;
    PyTealetNewArg new_arg;
    PyObject *dustbin;
    PyObject *main_wrapper;       /* strong ref to this thread's main tealet wrapper */
    PyObject *wrappers;           /* set of weakrefs to non-main wrappers in this main lineage */
    PyObject *domain_lock_obj;    /* strong ref to lineage lock object */
    uint64_t throw_next_token;    /* monotonically increasing throw token generator */
    uint64_t pending_throw_token; /* token to deliver on the next switch/run return */
    PyObject *throw_records;      /* dict[token] -> (exc_instance, fallback_tealet_or_None) */
    int last_error_remote;        /* set when the most recently raised exception was remotely delivered */
};

/* Extra data stored with each tealet for the Python binding.
 * This structure is stored in tealet->extra and provides type-safe
 * access to the associated PyTealetObject.
 */
typedef struct PyTealetExtra {
    PyTealetObject *pytealet;
} PyTealetExtra;

/* Helper macros for type-safe access to the tealet extra data */
#define TEALET_PYOBJECT(t) (TEALET_EXTRA((t), PyTealetExtra)->pytealet)
#define TEALET_SET_PYOBJECT(t, obj) (TEALET_EXTRA((t), PyTealetExtra)->pytealet = (obj))

/* The python tealet object */
struct PyTealetObject {
    PyObject_HEAD int state;
    tealet_t *tealet;
    unsigned long owner_tid;       /* thread that owns this tealet object */
    PyObject *domain_lock_obj;     /* strong ref to lineage lock object */
    PyObject *tracking_ref;        /* weakref object stored in main-lineage wrapper set */
    PyObject *prepared_func;       /* callable stored by prepare(), consumed by first switch */
    uint64_t inflight_throw_token; /* non-zero only while fallback-aware throw is in flight */
#if !defined(Py312P)
    PyObject *weakreflist; /* List of weak references */
#endif

    /* thread state information */
    PyTealetTstate tstate;
    /* Dormant frame snapshot and (3.12+) reversible frame rewrites. */
    PyTealetFrameInfo frame_info;
};

#endif

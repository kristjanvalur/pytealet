
#include "Python.h"
#include "frameobject.h"
#if PY_VERSION_HEX >= 0x030C0000
#include "internal/pycore_frame.h"
#endif
#include "pythread.h"
#include "structmember.h"
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "tealet.h"
#include "tealet_extras.h"

/* ===================================================================== */
/* Compile-Time Version Feature Flags                                    */
/* ===================================================================== */

/* Python minor-version helpers for readable version-specific conditionals. */
#if PY_VERSION_HEX >= 0x030A0000 && PY_VERSION_HEX < 0x030B0000
#define PY310 1
#endif

#if PY_VERSION_HEX >= 0x030B0000
#define Py311P 1
#if PY_VERSION_HEX < 0x030C0000
#define PY311 1
#endif
#endif

#if PY_VERSION_HEX >= 0x030C0000
#define PY312P 1
#if PY_VERSION_HEX < 0x030D0000
#define PY312 1
#endif
#endif

#if PY_VERSION_HEX >= 0x030D0000 && PY_VERSION_HEX < 0x030E0000
#define PY313 1
#endif

#if PY_VERSION_HEX >= 0x030E0000 && PY_VERSION_HEX < 0x030F0000
#define PY314 1
#endif

#if PY_VERSION_HEX >= 0x030F0000 && PY_VERSION_HEX < 0x03100000
#define PY315 1
#endif

#if defined(PY310) || defined(PY311) || defined(PY312)
#define PY_HAS_CFRAME
#endif

#if defined(PY310)
#define PY_HAS_TSTATE_FRAME
#endif

#if defined(PY_HAS_CFRAME)
#if defined(PY310)
typedef CFrame PyTealetCFrame;
#else
typedef _PyCFrame PyTealetCFrame;
#endif
#endif

#define STATE_NEW 0
#define STATE_STUB 1
#define STATE_RUN 2
#define STATE_EXIT 3

#ifndef PYTEALET_DEFER_DELETE
/* Keep the exited tealet in the pytealet structure for access to the tealet api. */
#define PYTEALET_DEFER_DELETE 0
#endif

/* ===================================================================== */
/* Core Types and Module State                                           */
/* ===================================================================== */

/* Forward declaration */
typedef struct PyTealetObject PyTealetObject;
static struct PyModuleDef _tealet_module;

typedef struct PyTealetModuleState {
    Py_tss_t tls_key;
    PyTypeObject *tealet_type;
    PyObject *tealet_error;
    PyObject *invalid_error;
    PyObject *state_error;
    PyObject *defunct_error;
} PyTealetModuleState;

typedef struct PyTealetNewArg {
    PyTealetObject *dest;
    PyTealetModuleState *mstate;
    PyObject *func;
    PyObject *arg;
} PyTealetNewArg;

/* the structure we associate with the main tealet */
typedef struct PyTealetMainData {
    long tid;
    PyTealetNewArg new_arg;
    PyObject *dustbin;
} PyTealetMainData;

/* initial number of slots in dustbin, to avoid realloc on push */
#define DUSTBIN_PREALLOC 10

/* Extra data stored with each tealet for the Python binding.
 * This structure is stored in tealet->extra and provides type-safe
 * access to the associated PyTealetObject.
 */
typedef struct PyTealetExtra {
    PyTealetObject *pytealet;
} PyTealetExtra;


/* structures to help query frame state for dormant tealets in 3.11 and above */
#if !defined(PY_HAS_TSTATE_FRAME)
typedef struct PyTealetFrameInfoEntry {
    _PyInterpreterFrame **location;
    _PyInterpreterFrame *old_value;
} PyTealetFrameInfoEntry;

typedef struct PyTealetFrameInfo {
    /* Snapshot of the dormant frame object for tealet.frame queries. */
    PyFrameObject *frame;
#if defined(PY312P)
    PyTealetFrameInfoEntry *items;
    Py_ssize_t size;
    Py_ssize_t capacity;
#endif
} PyTealetFrameInfo;
#endif

/* Helper macros for type-safe access to the tealet extra data */
#define TEALET_PYOBJECT(t) (TEALET_EXTRA((t), PyTealetExtra)->pytealet)
#define TEALET_SET_PYOBJECT(t, obj) (TEALET_EXTRA((t), PyTealetExtra)->pytealet = (obj))

/* Return a new reference to the Python wrapper for a raw tealet pointer.
 * Raises RuntimeError and returns NULL if the wrapper is unavailable.
 */
static PyObject *GetWrapperRef(tealet_t *tealet) {
    PyObject *wrapper;
    if (!tealet) {
        PyErr_SetString(PyExc_RuntimeError, "tealet unavailable");
        return NULL;
    }
    wrapper = (PyObject *)TEALET_PYOBJECT(tealet);
    if (!wrapper) {
        PyErr_SetString(PyExc_RuntimeError, "tealet wrapper unavailable");
        return NULL;
    }
    return Py_NewRef(wrapper);
}

/* Captures the Python thread-state snapshot for a tealet.
 *
 * Stored fields and semantics can vary across Python versions.
 * Before switching away from a tealet, we save the current PyThreadState here.
 * When switching back, we restore it and release any owned references.
 *
 * There is an optimized symmetric switch path between tealets A and B:
 * 1) A switches to B and moves thread-state into A's local snapshot.
 * 2) B switches back and moves its saved snapshot into PyThreadState.
 *
 * In that plain switch/switch case, state can be moved in and out without
 * refcount churn.
 *
 * Reference adjustment is only needed when ownership changes:
 * a) Creating/running a new tealet requires a copied snapshot with owned refs.
 * b) Exiting a tealet requires clearing its snapshot and releasing owned refs.
 */
struct PyTealetTstate {
    int has_state;     /* Debug helper: 1 when this struct currently stores a saved
                          tstate */

#if defined(PY_HAS_TSTATE_FRAME)
    PyFrameObject *frame;
#endif

    /* current exception state */
    PyObject *exc_type;
    PyObject *exc_val;
    PyObject *exc_tb;
    _PyErr_StackItem *exc_info;
    _PyErr_StackItem exc_state;

    /* current recursion state */
#if defined(PY310)
    int recursion_depth;
#elif defined(PY311)
    int recursion_remaining;
    int recursion_limit;
#else /* 3.12+ */
    int py_recursion_remaining;
    int py_recursion_limit;
    int c_recursion_remaining;
#endif

    int trash_delete_nesting;  /* destructor nesting level, conserved. */
    PyObject *context; /* Python 3.7+ contextvars */

#if defined(PY_HAS_CFRAME)
    /* Python 3.10-3.12: cframe tracks C-level call frames (removed in 3.13)
     * Stack-slicing preserves the CFrame struct itself; we just save the
     * pointer */
    PyTealetCFrame *cframe;
#endif
#if defined(Py311P)
#if defined(PY311)
    int cframe_use_tracing;  /* tracing flag from cframe */
#endif
    /* new in 3.11, these four must be preserved together */
    void *cframe_current_frame;
    _PyStackChunk *datastack_chunk;
    PyObject **datastack_top;
    PyObject **datastack_limit;
#endif
};

typedef struct PyTealetTstate PyTealetTstate;

/* ===================================================================== */
/* PyTealetTstate Snapshot Declarations                                  */
/* ===================================================================== */

/* Basic PyTealetTstate operations */

/* Initialize snapshot bookkeeping (no state saved yet). */
static void PyTealetTstate_Init(PyTealetTstate *saved);
/* Copy raw fields from PyThreadState into snapshot (no refcount changes). */
static void PyTealetTstate_Get(PyTealetTstate *dst, const PyThreadState *src);
/* Copy raw fields from snapshot back into PyThreadState. */
static void PyTealetTstate_Put(const PyTealetTstate *src, PyThreadState *dst);
/* Acquire owned references for objects captured in a saved snapshot. */
static void PyTealetTstate_IncRef(PyTealetTstate *saved);
/* Release owned references for objects captured in a saved snapshot. */
static void PyTealetTstate_DecRef(PyTealetTstate *saved, tealet_t *dustbin_tealet);

/* Higher level helper used in switching */

/* Save a copied snapshot and own references (for duplicated/new ownership). */
static void PyTealetTstate_Copy(PyTealetTstate *dst, const PyThreadState *src);
/* Drop a saved snapshot and release owned references. */
static void PyTealetTstate_Drop(PyTealetTstate *dst, tealet_t *dustbin_tealet);
/* Move active PyThreadState into snapshot before switching away. */
static void PyTealetTstate_Save(PyTealetTstate *dst, PyThreadState *src);
/* Restore active PyThreadState from snapshot after switching back. */
static void PyTealetTstate_Restore(PyTealetTstate *src, PyThreadState *dst);

/* The python tealet object */
struct PyTealetObject {
    PyObject_HEAD int state;
    tealet_t *tealet;
    PyObject *weakreflist; /* List of weak references */

    /* thread state information */
    PyTealetTstate tstate;
#if !defined(PY_HAS_TSTATE_FRAME)
    /* Dormant frame snapshot and (3.12+) reversible frame rewrites. */
    PyTealetFrameInfo frame_info;
#endif
};

/* helpers for getting main and current and checking relationship */
static PyTealetModuleState *GetModuleStateFromClass(PyTypeObject *cls);
static PyTealetObject *GetMain(PyTealetModuleState *mstate, int create);
static PyTealetObject *GetCurrent(PyTealetModuleState *mstate, PyTealetObject *main, int create_main);
static int CheckTarget(PyTealetModuleState *mstate, PyTealetObject *target, PyTealetObject *main);
static void dustbin_push(tealet_t *tealet, PyObject *obj);

static tealet_t *pytealet_main(tealet_t *t_current, void *arg);

/* ===================================================================== */
/* Type and Module Access Helpers                                        */
/* ===================================================================== */

static PyTealetModuleState *GetModuleStateFromClass(PyTypeObject *cls) {
#if defined(Py311P)
    PyObject *module = PyType_GetModuleByDef(cls, &_tealet_module);
    if (module) {
        PyTealetModuleState *mstate = (PyTealetModuleState *)PyModule_GetState(module);
        assert(mstate != NULL);
        return mstate;
    }
    return NULL;
#else
    PyTypeObject *cur = cls;
    while (cur) {
        PyTealetModuleState *mstate = (PyTealetModuleState *)PyType_GetModuleState(cur);
        cur = cur->tp_base;
        if (mstate)
            return mstate;
        if (PyErr_Occurred()) {
            if (PyErr_ExceptionMatches(PyExc_TypeError) && cur != NULL)
                PyErr_Clear();
            else
                return NULL;
        }
    }
    if (!PyErr_Occurred())
        PyErr_SetString(PyExc_TypeError, "type is not part of a module");
    return NULL;
#endif
}

static int PyTealet_Check(PyObject *op, PyTealetModuleState *mstate) {
    return mstate && mstate->tealet_type && PyObject_TypeCheck(op, mstate->tealet_type);
}

static int PyTealet_CheckExact(PyObject *op, PyTealetModuleState *mstate) {
    return mstate && mstate->tealet_type && (Py_TYPE(op) == mstate->tealet_type);
}

/* ===================================================================== */
/* PyTealetTstate Snapshot Implementation                                */
/* ===================================================================== */

/* ===================================================================== */
/* PyTealetFrameInfo Methods                                             */
/* ===================================================================== */

#if !defined(PY_HAS_TSTATE_FRAME)
static void PyTealetFrameInfo_Init(PyTealetFrameInfo *info) {
    info->frame = NULL;
#if defined(PY312P)
    info->items = NULL;
    info->size = 0;
    info->capacity = 0;
#endif
}

static void PyTealetFrameInfo_Fini(PyTealetFrameInfo *info) {
#if defined(PY312P)
    free(info->items);
    info->items = NULL;
    info->size = 0;
    info->capacity = 0;
#endif
}

#if defined(PY312P)
static void PyTealetFrameInfo_ClearRewrites(PyTealetFrameInfo *info) { info->size = 0; }

static int PyTealetFrameInfo_RecordRewrite(PyTealetFrameInfo *info, _PyInterpreterFrame **location) {
    PyTealetFrameInfoEntry *entry;
    Py_ssize_t next_capacity;
    void *new_items;

    if (info->size == info->capacity) {
        next_capacity = info->capacity ? info->capacity * 2 : 8;
        new_items = realloc(info->items, (size_t)next_capacity * sizeof(PyTealetFrameInfoEntry));
        if (!new_items) {
            PyErr_NoMemory();
            return -1;
        }
        info->items = (PyTealetFrameInfoEntry *)new_items;
        info->capacity = next_capacity;
    }

    entry = &info->items[info->size++];
    entry->location = location;
    entry->old_value = *location;
    return 0;
}

/* 3.12+: expose original links by restoring rewritten frame pointers */
static void PyTealetFrameInfo_ExposeFrames(PyTealetFrameInfo *info) {
    while (info->size > 0) {
        PyTealetFrameInfoEntry *entry = &info->items[--info->size];
        *entry->location = entry->old_value;
    }
}
#endif

/* 3.12+: hide unsafe/incomplete frames by rewriting frame links */
static int PyTealetFrameInfo_HideFrames(PyTealetFrameInfo *info, PyFrameObject *top_frame) {
#if defined(PY312P)
    _PyInterpreterFrame **last_link;
    _PyInterpreterFrame *iframe;

    if (!top_frame) {
        return 0;
    }

    PyTealetFrameInfo_ClearRewrites(info);
    last_link = &top_frame->f_frame;
    iframe = top_frame->f_frame;
    while (iframe) {
        if (!_PyFrame_IsIncomplete(iframe) && iframe->owner != FRAME_OWNED_BY_CSTACK) {
            /* a complete frame.  if the last link didn't point to it, rewrite. */
            if (*last_link != iframe) {
                if (PyTealetFrameInfo_RecordRewrite(info, last_link) < 0) {
                    PyTealetFrameInfo_ExposeFrames(info);
                    return -1;
                }
                *last_link = iframe;
            }
            last_link = &iframe->previous;
        }
        iframe = iframe->previous;
    }

    /* handle the last link */
    if (*last_link != NULL) {
        if (PyTealetFrameInfo_RecordRewrite(info, last_link) < 0) {
            PyTealetFrameInfo_ExposeFrames(info);
            return -1;
        }
        *last_link = NULL;
    }
    return 0;
#else
    (void)info;
    (void)top_frame;
    return 0;
#endif
}

static int PyTealetFrameInfo_Capture(PyTealetFrameInfo *info, int rewrite_chain) {
    PyFrameObject *frame = (PyFrameObject *)PyEval_GetFrame();
    if (!frame) {
        info->frame = NULL;
        return 0;
    }
    if (rewrite_chain && PyTealetFrameInfo_HideFrames(info, frame) < 0) {
        return -1;
    }
    Py_XSETREF(info->frame, (PyFrameObject *)Py_XNewRef((PyObject *)frame));
    return 0;
}

static void PyTealetFrameInfo_Release(PyTealetFrameInfo *info, tealet_t *dustbin_tealet) {
#if defined(PY312P)
    PyTealetFrameInfo_ExposeFrames(info);
#endif
    if (dustbin_tealet) {
        dustbin_push(dustbin_tealet, (PyObject *)info->frame);
        info->frame = NULL;
    } else {
        Py_CLEAR(info->frame);
    }
}

#endif

static void PyTealetTstate_Init(PyTealetTstate *saved) {
    saved->has_state = 0;
}

/* Raw copy the tstate files from PyThreadState to our local structure */
static void PyTealetTstate_Get(PyTealetTstate *dst, const PyThreadState *src) {
#if defined(PY_HAS_TSTATE_FRAME)
    dst->frame = src->frame;
#endif
#if defined(PY310)
    dst->recursion_depth = src->recursion_depth;
#elif defined(PY311)
    dst->recursion_remaining = src->recursion_remaining;
    dst->recursion_limit = src->recursion_limit;
#else /* 3.12+ */
    dst->py_recursion_remaining = src->py_recursion_remaining;
    dst->py_recursion_limit = src->py_recursion_limit;
    dst->c_recursion_remaining = src->c_recursion_remaining;
#endif

#if defined(PY310) || defined(PY311)
    dst->exc_type = src->curexc_type;
    dst->exc_val = src->curexc_value;
    dst->exc_tb = src->curexc_traceback;
#else
    dst->exc_type = NULL;
    dst->exc_val = NULL;
    dst->exc_tb = NULL;
#endif

    dst->exc_state = src->exc_state;
    /* Keep dst->exc_info self-contained when it points at exc_state. */
    if (src->exc_info == &src->exc_state)
        dst->exc_info = &dst->exc_state;
    else
        dst->exc_info = src->exc_info;

    dst->context = src->context;

#if defined(PY_HAS_CFRAME)
    dst->cframe = src->cframe;
#endif
#if defined(Py311P)
    dst->cframe_current_frame = src->cframe ? (void *)src->cframe->current_frame : NULL;
#if defined(PY311)
    dst->cframe_use_tracing = src->cframe ? src->cframe->use_tracing : 0;
#endif
    dst->datastack_chunk = src->datastack_chunk;
    dst->datastack_top = src->datastack_top;
    dst->datastack_limit = src->datastack_limit;
#endif
#if !defined(PY312P)
    dst->trash_delete_nesting = src->trash_delete_nesting;
#else /* 3.12+ */
    dst->trash_delete_nesting = src->trash.delete_nesting;
#endif
}

/* Raw copy previously saved tealet tstate into PyThreadState. */
static void PyTealetTstate_Put(const PyTealetTstate *src, PyThreadState *dst) {
#if defined(PY_HAS_TSTATE_FRAME)
    dst->frame = src->frame;
#endif
#if defined(PY310)
    dst->recursion_depth = src->recursion_depth;
#elif defined(PY311)
    dst->recursion_remaining = src->recursion_remaining;
    dst->recursion_limit = src->recursion_limit;
#else /* 3.12+ */
    dst->py_recursion_remaining = src->py_recursion_remaining;
    dst->py_recursion_limit = src->py_recursion_limit;
    dst->c_recursion_remaining = src->c_recursion_remaining;
#endif

#if defined(PY310) || defined(PY311)
    dst->curexc_type = src->exc_type;
    dst->curexc_value = src->exc_val;
    dst->curexc_traceback = src->exc_tb;
#endif

    dst->exc_state = src->exc_state;
    if (src->exc_info == &src->exc_state)
        dst->exc_info = &dst->exc_state;
    else
        dst->exc_info = src->exc_info;

    dst->context = src->context;
    dst->context_ver++; /* Invalidate contextvars cache */

#if defined(PY_HAS_CFRAME)
    dst->cframe = src->cframe;
#endif
#if defined(Py311P)
    if (dst->cframe) {
#if defined(PY311)
        dst->cframe->use_tracing = src->cframe_use_tracing;
#endif
        dst->cframe->current_frame = src->cframe_current_frame;
    }
    dst->datastack_chunk = src->datastack_chunk;
    dst->datastack_top = src->datastack_top;
    dst->datastack_limit = src->datastack_limit;
#endif
#if !defined(PY312P)
    dst->trash_delete_nesting = src->trash_delete_nesting;
#else /* 3.12+ */
    dst->trash.delete_nesting = src->trash_delete_nesting;
#endif
}

/* Increment and decrement the reference count of the tstate's references.
 * we need to Increment the references when we create new tealets from an
 * existing one (or main), and decrement when a tealet terminates.
 */
static void PyTealetTstate_IncRef(PyTealetTstate *saved) {
    assert(saved->has_state == 1);
#if defined(PY_HAS_TSTATE_FRAME)
    Py_XINCREF(saved->frame);
#endif
    Py_XINCREF(saved->exc_type);
    Py_XINCREF(saved->exc_val);
    Py_XINCREF(saved->exc_tb);
    Py_XINCREF(saved->exc_state.exc_value);
    /* exc_info is a pointer to exc_state or a stack item, so we don't own a
     * reference to it */
    Py_XINCREF(saved->context);
}

static void dustbin_push(tealet_t *tealet, PyObject *obj) {
    PyTealetMainData *mdata;
    if (!obj)
        return;
    if (!tealet) {
        Py_DECREF(obj);
        return;
    }
    mdata = (PyTealetMainData *)*tealet_main_userpointer(tealet);
    if (!mdata || !mdata->dustbin || !PyList_Check(mdata->dustbin)) {
        Py_DECREF(obj);
        return;
    }
    if (PyList_Append(mdata->dustbin, obj) < 0) {
        Py_DECREF(obj);
        PyErr_WriteUnraisable(Py_None);
        PyErr_Clear();
        return;
    }
    Py_DECREF(obj);
}

/* Clear deferred decref objects after a safe switch point. */
static void dustbin_clear(tealet_t *tealet) {
    PyTealetMainData *mdata = (PyTealetMainData *)*tealet_main_userpointer(tealet);
    Py_ssize_t n;
    n = PyList_GET_SIZE(mdata->dustbin);
    if (n == 0)
        return;
    if (PyList_SetSlice(mdata->dustbin, 0, n, NULL) < 0) {
        PyErr_WriteUnraisable(Py_None);
        PyErr_Clear();
    }
}

static void PyTealetTstate_DecRef(PyTealetTstate *saved, tealet_t *dustbin_tealet) {
    assert(saved->has_state == 1);
    if (dustbin_tealet) {
#if defined(PY_HAS_TSTATE_FRAME)
        dustbin_push(dustbin_tealet, (PyObject *)saved->frame);
#endif
        dustbin_push(dustbin_tealet, saved->exc_type);
        dustbin_push(dustbin_tealet, saved->exc_val);
        dustbin_push(dustbin_tealet, saved->exc_tb);
        dustbin_push(dustbin_tealet, saved->exc_state.exc_value);
        dustbin_push(dustbin_tealet, saved->context);
    } else {
#if defined(PY_HAS_TSTATE_FRAME)
        Py_XDECREF(saved->frame);
#endif
        Py_XDECREF(saved->exc_type);
        Py_XDECREF(saved->exc_val);
        Py_XDECREF(saved->exc_tb);
        Py_XDECREF(saved->exc_state.exc_value);
        Py_XDECREF(saved->context);
    }
}

/* Debug-only hygiene helper: clear active Python thread state slots. */
static void PyTealetTstate_ClearPy(PyThreadState *py_tstate) {
#if defined(Py_DEBUG)
#if defined(PY_HAS_TSTATE_FRAME)
    py_tstate->frame = NULL;
#endif
#if defined(PY310) || defined(PY311)
    py_tstate->curexc_type = NULL;
    py_tstate->curexc_value = NULL;
    py_tstate->curexc_traceback = NULL;
#endif
    py_tstate->exc_info = NULL; /* use this as a sentinel, should never be null
                                   in a valid situation */
    py_tstate->exc_state.exc_value = NULL;
#if defined(PY310)
    py_tstate->recursion_depth = 0;
#elif defined(PY311)
    py_tstate->recursion_remaining = 0;
    py_tstate->recursion_limit = 0;
#else /* 3.12+ */
    py_tstate->py_recursion_remaining = 0;
    py_tstate->py_recursion_limit = 0;
    py_tstate->c_recursion_remaining = 0;
#endif
#if !defined(PY312P)
    py_tstate->trash_delete_nesting = 0;
#else /* 3.12+ */
    py_tstate->trash.delete_nesting = 0;
#endif
    py_tstate->context = NULL;
#if defined(PY_HAS_CFRAME)
    py_tstate->cframe = NULL;
#endif
#else
    (void)py_tstate;
#endif
}

/* Debug-only hygiene helper: verify sentinel clear state. */
static void PyTealetTstate_AssertClearPy(PyThreadState *py_tstate) {
#if defined(Py_DEBUG)
    /* should never be null in a valid situation, null indicates that we
     * previously cleared it.*/
    assert(py_tstate->exc_info == NULL);
#else
    (void)py_tstate;
#endif
}

/* copy the threadstate, e.g. when we create a stub */
static void PyTealetTstate_Copy(PyTealetTstate *dst, const PyThreadState *src) {
    assert(dst->has_state == 0);
    PyTealetTstate_Get(dst, src);
    dst->has_state = 1;
    PyTealetTstate_IncRef(dst);
}

/* drop our own threadstate refs, e.g. after failure, or at tealet end */
static void PyTealetTstate_Drop(PyTealetTstate *dst, tealet_t *dustbin_tealet) {
    if (!dst->has_state)
        return;
    PyTealetTstate_DecRef(dst, dustbin_tealet);
    dst->has_state = 0;
}

/* Move out the threadstate to a saved struct before switch. someone will
 * restore after. */
static void PyTealetTstate_Save(PyTealetTstate *dst, PyThreadState *src) {
    assert(dst->has_state == 0);
    PyTealetTstate_Get(dst, src);
    PyTealetTstate_ClearPy(src);
    dst->has_state = 1;
}

/* restore the threadstate, after someon has saved it.*/
static void PyTealetTstate_Restore(PyTealetTstate *src, PyThreadState *dst) {
    assert(src->has_state == 1);
    PyTealetTstate_AssertClearPy(dst);
    PyTealetTstate_Put(src, dst);
    src->has_state = 0;
}

/* get the far pointer that we need at least ot store any stack based data
 * currently in the python tstate.  this varies by python version
 */

static void *PyTealet_GetStackFar(const PyThreadState *py_tstate) {
#if defined(PY_HAS_CFRAME)
    /* python 3.10 has cframe on stack.  make sure we save our stacks to include
     * this whole structure
     */
    if (py_tstate->cframe)
        return tealet_stack_further(&py_tstate->cframe[0], &py_tstate->cframe[1]);
#else
    (void)py_tstate;
#endif
    return NULL;
}

/* ===================================================================== */
/* Python Tealet Type API (Methods and Accessors)                        */
/* ===================================================================== */

static PyObject *pytealet_new(PyTypeObject *subtype, PyObject *args, PyObject *kwds) {
    PyTealetObject *src = NULL;
    PyTealetObject *result;
    PyTealetModuleState *mstate = GetModuleStateFromClass(subtype);
    if (!mstate)
        return NULL;
    if (args && PyTuple_GET_SIZE(args) > 0) {
        src = (PyTealetObject *)PyTuple_GET_ITEM(args, 0);
        if (!PyTealet_Check((PyObject *)src, mstate)) {
            PyErr_SetNone(PyExc_TypeError);
            return NULL;
        }
        if (src->state != STATE_NEW && src->state != STATE_STUB) {
            PyErr_SetString(mstate->state_error, "state must be new or stub");
            return NULL;
        }
    }
    result = (PyTealetObject *)subtype->tp_alloc(subtype, 0);
    if (!result)
        return NULL;
    result->state = STATE_NEW;
    result->tealet = NULL;
    PyTealetTstate_Init(&result->tstate);
#if !defined(PY_HAS_TSTATE_FRAME)
    PyTealetFrameInfo_Init(&result->frame_info);
#endif
    result->weakreflist = NULL;

    if (src) {
        if (src->state == STATE_STUB) {
            /* duplicate the stub tealet and the tstate */
            result->tealet = tealet_duplicate(src->tealet);
            if (!result->tealet) {
                Py_DECREF(result);
                return PyErr_NoMemory();
            }
            TEALET_SET_PYOBJECT(result->tealet, result);
            result->tstate = src->tstate;
            PyTealetTstate_IncRef(&result->tstate);
#if !defined(PY_HAS_TSTATE_FRAME)
            /* Stub tealets should not carry dormant frame snapshots. */
            PyTealetFrameInfo_Init(&result->frame_info);
#endif
        }
        result->state = src->state;
    }
    return (PyObject *)result;
}

static void pytealet_dealloc(PyObject *obj) {
    PyTealetObject *tealet = (PyTealetObject *)obj;
    if (tealet->state == STATE_RUN) {
        int err = PyErr_WarnEx(PyExc_RuntimeWarning, "freeing an active tealet leaks memory", 1);
        if (err) {
            PyErr_WriteUnraisable(Py_None);
        }
    }
    /* Release any owned saved thread-state references */
    PyTealetTstate_Drop(&tealet->tstate, NULL);
#if !defined(PY_HAS_TSTATE_FRAME)
    PyTealetFrameInfo_Release(&tealet->frame_info, NULL);
    PyTealetFrameInfo_Fini(&tealet->frame_info);
#endif
    if (tealet->weakreflist != NULL)
        PyObject_ClearWeakRefs(obj);
    if (tealet->tealet)
        tealet_delete(tealet->tealet);
    Py_TYPE(obj)->tp_free(obj);
}

static PyObject *pytealet_stub(PyObject *self, PyTypeObject *defining_class, PyObject *const *args, Py_ssize_t nargs,
                               PyObject *kwnames) {
    PyTealetObject *main, *pytealet = (PyTealetObject *)self;
    PyTealetModuleState *mstate = (PyTealetModuleState *)PyType_GetModuleState(defining_class);
    tealet_t *tresult;
    PyThreadState *tstate = PyThreadState_GET();
    void *stack_far;
    if (!mstate)
        return NULL;
    if (nargs != 0 || (kwnames && PyTuple_GET_SIZE(kwnames) > 0)) {
        PyErr_SetString(PyExc_TypeError, "stub() takes no arguments");
        return NULL;
    }
    if (pytealet->state != STATE_NEW) {
        PyErr_SetString(mstate->state_error, "must be new");
        return NULL;
    }
    assert(pytealet->tealet == NULL);
    main = GetMain(mstate, 1);
    if (!main)
        return NULL;
    stack_far = PyTealet_GetStackFar(PyThreadState_GET());
    tresult = tealet_stub_new(main->tealet, stack_far);
    if (!tresult)
        return PyErr_NoMemory();
    PyTealetTstate_Copy(&pytealet->tstate, tstate);
    pytealet->tealet = tresult;
    pytealet->state = STATE_STUB;
    TEALET_SET_PYOBJECT(tresult, pytealet);
    return Py_NewRef(self);
}

/* return the current tealet for this tealet lineage */
static PyObject *pytealet_current(PyObject *self, PyTypeObject *defining_class, PyObject *const *args,
                                  Py_ssize_t nargs, PyObject *kwnames) {
    PyTealetModuleState *mstate = (PyTealetModuleState *)PyType_GetModuleState(defining_class);
    PyTealetObject *current;
    PyTealetObject *base = (PyTealetObject *)self;
    if (!mstate)
        return NULL;
    if (nargs != 0 || (kwnames && PyTuple_GET_SIZE(kwnames) > 0)) {
        PyErr_SetString(PyExc_TypeError, "current() takes no arguments");
        return NULL;
    }
    if (!base->tealet) {
        PyErr_SetString(mstate->state_error, "must be active");
        return NULL;
    }
    current = GetCurrent(mstate, base, 0);
    if (!current)
        return NULL;
    return Py_NewRef((PyObject *)current);
}

/* return the previous tealet (the one that switched to this tealet lineage) */
static PyObject *pytealet_previous(PyObject *self, PyTypeObject *defining_class, PyObject *const *args,
                                   Py_ssize_t nargs, PyObject *kwnames) {
    PyTealetModuleState *mstate = (PyTealetModuleState *)PyType_GetModuleState(defining_class);
    PyTealetObject *base = (PyTealetObject *)self;
    PyObject *prev;
    tealet_t *anchor;
    tealet_t *raw_prev;
    if (!mstate)
        return NULL;
    if (nargs != 0 || (kwnames && PyTuple_GET_SIZE(kwnames) > 0)) {
        PyErr_SetString(PyExc_TypeError, "previous() takes no arguments");
        return NULL;
    }

    if (!base->tealet) {
        PyErr_SetString(mstate->state_error, "must be active");
        return NULL;
    }
    anchor = base->tealet;
    raw_prev = tealet_previous(anchor);
    if (!raw_prev)
        Py_RETURN_NONE;
    prev = GetWrapperRef(raw_prev);
    return prev;
}

/* return the main tealet for this tealet lineage */
static PyObject *pytealet_main_method(PyObject *self, PyTypeObject *defining_class, PyObject *const *args,
                                      Py_ssize_t nargs, PyObject *kwnames) {
    PyTealetModuleState *mstate = (PyTealetModuleState *)PyType_GetModuleState(defining_class);
    PyTealetObject *base = (PyTealetObject *)self;
    if (!mstate)
        return NULL;
    if (nargs != 0 || (kwnames && PyTuple_GET_SIZE(kwnames) > 0)) {
        PyErr_SetString(PyExc_TypeError, "main() takes no arguments");
        return NULL;
    }
    if (!base->tealet) {
        PyErr_SetString(mstate->state_error, "must be active");
        return NULL;
    } else {
        return GetWrapperRef(base->tealet->main);
    }
}

/* run a tealet and optinonally run */
static PyObject *pytealet_run(PyObject *self, PyTypeObject *defining_class, PyObject *const *args, Py_ssize_t nargs,
                              PyObject *kwnames) {
    PyTealetModuleState *mstate = (PyTealetModuleState *)PyType_GetModuleState(defining_class);
    PyTealetObject *target = (PyTealetObject *)self;
    PyTealetObject *current;
    PyObject *func;
    PyObject *farg;
    int fail;
    tealet_t *tealet;
    PyThreadState *tstate = PyThreadState_GET();
    PyObject *result;
    int created_from_new;
    PyTealetMainData *mdata;
    PyTealetNewArg *ptarg;
    void *switch_arg;
    if (!mstate)
        return NULL;

    current = GetCurrent(mstate, target, 0);
    if (!current)
        return NULL;
    if (CheckTarget(mstate, target, current))
        return NULL;

    if (target->state != STATE_NEW && target->state != STATE_STUB) {
        PyErr_SetString(mstate->state_error, "must be new or stub");
        return NULL;
    }

	/* manual FASTCALL argument parsing */
    func = farg = NULL;
    if (nargs >= 1)
        func = args[0];
    if (nargs >= 2)
        farg = args[1];
    if (nargs > 2) {
        PyErr_Format(PyExc_TypeError, "run() takes at most 2 arguments (%zd given)", nargs);
        return NULL;
    }

    if (kwnames && PyTuple_GET_SIZE(kwnames) > 1) {
        PyErr_SetString(PyExc_TypeError, "run() takes at most 2 keyword arguments");
        return NULL;
    }
    if (kwnames && PyTuple_GET_SIZE(kwnames) > 0) {
        Py_ssize_t i;
        for (i = 0; i < PyTuple_GET_SIZE(kwnames); i++) {
            PyObject *key = PyTuple_GET_ITEM(kwnames, i);
            PyObject *val = args[nargs + i];
            if (!PyUnicode_Check(key)) {
                PyErr_SetString(PyExc_TypeError, "run() keyword names must be strings");
                return NULL;
            }
            if (PyUnicode_CompareWithASCIIString(key, "function") == 0) {
                if (func != NULL) {
                    PyErr_SetString(PyExc_TypeError, "run() got multiple values for argument 'function'");
                    return NULL;
                }
                func = val;
            } else if (PyUnicode_CompareWithASCIIString(key, "arg") == 0) {
                if (farg != NULL) {
                    PyErr_SetString(PyExc_TypeError, "run() got multiple values for argument 'arg'");
                    return NULL;
                }
                farg = val;
            } else {
                PyErr_Format(PyExc_TypeError, "run() got an unexpected keyword argument '%U'", key);
                return NULL;
            }
        }
    }

    if (func == NULL) {
        PyErr_SetString(PyExc_TypeError, "run() missing required argument 'function' (pos 1)");
        return NULL;
    }
    if (farg == NULL)
        farg = Py_None;

    created_from_new = (target->state == STATE_NEW);
    mdata = (PyTealetMainData *)*tealet_main_userpointer(current->tealet);
    ptarg = &mdata->new_arg;
    switch_arg = (void *)ptarg;

    ptarg->dest = target;
    ptarg->mstate = mstate;
    ptarg->func = func;
    ptarg->arg = farg;

    if (!created_from_new) {
        if (PyTealetFrameInfo_Capture(&current->frame_info, 1) < 0)
            PyErr_Clear();
        PyTealetTstate_Save(&current->tstate, tstate);
        fail = tealet_stub_run(target->tealet, pytealet_main, &switch_arg);
        PyTealetTstate_Restore(&current->tstate, tstate);
        PyTealetFrameInfo_Release(&current->frame_info, NULL);
        if (fail) {
            PyErr_NoMemory();
            result = NULL;
            goto run_cleanup;
        }
    } else {
        void *stack_limit = PyTealet_GetStackFar(tstate);
        PyTealetTstate_Copy(&current->tstate, tstate);
        tealet = tealet_new(current->tealet, pytealet_main, &switch_arg, stack_limit);
        if (!tealet) {
            PyTealetTstate_Drop(&current->tstate, NULL);
            PyErr_NoMemory();
            result = NULL;
            goto run_cleanup;
        }
        PyTealetTstate_Restore(&current->tstate, tstate);
    }

    result = (PyObject *)switch_arg;
run_cleanup:
    dustbin_clear(current->tealet);
    return result;
}

/* switch to a different tealet */
static PyObject *pytealet_switch(PyObject *self, PyTypeObject *defining_class, PyObject *const *args, Py_ssize_t nargs,
                                 PyObject *kwnames) {
    PyTealetModuleState *mstate = (PyTealetModuleState *)PyType_GetModuleState(defining_class);
    PyTealetObject *target = (PyTealetObject *)self;
    PyTealetObject *current;
    int fail;
    PyThreadState *tstate = PyThreadState_GET();
    PyObject *pyarg = Py_None;
    void *switch_arg;
    PyObject *result;
    if (!mstate)
        return NULL;

    if (kwnames && PyTuple_GET_SIZE(kwnames) > 0) {
        PyErr_SetString(PyExc_TypeError, "switch() takes no keyword arguments");
        return NULL;
    }
    if (nargs > 1) {
        PyErr_Format(PyExc_TypeError, "switch() takes at most 1 argument (%zd given)", nargs);
        return NULL;
    }
    if (nargs == 1)
        pyarg = args[0];

    if (target->state != STATE_RUN) {
        PyErr_SetString(mstate->state_error, "must be active");
        return NULL;
    }
    assert(target->tealet);
	/* we don't have a source tealet, so we must get it from the thread state. */
    current = GetCurrent(mstate, NULL, 0);
    if (!current || CheckTarget(mstate, target, current))
        return NULL;

    Py_INCREF(pyarg);
    switch_arg = (void *)pyarg;
    if (PyTealetFrameInfo_Capture(&current->frame_info, 1) < 0)
        PyErr_Clear();
    PyTealetTstate_Save(&current->tstate, tstate);
    fail = tealet_switch(target->tealet, &switch_arg);
    PyTealetTstate_Restore(&current->tstate, tstate);
    PyTealetFrameInfo_Release(&current->frame_info, NULL);

    dustbin_clear(current->tealet);

    if (fail == TEALET_ERR_DEFUNCT) {
        Py_DECREF(pyarg);
        PyErr_SetString(mstate->defunct_error, "target is defunct");
        return NULL;
    } else if (fail == TEALET_ERR_MEM) {
        Py_DECREF(pyarg);
        return PyErr_NoMemory();
    }
    result = (PyObject *)switch_arg;
    return result;
}

static struct PyMethodDef pytealet_methods[] = {
    {"stub", (PyCFunction)(void (*)(void))pytealet_stub, METH_METHOD | METH_FASTCALL | METH_KEYWORDS, ""},
    {"current", (PyCFunction)(void (*)(void))pytealet_current, METH_METHOD | METH_FASTCALL | METH_KEYWORDS, ""},
    {"previous", (PyCFunction)(void (*)(void))pytealet_previous, METH_METHOD | METH_FASTCALL | METH_KEYWORDS, ""},
    {"main", (PyCFunction)(void (*)(void))pytealet_main_method, METH_METHOD | METH_FASTCALL | METH_KEYWORDS, ""},
    {"run", (PyCFunction)(void (*)(void))pytealet_run, METH_METHOD | METH_FASTCALL | METH_KEYWORDS, ""},
    {"switch", (PyCFunction)(void (*)(void))pytealet_switch, METH_METHOD | METH_FASTCALL | METH_KEYWORDS, ""},
    {NULL, NULL} /* sentinel */
};

/************
 * Properties
 */
static PyObject *pytealet_get_main(PyObject *_self, void *_closure) {
    PyTealetObject *self = (PyTealetObject *)_self;
    PyTealetModuleState *mstate = GetModuleStateFromClass(Py_TYPE(self));
    if (!mstate)
        return NULL;

    if (!self->tealet) {
       /* happens only for new tealets, not yet run.  then we have to find the current for this thread.
		 * but we don't attempt to create a new one.
		 */
        return Py_XNewRef((PyObject *)GetMain(mstate, 0));
    } else {
        return GetWrapperRef(self->tealet->main);
    }
}

static PyObject *pytealet_get_state(PyObject *_self, void *_closure) {
    PyTealetObject *self = (PyTealetObject *)_self;
    return PyLong_FromLong(self->state);
}

static PyObject *pytealet_get_frame(PyObject *_self, void *_closure) {
    PyTealetObject *self = (PyTealetObject *)_self;
    PyTealetObject *current;
    PyTealetModuleState *mstate = GetModuleStateFromClass(Py_TYPE(self));
    if (!mstate)
        return NULL;
#if defined(PY_HAS_TSTATE_FRAME)
    PyObject *frame = self->tstate.has_state ? (PyObject *)self->tstate.frame : NULL;
#else
    PyObject *frame = self->tstate.has_state ? (PyObject *)self->frame_info.frame : NULL;
#endif
    if (frame)
        return Py_NewRef(frame);

    /* No stored frame (e.g. new/stub): only current tealet exposes live frame. */
    current = GetCurrent(mstate, NULL, 0);
    if (current == self)
        frame = (PyObject *)PyEval_GetFrame();

    if (!frame)
        frame = Py_None;
    return Py_NewRef(frame);
}

static PyObject *pytealet_get_tid(PyObject *_self, void *_closure) {
    PyTealetObject *self = (PyTealetObject *)_self;
    long tid = 0;
    if (self->tealet) {
        PyTealetMainData *mdata = (PyTealetMainData *)*tealet_main_userpointer(self->tealet);
        tid = mdata->tid;
    }
    return PyLong_FromLong(tid);
}

static struct PyGetSetDef pytealet_getset[] = {{"state", pytealet_get_state, NULL, "", NULL},
                                               {"frame", pytealet_get_frame, NULL, "", NULL},
                                               {"thread_id", pytealet_get_tid, NULL, "", NULL},
                                               {0}};

/* ===================================================================== */
/* Python Type Metadata                                                  */
/* ===================================================================== */

/* CPython type slot table stores C function pointers in void* fields by API
 * design. */
#if defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#endif
static PyType_Slot pytealet_type_slots[] = {{Py_tp_dealloc, pytealet_dealloc},
                                            {Py_tp_methods, pytealet_methods},
                                            {Py_tp_getset, pytealet_getset},
                                            {Py_tp_new, pytealet_new},
                                            {0, NULL}};
#if defined(__GNUC__)
#pragma GCC diagnostic pop
#endif

static PyType_Spec pytealet_type_spec = {"_tealet.tealet", sizeof(PyTealetObject), 0,
                                         Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, pytealet_type_slots};

/* ===================================================================== */
/* Runtime Support (Allocator and Lineage)                               */
/* ===================================================================== */

/* Wrapper functions for system malloc/free to match libtealet's allocator API.
 */
static void *tealet_malloc_wrapper(size_t size, void *context) {
    (void)context; /* unused */
    return malloc(size);
}

static void tealet_free_wrapper(void *ptr, void *context) {
    (void)context; /* unused */
    free(ptr);
}

/* return a borrowed reference to this thread's main tealet */
static PyTealetObject *GetMain(PyTealetModuleState *mstate, int create) {
    /* Get the thread's main tealet */
    PyTealetObject *t_main;
    if (!mstate)
        return NULL;
    t_main = (PyTealetObject *)PyThread_tss_get(&mstate->tls_key);
    if (!t_main && !create) {
        return NULL;
    }

    /* main tealet doesn't exist yet.  create it. */
    if (!t_main) {
        tealet_alloc_t talloc;
        tealet_t *tmain;
        PyTealetMainData *mdata;
        /* Use system malloc/free so valgrind can detect heap corruption */
        talloc.malloc_p = tealet_malloc_wrapper;
        talloc.free_p = tealet_free_wrapper;
        talloc.context = NULL;
        tmain = tealet_initialize(&talloc, sizeof(PyTealetExtra));
        if (!tmain) {
            PyErr_NoMemory();
            return NULL;
        }
        {
            const char *check_stack_env = getenv("PYTEALET_CHECK_STACK");
            if (check_stack_env && *check_stack_env && *check_stack_env != '0') {
                if (tealet_configure_check_stack(tmain, 0) < 0) {
                    tealet_finalize(tmain);
                    PyErr_SetString(PyExc_RuntimeError, "tealet_configure_check_stack failed");
                    return NULL;
                }
            }
        }
        mdata = (PyTealetMainData *)PyMem_Malloc(sizeof(*mdata));
        if (!mdata) {
            tealet_finalize(tmain);
            PyErr_NoMemory();
            return NULL;
        }
        memset(mdata, 0, sizeof(*mdata));
        mdata->tid = PyThread_get_thread_ident();
        mdata->dustbin = PyList_New(DUSTBIN_PREALLOC);
        if (!mdata->dustbin) {
            tealet_finalize(tmain);
            PyMem_Free(mdata);
            PyErr_NoMemory();
            return NULL;
        }
        if (PyList_SetSlice(mdata->dustbin, 0, DUSTBIN_PREALLOC, NULL) < 0) {
            Py_DECREF(mdata->dustbin);
            tealet_finalize(tmain);
            PyMem_Free(mdata);
            return NULL;
        }
        *tealet_main_userpointer(tmain) = (void *)mdata;

        /* create the main tealet */
        t_main = (PyTealetObject *)pytealet_new(mstate->tealet_type, NULL, NULL);
        if (!t_main) {
            tealet_finalize(tmain);
            PyMem_Free(mdata);
            return NULL;
        }
        t_main->tealet = tmain;
        t_main->state = STATE_RUN;
        TEALET_SET_PYOBJECT(tmain, t_main); /* back link */
        if (PyThread_tss_set(&mstate->tls_key, (void *)t_main) != 0) {
            TEALET_SET_PYOBJECT(tmain, NULL);
            t_main->tealet = NULL;
            Py_DECREF(t_main);
            tealet_finalize(tmain);
            PyMem_Free(mdata);
            PyErr_SetString(PyExc_RuntimeError, "failed to set thread-local main tealet");
            return NULL;
        }
    }
    assert(t_main->tealet);
    assert(TEALET_IS_MAIN(t_main->tealet));
    assert(t_main->state == STATE_RUN);
    return t_main;
}

/* return a borrowed ref to this threads current tealet */
static PyTealetObject *GetCurrent(PyTealetModuleState *mstate, PyTealetObject *pytealet, int create_main) {
    /* if we are being passed no tealet, or it is a new tealet,
     * we must get the current main from the thread-local storage */
    if (!pytealet || !pytealet->tealet)
        pytealet = GetMain(mstate, create_main);
    if (!pytealet)
        return NULL;
    return TEALET_PYOBJECT(tealet_current(pytealet->tealet));
}

/* check if a target tealet is valid */
static int CheckTarget(PyTealetModuleState *mstate, PyTealetObject *target, PyTealetObject *ref) {
    if (!ref)
        ref = GetMain(mstate, 1);
    if (!ref)
        return -1;
    if (!target->tealet)
        return 0; /* no tealet yet */
    if (ref->tealet->main != target->tealet->main) {
        PyErr_SetString(mstate->invalid_error, "foreign tealet");
        return -1;
    }
    return 0;
}

/* ===================================================================== */
/* Core Runtime Switching Callback                                       */
/* ===================================================================== */

/* The main function.  Invoked either from tealet.new or tealet.run */
static tealet_t *pytealet_main(tealet_t *t_current, void *arg) {
    PyTealetNewArg *targ = (PyTealetNewArg *)arg;
    PyTealetModuleState *mstate = targ->mstate;
    PyTealetObject *tealet = targ->dest;
    PyObject *func = targ->func;
    PyObject *farg = targ->arg;
    PyObject *result, *return_arg;
    PyTealetObject *return_to;
    tealet_t *t_return;
    int exit_mode = TEALET_EXIT_DELETE;
    PyThreadState *tstate = PyThreadState_GET();
#if defined(PY_HAS_CFRAME)
    PyTealetCFrame trace_info;
#endif

    if (tealet->state == STATE_STUB) {
        assert(t_current == tealet->tealet);
        assert(TEALET_PYOBJECT(t_current) == tealet);

        /* set the tstate from our own copy */
        PyTealetTstate_Restore(&tealet->tstate, tstate);
#if !defined(PY_HAS_TSTATE_FRAME)
        PyTealetFrameInfo_Release(&tealet->frame_info, NULL);
#endif
    } else {
        assert(tealet->state == STATE_NEW);
        /* set up the pointer in the tealet */
        tealet->tealet = t_current;
        TEALET_SET_PYOBJECT(t_current, tealet);
#if defined(Py311P)
        /* First entry of a brand-new tealet must not inherit parent eval
         * frame/datastack links from another C stack.
         */
        trace_info = tstate->root_cframe;
        tstate->cframe = &trace_info;
        tstate->cframe->previous = &tstate->root_cframe;
        tstate->cframe->current_frame = NULL;
        tstate->datastack_chunk = NULL;
        tstate->datastack_top = NULL;
        tstate->datastack_limit = NULL;
#endif
    }

    /* We only have borrowed references from the calling tealet.
     * the argument to the function will get their own reference, but
     * anything we need after the function we keep oru own references
     * for, because when the function returns, the calling tealet
     * may have exited and dropped the references we borrowed.
     */
    Py_INCREF(func);
    Py_INCREF(tealet);

    /* clear frame and run the tealet function */
    tealet->state = STATE_RUN;
    result = PyObject_CallFunctionObjArgs(func, tealet, farg, NULL);

    /* return_to can be a tuple of tealet, arg */
    return_to = NULL;
    return_arg = NULL;
    if (result && PyTuple_Check(result)) {
        /* arg and return_to are borrowed refs */
        if (PyTuple_GET_SIZE(result) > 0)
            return_to = (PyTealetObject *)PyTuple_GET_ITEM(result, 0);
        if (PyTuple_GET_SIZE(result) > 1)
            return_arg = PyTuple_GET_ITEM(result, 1);
    } else
        return_to = (PyTealetObject *)result;

    /* perform sanity checks on the result */
    if (return_to) {
        /* it is ok to rock the GC boat here, because we will switch to
         * main in case of error, and main is always around
         */
        if (!PyTealet_Check((PyObject *)return_to, mstate)) {
            return_to = NULL;
            PyErr_SetString(PyExc_TypeError, "tealet object expected");
        } else if (return_to->state != STATE_RUN) {
            return_to = NULL;
            PyErr_SetString(mstate->state_error, "must be 'run'");
        } else if (CheckTarget(mstate, return_to, tealet))
            return_to = NULL;
    }
    if (!return_to) {
        Py_CLEAR(result);
        return_arg = NULL;
    }
    if (!return_arg)
        return_arg = Py_None;

    /* handle errors */
    if (!return_to) {
        PyErr_WriteUnraisable(func);
        /* must switch to main */
        return_to = GetMain(mstate, 0);
        assert(return_to);
        result = (PyObject *)return_to;
        Py_INCREF(result);
    }
    /* now, the reference to return_to and return_arg are borrowed, kept alive
     * by 'result', which may be the same as return_to.
     */

    /* clear the old tealet */
    tealet->state = STATE_EXIT;
    if (PYTEALET_DEFER_DELETE)
        exit_mode = TEALET_EXIT_DEFAULT;
    if (exit_mode == TEALET_EXIT_DELETE) {
        tealet->tealet = NULL; /* will be auto-deleted on return */
        TEALET_SET_PYOBJECT(t_current, NULL);
    }
    t_return = return_to->tealet;

    /* decref the objects after the switch */
    dustbin_push(t_return, func);
    dustbin_push(t_return, (PyObject *)tealet);
    dustbin_push(t_return, result);

    Py_INCREF(return_arg);

    /* Tealet is exiting permanently: clear active PyThreadState for the switch,
     * then drop saved refs immediately so frame locals (including 'current')
     * do not keep the Python tealet object alive until GC.
     */
#if !defined(PY_HAS_TSTATE_FRAME)
    if (PyTealetFrameInfo_Capture(&tealet->frame_info, 1) < 0)
        PyErr_Clear();
#endif
    PyTealetTstate_Save(&tealet->tstate, tstate);
#if !defined(PY_HAS_TSTATE_FRAME)
    PyTealetFrameInfo_Release(&tealet->frame_info, t_return);
#endif
    PyTealetTstate_Drop(&tealet->tstate, t_return);

    if (tealet_exit(t_return, (void *)return_arg, exit_mode))
        tealet_exit(t_return->main, (void *)return_arg, exit_mode);
    /* never reach here */
    return 0;
}

/* ===================================================================== */
/* Module API and Lifecycle                                              */
/* ===================================================================== */

static PyObject *module_current(PyObject *mod, PyObject *Py_UNUSED(_ignored)) {
    PyTealetModuleState *mstate = (PyTealetModuleState *)PyModule_GetState(mod);
    if (!mstate) {
        PyErr_SetString(PyExc_RuntimeError, "_tealet module state unavailable");
        return NULL;
    }
	/* get the current.  if there is no main tealet at this time, create it. */
    return Py_XNewRef((PyObject *)GetCurrent(mstate, NULL, 1));
}

static PyObject *module_main(PyObject *mod, PyObject *Py_UNUSED(_ignored)) {
    PyTealetModuleState *mstate = (PyTealetModuleState *)PyModule_GetState(mod);
    if (!mstate) {
        PyErr_SetString(PyExc_RuntimeError, "_tealet module state unavailable");
        return NULL;
    }
	/* create main if it doesn't already exist for this thread */
    return Py_XNewRef((PyObject *)GetMain(mstate, 1));
}

static PyMethodDef module_methods[] = {
    {"current", (PyCFunction)module_current, METH_NOARGS, ""},
    {"main", (PyCFunction)module_main, METH_NOARGS, ""},
    {NULL, NULL, 0, NULL} /* Sentinel */
};

static int pytealet_module_exec(PyObject *m) {
    PyTealetModuleState *mstate = (PyTealetModuleState *)PyModule_GetState(m);
    PyTealetObject *main;
    PyObject *type_obj;

    if (!mstate) {
        PyErr_SetString(PyExc_RuntimeError, "failed to get _tealet module state");
        return -1;
    }

    memset(&mstate->tls_key, 0, sizeof(mstate->tls_key));
    mstate->tealet_type = NULL;
    mstate->tealet_error = NULL;
    mstate->invalid_error = NULL;
    mstate->state_error = NULL;
    mstate->defunct_error = NULL;

    if (!PyThread_tss_is_created(&mstate->tls_key)) {
        if (PyThread_tss_create(&mstate->tls_key) != 0) {
            PyErr_SetString(PyExc_RuntimeError, "failed to create thread-local key");
            return -1;
        }
    }

    type_obj = PyType_FromModuleAndSpec(m, &pytealet_type_spec, NULL);
    if (!type_obj)
        return -1;
    mstate->tealet_type = (PyTypeObject *)type_obj;
    if (PyModule_AddObjectRef(m, "tealet", type_obj) < 0) {
        Py_DECREF(type_obj);
        return -1;
    }
    Py_DECREF(type_obj);

    main = GetMain(mstate, 1);
    if (!main)
        return -1;

    mstate->tealet_error = PyErr_NewException("_tealet.TealetError", NULL, NULL);
    if (!mstate->tealet_error)
        return -1;
    Py_INCREF(mstate->tealet_error);
    if (PyModule_AddObject(m, "TealetError", mstate->tealet_error) < 0)
        return -1;

    mstate->defunct_error = PyErr_NewException("_tealet.DefunctError", mstate->tealet_error, NULL);
    if (!mstate->defunct_error)
        return -1;
    Py_INCREF(mstate->defunct_error);
    if (PyModule_AddObject(m, "DefunctError", mstate->defunct_error) < 0)
        return -1;

    mstate->invalid_error = PyErr_NewException("_tealet.InvalidError", mstate->tealet_error, NULL);
    if (!mstate->invalid_error)
        return -1;
    Py_INCREF(mstate->invalid_error);
    if (PyModule_AddObject(m, "InvalidError", mstate->invalid_error) < 0)
        return -1;

    mstate->state_error = PyErr_NewException("_tealet.StateError", mstate->tealet_error, NULL);
    if (!mstate->state_error)
        return -1;
    Py_INCREF(mstate->state_error);
    if (PyModule_AddObject(m, "StateError", mstate->state_error) < 0)
        return -1;

    PyModule_AddIntMacro(m, STATE_NEW);
    PyModule_AddIntMacro(m, STATE_STUB);
    PyModule_AddIntMacro(m, STATE_RUN);
    PyModule_AddIntMacro(m, STATE_EXIT);
    if (PyModule_AddIntConstant(m, "PYTEALET_DEFER_DELETE", PYTEALET_DEFER_DELETE) < 0)
        return -1;

    return 0;
}

static int pytealet_module_traverse(PyObject *m, visitproc visit, void *arg) {
    PyTealetModuleState *mstate = (PyTealetModuleState *)PyModule_GetState(m);
    if (!mstate)
        return 0;
    Py_VISIT(mstate->tealet_error);
    Py_VISIT(mstate->invalid_error);
    Py_VISIT(mstate->state_error);
    Py_VISIT(mstate->defunct_error);
    return 0;
}

static int pytealet_module_clear(PyObject *m) {
    PyTealetModuleState *mstate = (PyTealetModuleState *)PyModule_GetState(m);
    if (!mstate)
        return 0;
    Py_CLEAR(mstate->tealet_error);
    Py_CLEAR(mstate->invalid_error);
    Py_CLEAR(mstate->state_error);
    Py_CLEAR(mstate->defunct_error);
    mstate->tealet_type = NULL;
    return 0;
}

static void pytealet_module_free(void *m) {
    PyTealetModuleState *mstate = (PyTealetModuleState *)PyModule_GetState((PyObject *)m);
    if (!mstate)
        return;
    /* TODO: Per-thread teardown for mstate->tls_key is deferred.
     * Deleting the TSS key does not decref thread-local PyObject* values.
     * Implement per-mstate thread shutdown cleanup in a follow-up change.
     */
    if (PyThread_tss_is_created(&mstate->tls_key))
        PyThread_tss_delete(&mstate->tls_key);
}

/* CPython API uses void* in module slots; this conversion is intentional. */
#if defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#endif
static PyModuleDef_Slot _tealet_module_slots[] = {{Py_mod_exec, pytealet_module_exec}, {0, NULL}};
#if defined(__GNUC__)
#pragma GCC diagnostic pop
#endif

static struct PyModuleDef _tealet_module = {PyModuleDef_HEAD_INIT,
                                            "_tealet", /* name of module */
                                            NULL,      /* module documentation, may be NULL */
                                            sizeof(PyTealetModuleState),
                                            module_methods,
                                            _tealet_module_slots,
                                            pytealet_module_traverse,
                                            pytealet_module_clear,
                                            pytealet_module_free};

PyMODINIT_FUNC PyInit__tealet(void) { return PyModuleDef_Init(&_tealet_module); }


#include "Python.h"
#include "frameobject.h"
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

#if PY_VERSION_HEX >= 0x030B0000 && PY_VERSION_HEX < 0x030C0000
#define PY311 1
#endif

#if PY_VERSION_HEX >= 0x030C0000 && PY_VERSION_HEX < 0x030D0000
#define PY312 1
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
    PyFrameObject *frame;
    PyObject *exc_type;
    PyObject *exc_val;
    PyObject *exc_tb;
    _PyErr_StackItem *exc_info;
    _PyErr_StackItem exc_state;
    int recursion_depth;
    int trash_delete_nesting;
    PyObject *context; /* Python 3.7+ contextvars */
    int has_state;     /* Debug helper: 1 when this struct currently stores a saved
                          tstate */
    /* Python 3.10-3.12: cframe tracks C-level call frames (removed in 3.13)
     * Stack-slicing preserves the CFrame struct itself; we just save the
     * pointer */
#if defined(PY_HAS_CFRAME)
    CFrame *cframe;
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
};

/* helpers for getting main and current and checking relationship */
static PyTealetModuleState *GetModuleStateFromClass(PyTypeObject *cls, int set_error);
static PyTealetObject *GetMain(PyTealetModuleState *mstate, int create);
static PyTealetObject *GetCurrent(PyTealetModuleState *mstate, PyTealetObject *main, int create_main);
static int CheckTarget(PyTealetModuleState *mstate, PyTealetObject *target, PyTealetObject *main);

static tealet_t *pytealet_main(tealet_t *t_current, void *arg);

/* ===================================================================== */
/* Type and Module Access Helpers                                        */
/* ===================================================================== */

/* TODO(py311+): For __init__/tp_init and other paths that only have a type,
 * prefer PyType_GetModuleByDef(type, &_tealet_module) to resolve this module,
 * then PyModule_GetState(module). This should replace fallback class-walk logic
 * once our minimum supported Python version includes that API.
 */
static PyTealetModuleState *GetModuleStateFromClass(PyTypeObject *cls, int set_error) {
    PyTypeObject *cur = cls;
    while (cur) {
        PyTealetModuleState *mstate = (PyTealetModuleState *)PyType_GetModuleState(cur);
        if (mstate)
            return mstate;
        if (PyErr_Occurred()) {
            if (PyErr_ExceptionMatches(PyExc_TypeError))
                PyErr_Clear();
            else
                return NULL;
        }
        cur = cur->tp_base;
    }
    if (set_error)
        PyErr_SetString(PyExc_RuntimeError, "_tealet module state unavailable");
    return NULL;
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

static void PyTealetTstate_Init(PyTealetTstate *saved) { saved->has_state = 0; }

/* Raw copy the tstate files from PyThreadState to our local structure */
static void PyTealetTstate_Get(PyTealetTstate *dst, const PyThreadState *src) {
    dst->frame = src->frame;
    dst->recursion_depth = src->recursion_depth;

    dst->exc_type = src->curexc_type;
    dst->exc_val = src->curexc_value;
    dst->exc_tb = src->curexc_traceback;

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
    dst->trash_delete_nesting = src->trash_delete_nesting;
}

/* Raw copy previously saved tealet tstate into PyThreadState. */
static void PyTealetTstate_Put(const PyTealetTstate *src, PyThreadState *dst) {
    dst->frame = src->frame;
    dst->recursion_depth = src->recursion_depth;

    dst->curexc_type = src->exc_type;
    dst->curexc_value = src->exc_val;
    dst->curexc_traceback = src->exc_tb;

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
    dst->trash_delete_nesting = src->trash_delete_nesting;
}

/* Increment and decrement the reference count of the tstate's references.
 * we need to Increment the references when we create new tealets from an
 * existing one (or main), and decrement when a tealet terminates.
 */
static void PyTealetTstate_IncRef(PyTealetTstate *saved) {
    assert(saved->has_state == 1);
    Py_XINCREF(saved->frame);
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
        dustbin_push(dustbin_tealet, (PyObject *)saved->frame);
        dustbin_push(dustbin_tealet, saved->exc_type);
        dustbin_push(dustbin_tealet, saved->exc_val);
        dustbin_push(dustbin_tealet, saved->exc_tb);
        dustbin_push(dustbin_tealet, saved->exc_state.exc_value);
        dustbin_push(dustbin_tealet, saved->context);
    } else {
        Py_XDECREF(saved->frame);
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
    py_tstate->frame = NULL;
    py_tstate->curexc_type = NULL;
    py_tstate->curexc_value = NULL;
    py_tstate->curexc_traceback = NULL;
    py_tstate->exc_info = NULL; /* use this as a sentinel, should never be null
                                   in a valid situation */
    py_tstate->exc_state.exc_value = NULL;
    py_tstate->recursion_depth = 0;
    py_tstate->trash_delete_nesting = 0;
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
    PyTealetModuleState *mstate = GetModuleStateFromClass(subtype, 1);
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
        PyTealetTstate_Save(&current->tstate, tstate);
        fail = tealet_stub_run(target->tealet, pytealet_main, &switch_arg);
        PyTealetTstate_Restore(&current->tstate, tstate);
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
    PyTealetTstate_Save(&current->tstate, tstate);
    fail = tealet_switch(target->tealet, &switch_arg);
    PyTealetTstate_Restore(&current->tstate, tstate);

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
    PyTealetModuleState *mstate = GetModuleStateFromClass(Py_TYPE(self), 1);
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
    PyTealetModuleState *mstate = GetModuleStateFromClass(Py_TYPE(self), 1);
    if (!mstate)
        return NULL;
    PyObject *frame = self->tstate.has_state ? (PyObject *)self->tstate.frame : NULL;
    if (!frame) {
        /* is it the current tealet of the current thread? */
        if (self == GetCurrent(mstate, NULL, 0)) {
            PyThreadState *tstate = PyThreadState_GET();
            frame = (PyObject *)tstate->frame;
        }
    }
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

    if (tealet->state == STATE_STUB) {
        assert(t_current == tealet->tealet);
        assert(TEALET_PYOBJECT(t_current) == tealet);

        /* set the tstate from our own copy */
        PyTealetTstate_Restore(&tealet->tstate, tstate);
    } else {
        assert(tealet->state == STATE_NEW);
        /* set up the pointer in the tealet */
        tealet->tealet = t_current;
        TEALET_SET_PYOBJECT(t_current, tealet);
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
    PyTealetTstate_Save(&tealet->tstate, tstate);
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

static PyObject *hide_frame(PyObject *self, PyObject *_args) {
    /* this function calls a method, clearing the frame.  This hides
     * higher frames in the callstack
     */
    PyObject *func, *args = NULL, *kwds = NULL;
    PyThreadState *tstate = PyThreadState_GET();
    PyFrameObject *f = tstate->frame;
    PyObject *result;
    if (!PyArg_ParseTuple(_args, "O|OO:hide_frame", &func, &args, &kwds))
        return NULL;
    if (!args) {
        PyObject *empty = PyTuple_New(0);
        if (!empty)
            return NULL;
        tstate->frame = NULL;
        result = PyObject_Call(func, empty, kwds);
        Py_DECREF(empty);
    } else {
        tstate->frame = NULL;
        result = PyObject_Call(func, args, kwds);
    }
    tstate->frame = f;
    return result;
}

static PyMethodDef module_methods[] = {
    {"current", (PyCFunction)module_current, METH_NOARGS, ""},
    {"main", (PyCFunction)module_main, METH_NOARGS, ""},
    {"hide_frame", (PyCFunction)hide_frame, METH_VARARGS, ""},
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

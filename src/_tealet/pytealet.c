
/* pytealet.c - core runtime logic for Python tealet objects.
 *
 * This file implements active tealet behavior: object methods, switch/run paths,
 * runtime helpers, and thread-state integration used during context switches.
 */

#include "Python.h"
#include "frameobject.h"
#include "pythread.h"
#include "structmember.h"
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "frame_info.h"
#include "pytealet.h"
#include "pytealet_module.h"
#include "tealet.h"
#include "tealet_extras.h"
#include "tstate_state.h"

/* ===================================================================== */
/* Core Types and Module State                                           */
/* ===================================================================== */

typedef struct PyTealetNewArg {
    PyTealetObject *dest;
    PyTealetModuleState *mstate;
    PyObject *func;
    PyObject *arg;
} PyTealetNewArg;

/* the structure we associate with the main tealet */
struct PyTealetMainData {
    long tid;
    struct PyTealetMainData *ring_prev;
    struct PyTealetMainData *ring_next;
    PyTealetNewArg new_arg;
    PyObject *dustbin;
    PyObject *main_wrapper; /* strong ref to this thread's main tealet wrapper */
    PyObject *wrappers;     /* set of weakrefs to non-main wrappers in this main lineage */
#if PYTEALET_FREE_THREADED
    PyThread_type_lock domain_lock;
#endif
};

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

/* The python tealet object */
struct PyTealetObject {
    PyObject_HEAD int state;
    tealet_t *tealet;
    unsigned long owner_tid; /* thread that owns this tealet object */
    PyObject *tracking_ref;  /* weakref object stored in main-lineage wrapper set */
#if !defined(Py312P)
    PyObject *weakreflist; /* List of weak references */
#endif

    /* thread state information */
    PyTealetTstate tstate;
    /* Dormant frame snapshot and (3.12+) reversible frame rewrites. */
    PyTealetFrameInfo frame_info;
};

/* helpers for getting main and current and checking relationship */
static PyTealetModuleState *GetModuleStateFromClass(PyTypeObject *cls);
PyTealetObject *GetMain(PyTealetModuleState *mstate, int create, PyTealetMainData **mdata_out);
PyTealetObject *GetCurrent(PyTealetModuleState *mstate, PyTealetObject *main, int create_main,
                           PyTealetMainData **mdata_out);
static int CheckTarget(PyTealetModuleState *mstate, PyTealetObject *target, PyTealetObject *main);
static PyObject *pytealet_new_impl(PyTypeObject *subtype, PyObject *args, PyObject *kwds, int creating_main);
static int pytealet_track_wrapper(PyTealetMainData *mdata, PyTealetObject *wrapper);
static void pytealet_untrack_wrapper(PyTealetObject *wrapper);
static int pytealet_link_thread_data(PyTealetModuleState *mstate, PyTealetMainData *mdata);
static void pytealet_unlink_thread_data(PyTealetModuleState *mstate, PyTealetMainData *mdata);

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

static int PyTealet_SetPanicErrorWithValue(PyTealetModuleState *mstate, const char *what, PyObject *value) {
    PyObject *exc_type;
    PyObject *msg_obj;
    PyObject *exc_obj;
    const char *msg = what ? what : "tealet panic";

    if (!mstate || !mstate->panic_error) {
        PyErr_SetString(PyExc_RuntimeError, msg);
        return -1;
    }

    exc_type = mstate->panic_error;
    msg_obj = PyUnicode_FromString(msg);
    if (!msg_obj)
        return -1;
    exc_obj = PyObject_CallFunctionObjArgs(exc_type, msg_obj, NULL);
    Py_DECREF(msg_obj);
    if (!exc_obj)
        return -1;

    if (!value)
        value = Py_None;
    if (PyObject_SetAttrString(exc_obj, "value", value) < 0) {
        Py_DECREF(exc_obj);
        return -1;
    }

    PyErr_SetObject(exc_type, exc_obj);
    Py_DECREF(exc_obj);
    return -1;
}

/* Translate libtealet TEALET_ERR_* codes to Python exceptions.
 * panic_value is an owned reference that is consumed (stolen) by this helper.
 */
static int PyTealet_TranslateTealetError(PyTealetModuleState *mstate, int err, const char *what,
                                         PyObject *panic_value) {
    const char *msg = what ? what : "tealet operation failed";
    if (err != TEALET_ERR_PANIC && panic_value) {
        Py_DECREF(panic_value);
        panic_value = NULL;
    }
    if (err == 0)
        return 0;
    if (err == TEALET_ERR_MEM) {
        PyErr_NoMemory();
        return -1;
    }
    if (err == TEALET_ERR_DEFUNCT) {
        if (mstate && mstate->defunct_error)
            PyErr_SetString(mstate->defunct_error, msg);
        else
            PyErr_SetString(PyExc_RuntimeError, msg);
        return -1;
    }
    if (err == TEALET_ERR_PANIC) {
        int tr = PyTealet_SetPanicErrorWithValue(mstate, msg, panic_value ? panic_value : Py_None);
        Py_XDECREF(panic_value);
        return tr;
    }
    if (err == TEALET_ERR_INVAL) {
        PyErr_SetString(PyExc_RuntimeError, msg);
        return -1;
    }
#ifdef TEALET_ERR_INTEGRITY
    if (err == TEALET_ERR_INTEGRITY) {
        PyErr_SetString(PyExc_RuntimeError, msg);
        return -1;
    }
#endif
    PyErr_Format(PyExc_RuntimeError, "%s (libtealet error %d)", msg, err);
    return -1;
}

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

static int pytealet_track_wrapper(PyTealetMainData *mdata, PyTealetObject *wrapper) {
    PyObject *wref;

    assert(mdata);
    assert(wrapper);
    assert(mdata->wrappers);
    assert(!wrapper->tracking_ref);
    assert(wrapper->tealet);
    assert(!TEALET_IS_MAIN(wrapper->tealet));

    wref = PyWeakref_NewRef((PyObject *)wrapper, NULL);
    if (!wref)
        return -1;
    if (PySet_Add(mdata->wrappers, wref) < 0) {
        Py_DECREF(wref);
        return -1;
    }
    wrapper->tracking_ref = wref;
    return 0;
}

static int pytealet_link_thread_data(PyTealetModuleState *mstate, PyTealetMainData *mdata) {
    PyTealetMainData *head;

    assert(mstate);
    assert(mdata);
    assert(mstate->thread_data_lock);

    PyThread_acquire_lock(mstate->thread_data_lock, WAIT_LOCK);
    head = mstate->thread_data_ring;
    if (!head) {
        mdata->ring_prev = mdata;
        mdata->ring_next = mdata;
        mstate->thread_data_ring = mdata;
    } else {
        PyTealetMainData *tail = head->ring_prev;
        assert(tail);
        mdata->ring_next = head;
        mdata->ring_prev = tail;
        tail->ring_next = mdata;
        head->ring_prev = mdata;
    }
    PyThread_release_lock(mstate->thread_data_lock);
    return 0;
}

static void pytealet_unlink_thread_data(PyTealetModuleState *mstate, PyTealetMainData *mdata) {
    assert(mstate);
    assert(mdata);
    assert(mstate->thread_data_lock);

    PyThread_acquire_lock(mstate->thread_data_lock, WAIT_LOCK);
    if (mdata->ring_next && mdata->ring_prev) {
        if (mdata->ring_next == mdata) {
            assert(mdata->ring_prev == mdata);
            if (mstate->thread_data_ring == mdata)
                mstate->thread_data_ring = NULL;
        } else {
            if (mstate->thread_data_ring == mdata)
                mstate->thread_data_ring = mdata->ring_next;
            mdata->ring_prev->ring_next = mdata->ring_next;
            mdata->ring_next->ring_prev = mdata->ring_prev;
        }
        mdata->ring_prev = NULL;
        mdata->ring_next = NULL;
    }
    PyThread_release_lock(mstate->thread_data_lock);
}

static void pytealet_untrack_wrapper(PyTealetObject *wrapper) {
    PyTealetMainData *mdata;
    assert(wrapper);
    if (!wrapper->tracking_ref)
        return;

    /* if the tealet has been deleted, we can't get at the correct main
     * data, so we just let it go
     */
    if (wrapper->tealet) {
        mdata = (PyTealetMainData *)*tealet_main_userpointer(wrapper->tealet->main);
        assert(mdata && mdata->wrappers);
        if (PySet_Discard(mdata->wrappers, wrapper->tracking_ref) < 0) {
            PyErr_WriteUnraisable(Py_None);
            PyErr_Clear();
        }
    }
    Py_CLEAR(wrapper->tracking_ref);
}

/* Resolve a weakref to a strong reference when alive.
 * Returns: 1 if alive (*obj_out is new ref), 0 if dead (*obj_out is NULL),
 * -1 on API error.
 */
static int pytealet_weakref_get_live(PyObject *wref, PyObject **obj_out) {
    *obj_out = NULL;
#if defined(PY313P)
    return PyWeakref_GetRef(wref, obj_out);
#else
    {
        PyObject *obj = PyWeakref_GetObject(wref);
        if (!obj || obj == Py_None)
            return 0;
        *obj_out = Py_NewRef(obj);
        return 1;
    }
#endif
}

/* get the far pointer that we need at least ot store any stack based data
 * currently in the python tstate.  this varies by python version
 */

static void *PyTealet_GetStackFar(const PyThreadState *py_tstate) {
#if defined(PY_HAS_TSTATE_CFRAME) && !defined(Py311P)
    /* Python 3.10 keeps cframe on the stack; ensure saved stack range
     * includes that structure.
     */
    if (py_tstate->cframe)
        return tealet_stack_further(&py_tstate->cframe[0], &py_tstate->cframe[1]);
#else
    /* Py311P we have our own stack local object pointed to by tstate->cframe
     * so we don't need to take it into account.
     */
    (void)py_tstate;
#endif
    return NULL;
}

/* ===================================================================== */
/* Python Tealet Type API (Methods and Accessors)                        */
/* ===================================================================== */

static PyObject *pytealet_new_impl(PyTypeObject *subtype, PyObject *args, PyObject *kwds, int creating_main) {
    PyTealetObject *src = NULL;
    PyTealetObject *result;
    PyTealetMainData *lineage_mdata;
    PyTealetModuleState *mstate = GetModuleStateFromClass(subtype);
    unsigned long current_tid;
    if (!mstate)
        return NULL;

    /* Every non-main tealet object is bound to an existing thread-main. */
    if (!creating_main) {
        if (!GetMain(mstate, 1, NULL))
            return NULL;
    }
    current_tid = PyThread_get_thread_ident();

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
    result = (PyTealetObject *)PyType_GenericAlloc(subtype, 0);
    if (!result)
        return NULL;
    result->state = STATE_NEW;
    result->tealet = NULL;
    result->owner_tid = current_tid;
    result->tracking_ref = NULL;
    PyTealetTstate_Init(&result->tstate);
    PyTealetFrameInfo_Init(&result->frame_info);
#if !defined(Py312P)
    result->weakreflist = NULL;
#endif

    if (src) {
        /* we can pass STUB and NEW tealets in, in both cases the
         * result belongs to the same thread as the original.
         */
        if (src->state == STATE_STUB) {
            /* duplicate the stub tealet and the tstate */
            result->tealet = tealet_duplicate(src->tealet);
            if (!result->tealet) {
                Py_DECREF(result);
                return PyErr_NoMemory();
            }
            TEALET_SET_PYOBJECT(result->tealet, result);
            lineage_mdata = (PyTealetMainData *)*tealet_main_userpointer(result->tealet->main);
            if (pytealet_track_wrapper(lineage_mdata, result) < 0) {
                TEALET_SET_PYOBJECT(result->tealet, NULL);
                tealet_delete(result->tealet);
                result->tealet = NULL;
                Py_DECREF(result);
                return NULL;
            }
            PyTealetTstate_Duplicate(&result->tstate, &src->tstate);
            /* We don't capture frame info for stubs. */
        }
        result->state = src->state;
        result->owner_tid = src->owner_tid;
    }
    return (PyObject *)result;
}

static PyObject *pytealet_new(PyTypeObject *subtype, PyObject *args, PyObject *kwds) {
    return pytealet_new_impl(subtype, args, kwds, 0);
}

static void pytealet_dealloc(PyObject *obj) {
    PyTealetObject *tealet = (PyTealetObject *)obj;
    /* warn if we have an active tealet that is not a stub */
    if (tealet->tealet && tealet_status(tealet->tealet) == TEALET_STATUS_ACTIVE && tealet->state != STATE_STUB) {
        int err = PyErr_WarnEx(PyExc_RuntimeWarning, "freeing an active tealet leaks memory", 1);
        if (err) {
            PyErr_WriteUnraisable(Py_None);
        }
    }
    pytealet_untrack_wrapper(tealet);
    PyObject_ClearWeakRefs(obj);
    /* Release any owned saved thread-state references */
    PyTealetTstate_Drop(&tealet->tstate, NULL);
    PyTealetFrameInfo_Release(&tealet->frame_info, NULL);
    PyTealetFrameInfo_Fini(&tealet->frame_info);
    if (tealet->tealet)
        tealet_delete(tealet->tealet);
    Py_TYPE(obj)->tp_free(obj);
}

/* Thread policy:
 * - duplicate/new and deallocation are allowed cross-thread.
 * - volatile traversal/control APIs enforce owner-thread affinity.
 */
static int pytealet_require_owner_thread(PyTealetModuleState *mstate, PyTealetObject *tealet, const char *api) {
    if (tealet->owner_tid == PyThread_get_thread_ident())
        return 0;
    PyErr_Format(mstate->invalid_error, "cannot call %s() from a different thread", api);
    return -1;
}

static PyObject *pytealet_stub(PyObject *self, PyTypeObject *defining_class, PyObject *const *args, Py_ssize_t nargs,
                               PyObject *kwnames) {
    PyTealetObject *main, *pytealet = (PyTealetObject *)self;
    PyTealetModuleState *mstate = (PyTealetModuleState *)PyType_GetModuleState(defining_class);
    tealet_t *tresult;
    PyThreadState *tstate = PyThreadState_GET();
    void *stack_far;
    PyTealetMainData *mdata;

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
    if (pytealet_require_owner_thread(mstate, pytealet, "stub"))
        return NULL;
    assert(pytealet->tealet == NULL);
    main = GetMain(mstate, 1, &mdata);
    if (!main)
        return NULL;
    stack_far = PyTealet_GetStackFar(PyThreadState_GET());
    if (tealet_stub_new(main->tealet, &tresult, stack_far))
        return PyErr_NoMemory();
    PyTealetTstate_Copy(&pytealet->tstate, tstate, 1); /* dst (new) belongs to the new tealet */
    pytealet->tealet = tresult;
    pytealet->state = STATE_STUB;
    TEALET_SET_PYOBJECT(tresult, pytealet);
    if (pytealet_track_wrapper(mdata, pytealet) < 0) {
        TEALET_SET_PYOBJECT(tresult, NULL);
        tealet_delete(tresult);
        pytealet->tealet = NULL;
        pytealet->state = STATE_NEW;
        PyTealetTstate_Drop(&pytealet->tstate, NULL);
        return NULL;
    }
    return Py_NewRef(self);
}

/* return the current tealet for this tealet lineage */
static PyObject *pytealet_current(PyObject *self, PyTypeObject *defining_class, PyObject *const *args, Py_ssize_t nargs,
                                  PyObject *kwnames) {
    PyTealetModuleState *mstate = (PyTealetModuleState *)PyType_GetModuleState(defining_class);
    PyTealetObject *current;
    PyTealetObject *base = (PyTealetObject *)self;
    if (!mstate)
        return NULL;
    if (nargs != 0 || (kwnames && PyTuple_GET_SIZE(kwnames) > 0)) {
        PyErr_SetString(PyExc_TypeError, "current() takes no arguments");
        return NULL;
    }
    if (pytealet_require_owner_thread(mstate, base, "current"))
        return NULL;
    if (!base->tealet) {
        PyErr_SetString(mstate->state_error, "must be active");
        return NULL;
    }
    current = GetCurrent(mstate, base, 0, NULL);
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
    if (pytealet_require_owner_thread(mstate, base, "previous"))
        return NULL;

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
    if (pytealet_require_owner_thread(mstate, base, "main"))
        return NULL;
    if (!base->tealet) {
        PyErr_SetString(mstate->state_error, "must be active");
        return NULL;
    } else {
        return GetWrapperRef(base->tealet->main);
    }
}

static PyObject *pytealet_belongs_to_current(PyObject *self, PyTypeObject *defining_class, PyObject *const *args,
                                             Py_ssize_t nargs, PyObject *kwnames) {
    PyTealetObject *base = (PyTealetObject *)self;
    (void)defining_class;
    if (nargs != 0 || (kwnames && PyTuple_GET_SIZE(kwnames) > 0)) {
        PyErr_SetString(PyExc_TypeError, "belongs_to_current() takes no arguments");
        return NULL;
    }
    return PyBool_FromLong(base->owner_tid == PyThread_get_thread_ident());
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
    int frame_introspection_enabled;
    PyTealetMainData *mdata;
    PyTealetNewArg *ptarg;
    void *switch_arg;

    if (!mstate)
        return NULL;

    current = GetCurrent(mstate, NULL, 0, &mdata);
    if (!current && PyErr_Occurred())
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
    ptarg = &mdata->new_arg;
    switch_arg = (void *)ptarg;

    ptarg->dest = target;
    ptarg->mstate = mstate;
    ptarg->func = func;
    ptarg->arg = farg;

    frame_introspection_enabled = (mstate->frame_introspection_enabled != 0);
    if (frame_introspection_enabled)
        PyTealetFrameInfo_Capture(&current->frame_info, 1);
    if (!created_from_new) {
        PyTealetTstate_Save(&current->tstate, tstate);
        fail = tealet_stub_run(target->tealet, pytealet_main, &switch_arg);
        PyTealetTstate_Restore(&current->tstate, tstate);
    } else {
        void *stack_limit = PyTealet_GetStackFar(tstate);
        PyTealetTstate_Copy(&current->tstate, tstate, 0); /* src (current) belongs to new tealet */
        tealet = tealet_new(current->tealet);
        if (!tealet)
            fail = TEALET_ERR_MEM;
        else
            fail = tealet_run(tealet, pytealet_main, &switch_arg, stack_limit, TEALET_START_SWITCH);
        if (fail && fail != TEALET_ERR_PANIC) {
            PyTealetTstate_UndoCopy(&current->tstate, tstate, 0);
            if (tealet)
                tealet_delete(tealet);
        } else {
            PyTealetTstate_Restore(&current->tstate, tstate);
        }
    }
    if (frame_introspection_enabled)
        PyTealetFrameInfo_Release(&current->frame_info, NULL);
    if (fail) {
        PyTealet_TranslateTealetError(mstate, fail, "tealet run failed",
                                      fail == TEALET_ERR_PANIC ? (PyObject *)switch_arg : NULL);
        result = NULL;
    } else {
        result = (PyObject *)switch_arg;
    }
    dustbin_clear(current->tealet);
    return result;
}

/* switch to a different tealet */
static PyObject *pytealet_switch(PyObject *self, PyTypeObject *defining_class, PyObject *const *args, Py_ssize_t nargs,
                                 PyObject *kwnames) {
    PyTealetModuleState *mstate = (PyTealetModuleState *)PyType_GetModuleState(defining_class);
    PyTealetObject *target = (PyTealetObject *)self;
    PyTealetObject *current;
    int switch_flags = TEALET_XFER_DEFAULT;
    int panic_enabled;
    int fail;
    PyThreadState *tstate = PyThreadState_GET();
    PyObject *pyarg = Py_None;
    PyObject *panic_obj = NULL;
    void *switch_arg;
    PyObject *result;
    int frame_introspection_enabled;
    Py_ssize_t i;
    if (!mstate)
        return NULL;

    if (nargs > 1) {
        PyErr_Format(PyExc_TypeError, "switch() takes at most 1 argument (%zd given)", nargs);
        return NULL;
    }
    if (nargs == 1)
        pyarg = args[0];

    if (kwnames && PyTuple_GET_SIZE(kwnames) > 0) {
        for (i = 0; i < PyTuple_GET_SIZE(kwnames); i++) {
            PyObject *key = PyTuple_GET_ITEM(kwnames, i);
            PyObject *val = args[nargs + i];
            if (!PyUnicode_Check(key)) {
                PyErr_SetString(PyExc_TypeError, "switch() keyword names must be strings");
                return NULL;
            }
            if (PyUnicode_CompareWithASCIIString(key, "panic") == 0) {
                if (panic_obj != NULL) {
                    PyErr_SetString(PyExc_TypeError, "switch() got multiple values for argument 'panic'");
                    return NULL;
                }
                panic_obj = val;
            } else {
                PyErr_Format(PyExc_TypeError, "switch() got an unexpected keyword argument '%U'", key);
                return NULL;
            }
        }
    }
    if (panic_obj != NULL) {
        panic_enabled = PyObject_IsTrue(panic_obj);
        if (panic_enabled < 0)
            return NULL;
        if (panic_enabled)
            switch_flags |= TEALET_XFER_PANIC;
    }

    if (target->state != STATE_RUN) {
        PyErr_SetString(mstate->state_error, "must be active");
        return NULL;
    }
    assert(target->tealet);
    /* we don't have a source tealet, so we must get it from the thread state. */
    current = GetCurrent(mstate, NULL, 0, NULL);
    if (!current && PyErr_Occurred())
        return NULL;
    if (CheckTarget(mstate, target, current))
        return NULL;

    Py_INCREF(pyarg);
    switch_arg = (void *)pyarg;
    frame_introspection_enabled = (mstate->frame_introspection_enabled != 0);
    if (frame_introspection_enabled)
        PyTealetFrameInfo_Capture(&current->frame_info, 1);
    PyTealetTstate_Save(&current->tstate, tstate);
    fail = tealet_switch(target->tealet, &switch_arg, switch_flags);
    PyTealetTstate_Restore(&current->tstate, tstate);
    if (frame_introspection_enabled)
        PyTealetFrameInfo_Release(&current->frame_info, NULL);

    dustbin_clear(current->tealet);

    if (fail) {
        if (fail != TEALET_ERR_PANIC) {
            Py_DECREF(pyarg);
            switch_arg = NULL; /* non-panic errors don't return a value */
        }
        PyTealet_TranslateTealetError(mstate, fail, "tealet switch failed", (PyObject *)switch_arg);
        return NULL;
    }
    result = (PyObject *)switch_arg;
    return result;
}

static struct PyMethodDef pytealet_methods[] = {
    {"stub", (PyCFunction)(void (*)(void))pytealet_stub, METH_METHOD | METH_FASTCALL | METH_KEYWORDS, ""},
    {"current", (PyCFunction)(void (*)(void))pytealet_current, METH_METHOD | METH_FASTCALL | METH_KEYWORDS, ""},
    {"previous", (PyCFunction)(void (*)(void))pytealet_previous, METH_METHOD | METH_FASTCALL | METH_KEYWORDS, ""},
    {"main", (PyCFunction)(void (*)(void))pytealet_main_method, METH_METHOD | METH_FASTCALL | METH_KEYWORDS, ""},
    {"belongs_to_current", (PyCFunction)(void (*)(void))pytealet_belongs_to_current,
     METH_METHOD | METH_FASTCALL | METH_KEYWORDS, ""},
    {"run", (PyCFunction)(void (*)(void))pytealet_run, METH_METHOD | METH_FASTCALL | METH_KEYWORDS, ""},
    {"switch", (PyCFunction)(void (*)(void))pytealet_switch, METH_METHOD | METH_FASTCALL | METH_KEYWORDS, ""},
    {NULL, NULL} /* sentinel */
};

/* ===================================================================== */
/* Properties                                                            */
/* ===================================================================== */
static PyObject *pytealet_get_main(PyObject *_self, void *_closure) {
    PyTealetObject *self = (PyTealetObject *)_self;
    PyTealetModuleState *mstate = GetModuleStateFromClass(Py_TYPE(self));
    if (!mstate)
        return NULL;
    if (pytealet_require_owner_thread(mstate, self, "main"))
        return NULL;

    if (!self->tealet) {
        /* happens only for new tealets, not yet run.  then we have to find the current for this thread.
         * but we don't attempt to create a new one.
         */
        return Py_XNewRef((PyObject *)GetMain(mstate, 0, NULL));
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
    PyObject *frame;
    PyTealetModuleState *mstate = GetModuleStateFromClass(Py_TYPE(self));
    if (!mstate)
        return NULL;
#if defined(PY_HAS_TSTATE_FRAME)
    frame = self->tstate.has_state ? (PyObject *)self->tstate.frame_data.frame : NULL;
#else
    if (mstate->frame_introspection_enabled)
        frame = PyTealetFrameInfo_GetFrame(&self->frame_info);
    else
        frame = NULL;
#endif
    if (!frame) {
        /* is it the current tealet of the current thread? */
        if (self == GetCurrent(mstate, NULL, 0, NULL)) {
            frame = (PyObject *)PyEval_GetFrame();
        }
    }
    if (!frame)
        frame = Py_None;
    return Py_NewRef(frame);
}

static PyObject *pytealet_get_tid(PyObject *_self, void *_closure) {
    PyTealetObject *self = (PyTealetObject *)_self;
    return PyLong_FromUnsignedLong(self->owner_tid);
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

PyType_Spec pytealet_type_spec = {"_tealet.tealet", sizeof(PyTealetObject), 0,
                                  Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE
#if defined(Py312P)
                                      | Py_TPFLAGS_MANAGED_WEAKREF
#endif
                                  ,
                                  pytealet_type_slots};

/* Only needed on pre-3.12 builds where tp_weaklistoffset must be
 * set explicitly after dynamic type creation.
 */
#if !defined(Py312P)
Py_ssize_t PyTealet_WeaklistOffset(void) { return (Py_ssize_t)offsetof(PyTealetObject, weakreflist); }
#endif

/* ===================================================================== */
/* Runtime Support (Allocator and Lineage)                               */
/* ===================================================================== */

/* Wrapper functions for Python memory APIs to match libtealet's allocator API.
 */
static void *tealet_malloc_wrapper(size_t size, void *context) {
    (void)context; /* unused */
    return PyMem_Malloc(size);
}

static void tealet_free_wrapper(void *ptr, void *context) {
    (void)context; /* unused */
    PyMem_Free(ptr);
}

#if PYTEALET_FREE_THREADED
static void pytealet_domain_lock_cb(void *arg) {
    PyThread_type_lock lock = (PyThread_type_lock)arg;
    PyThread_acquire_lock(lock, WAIT_LOCK);
}

static void pytealet_domain_unlock_cb(void *arg) {
    PyThread_type_lock lock = (PyThread_type_lock)arg;
    PyThread_release_lock(lock);
}

static int pytealet_configure_domain_locking(tealet_t *main_tealet, PyTealetMainData *mdata) {
    tealet_lock_t locking;
    mdata->domain_lock = PyThread_allocate_lock();
    if (!mdata->domain_lock)
        return -1;

    locking.mode = TEALET_LOCK_AUTO;
    locking.lock = pytealet_domain_lock_cb;
    locking.unlock = pytealet_domain_unlock_cb;
    locking.arg = (void *)mdata->domain_lock;
    if (tealet_configure_set_locking(main_tealet, &locking) < 0) {
        PyThread_free_lock(mdata->domain_lock);
        mdata->domain_lock = NULL;
        return -1;
    }
    return 0;
}

static void pytealet_free_domain_lock(PyTealetMainData *mdata) {
    if (mdata && mdata->domain_lock) {
        PyThread_free_lock(mdata->domain_lock);
        mdata->domain_lock = NULL;
    }
}
#else
static int pytealet_configure_domain_locking(tealet_t *main_tealet, PyTealetMainData *mdata) {
    (void)main_tealet;
    (void)mdata;
    return 0;
}

static void pytealet_free_domain_lock(PyTealetMainData *mdata) { (void)mdata; }
#endif

/* return a borrowed reference to this thread's main tealet */
PyTealetObject *GetMain(PyTealetModuleState *mstate, int create, PyTealetMainData **mdata_out) {
    /* Get the thread's main tealet */
    PyTealetMainData *mdata;
    PyTealetObject *t_main;
    if (!mstate)
        return NULL;
    mdata = (PyTealetMainData *)PyThread_tss_get(&mstate->tls_key);
    if (!mdata && !create) {
        return NULL;
    }

    /* main tealet doesn't exist yet.  create it. */
    if (!mdata) {
        tealet_alloc_t talloc;
        tealet_t *tmain;
        /* Use PyMem allocators for libtealet heap allocations. */
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
        mdata->wrappers = PySet_New(NULL);
        if (!mdata->wrappers) {
            Py_DECREF(mdata->dustbin);
            tealet_finalize(tmain);
            PyMem_Free(mdata);
            PyErr_NoMemory();
            return NULL;
        }
        if (PyList_SetSlice(mdata->dustbin, 0, DUSTBIN_PREALLOC, NULL) < 0) {
            Py_DECREF(mdata->dustbin);
            Py_DECREF(mdata->wrappers);
            tealet_finalize(tmain);
            PyMem_Free(mdata);
            return NULL;
        }
        if (pytealet_configure_domain_locking(tmain, mdata) < 0) {
            Py_DECREF(mdata->dustbin);
            Py_DECREF(mdata->wrappers);
            tealet_finalize(tmain);
            pytealet_free_domain_lock(mdata);
            PyMem_Free(mdata);
            PyErr_SetString(PyExc_RuntimeError, "failed to configure tealet domain lock callbacks");
            return NULL;
        }
        *tealet_main_userpointer(tmain) = (void *)mdata;

        /* create the main tealet */
        t_main = (PyTealetObject *)pytealet_new_impl(mstate->tealet_type, NULL, NULL, 1);
        if (!t_main) {
            Py_DECREF(mdata->dustbin);
            Py_DECREF(mdata->wrappers);
            tealet_finalize(tmain);
            pytealet_free_domain_lock(mdata);
            PyMem_Free(mdata);
            return NULL;
        }
        t_main->tealet = tmain;
        t_main->state = STATE_RUN;
        TEALET_SET_PYOBJECT(tmain, t_main); /* back link */
        mdata->main_wrapper = (PyObject *)t_main;
        if (PyThread_tss_set(&mstate->tls_key, (void *)mdata) != 0) {
            TEALET_SET_PYOBJECT(tmain, NULL);
            t_main->tealet = NULL;
            Py_CLEAR(mdata->main_wrapper);
            Py_DECREF(mdata->dustbin);
            Py_DECREF(mdata->wrappers);
            tealet_finalize(tmain);
            pytealet_free_domain_lock(mdata);
            PyMem_Free(mdata);
            PyErr_SetString(PyExc_RuntimeError, "failed to set thread-local main tealet");
            return NULL;
        }
        if (pytealet_link_thread_data(mstate, mdata) < 0) {
            TEALET_SET_PYOBJECT(tmain, NULL);
            t_main->tealet = NULL;
            t_main->state = STATE_EXIT;
            Py_CLEAR(mdata->main_wrapper);
            PyThread_tss_set(&mstate->tls_key, NULL);
            Py_DECREF(mdata->dustbin);
            Py_DECREF(mdata->wrappers);
            tealet_finalize(tmain);
            pytealet_free_domain_lock(mdata);
            PyMem_Free(mdata);
            PyErr_SetString(PyExc_RuntimeError, "failed to register thread main data");
            return NULL;
        }
    } else {
        t_main = (PyTealetObject *)mdata->main_wrapper;
    }
    assert(t_main);
    assert(t_main->tealet);
    assert(TEALET_IS_MAIN(t_main->tealet));
    assert(t_main->state == STATE_RUN);
    if (mdata_out)
        *mdata_out = mdata;
    return t_main;
}

/* return a borrowed ref to this threads current tealet */
PyTealetObject *GetCurrent(PyTealetModuleState *mstate, PyTealetObject *pytealet, int create_main,
                           PyTealetMainData **mdata_out) {
    tealet_t *t_current;
    /* if we are being passed no tealet, or it is a new tealet,
     * we must get the current main from the thread-local storage */
    if (!pytealet || !pytealet->tealet) {
        pytealet = GetMain(mstate, create_main, mdata_out);
    }

    if (!pytealet)
        return NULL;
    if (mdata_out)
        *mdata_out = (PyTealetMainData *)*tealet_main_userpointer(pytealet->tealet->main);
    t_current = tealet_current(pytealet->tealet);
    if (t_current != pytealet->tealet) {
        return TEALET_PYOBJECT(t_current);
    }
    return pytealet;
}

/* Explicitly clean up this thread's tealet lineage and return wrappers whose
 * native tealet handles in the active state and forcibly invalidated.
 */
PyObject *PyTealet_ThreadCleanup(PyTealetModuleState *mstate) {
    PyTealetObject *current;
    PyTealetMainData *mdata;
    PyObject *nerfed;
    PyObject *wrappers = NULL;
    PyObject *iter = NULL;
    PyObject *wref;
    tealet_t *main_tealet;

    assert(mstate);
    nerfed = PyList_New(0);
    if (!nerfed)
        return NULL;

    current = GetCurrent(mstate, NULL, 0, &mdata);
    if (!current) {
        /* no current tealet , idempotent result (cleanup non-existing)*/
        return nerfed;
    }
    assert(mdata);
    if (!TEALET_IS_MAIN(current->tealet)) {
        Py_DECREF(nerfed);
        PyErr_SetString(mstate->state_error, "thread_cleanup() must be called from this thread's main tealet");
        return NULL;
    }
    main_tealet = current->tealet;

    wrappers = mdata->wrappers;
    assert(wrappers);

    iter = PyObject_GetIter(wrappers);
    if (!iter) {
        Py_DECREF(nerfed);
        return NULL;
    }

    while (iter && (wref = PyIter_Next(iter))) {
        PyObject *obj = NULL;
        PyTealetObject *wrapper;
        int weak_status;

        weak_status = pytealet_weakref_get_live(wref, &obj);
        Py_DECREF(wref);
        if (weak_status < 0) {
            Py_DECREF(iter);
            Py_DECREF(nerfed);
            return NULL;
        }
        if (weak_status == 0)
            continue;
        assert(PyTealet_Check(obj, mstate));
        wrapper = (PyTealetObject *)obj;
        if (!wrapper->tealet) {
            Py_DECREF(obj);
            continue;
        }
        /* ignore any main tealet, will be handled separately */
        if (TEALET_IS_MAIN(wrapper->tealet)) {
            Py_DECREF(obj);
            continue;
        }
        /* only add live tealets to the list*/
        int add_to_list =
            (tealet_status(wrapper->tealet) == TEALET_STATUS_ACTIVE);
        /* but stubs are okay to delete and don't leak memory */
        if (wrapper->state == STATE_STUB)
            add_to_list = 0;

        if (add_to_list) {
            if (PyList_Append(nerfed, obj) < 0) {
                PyErr_WriteUnraisable(Py_None);
                PyErr_Clear();
            }
        }

        /* deallocate the tealet handle.  if it was active, this destroys
         * the saved stack.
         */
        tealet_delete(wrapper->tealet);
        wrapper->tealet = NULL;
        wrapper->state = STATE_EXIT;
        Py_CLEAR(wrapper->tracking_ref);
        Py_DECREF(obj);
    }
    if (iter) {
        Py_DECREF(iter);
        if (PyErr_Occurred()) {
            /* errors during iter-next */
            Py_DECREF(nerfed);
            return NULL;
        }
    }

    /* clear main tealet, and destroy the linage */
    TEALET_SET_PYOBJECT(((PyTealetObject *)mdata->main_wrapper)->tealet, NULL);
    ((PyTealetObject *)mdata->main_wrapper)->tealet = NULL;
    ((PyTealetObject *)mdata->main_wrapper)->state = STATE_EXIT;
    Py_CLEAR(mdata->main_wrapper);
    tealet_finalize(main_tealet);

    Py_CLEAR(mdata->dustbin);
    Py_CLEAR(mdata->wrappers);
    pytealet_unlink_thread_data(mstate, mdata);
    pytealet_free_domain_lock(mdata);
    PyMem_Free(mdata);

    if (PyThread_tss_set(&mstate->tls_key, NULL) != 0) {
        PyErr_WriteUnraisable(Py_None);
        PyErr_Clear();
    }

    return nerfed;
}

/* check if a target tealet is valid, compared to a reference one.
 * we primarily use the thread_ids stored on the objects but
 * also assert the main line relationship
 */
static int CheckTarget(PyTealetModuleState *mstate, PyTealetObject *target, PyTealetObject *ref) {
    if (!ref)
        goto mismatch;

    if (target->owner_tid != ref->owner_tid)
        goto mismatch;

    if (ref->tealet && target->tealet) {
        /* assert main lineage relationship */
        assert(ref->tealet->main == target->tealet->main);
    }
    return 0;

mismatch:
    if (ref && ref->tealet && target->tealet) {
        assert(ref->tealet->main != target->tealet->main);
    }
    PyErr_SetString(mstate->invalid_error, "thread mismatch: cannot switch to a tealet from a different thread");
    return -1;
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
    PyTealetMainData *mdata;
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
        mdata = (PyTealetMainData *)*tealet_main_userpointer(t_current->main);
        if (pytealet_track_wrapper(mdata, tealet) < 0) {
            PyErr_WriteUnraisable(Py_None);
            PyErr_Clear();
        }
    }

    /* We only have borrowed references from the calling tealet.
     * the argument to the function will get their own reference, but
     * anything we need after the function we keep our own references
     * for, because when the function returns, the calling tealet
     * may have exited and dropped the references we borrowed.
     */
    Py_INCREF(func);
    Py_INCREF(tealet);

    /* The tealet now has its own private Thread state and we can modify safely.
     * initialize local python frame bookkeeping and memory arena
     */
    PyTealetTstate_Frame_Setup(&tealet->tstate, tstate);

    /* run the tealet function */
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
        } else if (CheckTarget(mstate, return_to, tealet)) {
            return_to = NULL;
        }
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
        return_to = GetMain(mstate, 0, NULL);
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
        exit_mode = TEALET_XFER_DEFAULT;
    if (exit_mode == TEALET_EXIT_DELETE) {
        tealet->tealet = NULL; /* will be auto-deleted on return */
        TEALET_SET_PYOBJECT(t_current, NULL);
    }
    t_return = return_to->tealet;

    /* decref the objects after the switch */
    PyTealet_dustbin_push(t_return, func);
    PyTealet_dustbin_push(t_return, (PyObject *)tealet);
    PyTealet_dustbin_push(t_return, result);

    Py_INCREF(return_arg);

    /* Tealet is exiting permanently: clear active PyThreadState for the switch,
     * then drop saved refs immediately so frame locals (including 'current')
     * do not keep the Python tealet object alive until GC.
     */
    PyTealetTstate_Frame_Cleanup(tstate, t_return);
    PyTealetTstate_Save(&tealet->tstate, tstate);
    PyTealetTstate_Drop(&tealet->tstate, t_return);

    {
        int exit_fail;
        exit_fail = tealet_exit(t_return, (void *)return_arg, exit_mode | TEALET_XFER_NOFAIL);
        if (exit_fail) {
            PyTealet_TranslateTealetError(mstate, exit_fail, "tealet exit failed", NULL);
            PyErr_WriteUnraisable(func);
            abort();
        }
    }
    /* never reach here */
    return 0;
}

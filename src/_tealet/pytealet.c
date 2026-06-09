
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
#include <stdlib.h>
#include <string.h>

#include "pytealet.h"
#include "pytealet_lineage.h"
#include "pytealet_runtime.h"
#include "pytealet_throw.h"
#include "tealet.h"
#include "tealet_extras.h"

#if defined(Py311P)
#include "cpython/context.h"
#else
#include "context.h"
#endif

/* ===================================================================== */
/* Core Types and Module State                                           */
/* ===================================================================== */

/* initial number of slots in dustbin, to avoid realloc on push */
#define DUSTBIN_PREALLOC 10

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

/* helpers for getting main and current and checking relationship */
static PyTealetModuleState *GetModuleStateFromClass(PyTypeObject *cls);
PyTealetObject *TryGetMain(PyTealetModuleState *mstate, PyTealetMainData **mdata_out);
PyTealetObject *TryGetCurrent(PyTealetModuleState *mstate, PyTealetMainData **mdata_out);
static int CheckTarget(PyTealetModuleState *mstate, PyTealetObject *target, PyTealetObject *main,
                       const char *operation);
static PyObject *pytealet_new_impl(PyTypeObject *subtype, PyObject *args, PyObject *kwds, int creating_main);
static int pytealet_track_wrapper(PyTealetMainData *mdata, PyTealetObject *wrapper, int lock_held);
static void pytealet_untrack_wrapper(PyTealetObject *wrapper, int lock_held);
static void pytealet_domain_lock(PyTealetMainData *mdata);
static void pytealet_domain_unlock(PyTealetMainData *mdata);
static int pytealet_collect_active_wrappers(PyTealetModuleState *mstate, PyTealetMainData *mdata, PyObject *active_out,
                                            PyTealetObject *caller, unsigned int collect_flags);
static PyObject *pytealet_thread_kill_inner(PyTealetModuleState *mstate, PyTealetMainData *mdata,
                                            Py_ssize_t cleanup_passes, PyTealetObject *caller, PyObject *kill_exc_spec);
static int pytealet_set_exception_inner(PyTealetModuleState *mstate, PyTealetObject *target, PyTealetObject *current,
                                        PyTealetMainData *mdata, PyObject *exc, PyObject *fallback);
static PyObject *pytealet_duplicate(PyObject *self, PyTypeObject *defining_class, PyObject *const *args,
                                    Py_ssize_t nargs, PyObject *kwnames);
static PyObject *pytealet_throw(PyObject *self, PyTypeObject *defining_class, PyObject *const *args, Py_ssize_t nargs,
                                PyObject *kwnames);
static PyObject *pytealet_set_exception(PyObject *self, PyTypeObject *defining_class, PyObject *const *args,
                                        Py_ssize_t nargs, PyObject *kwnames);

enum {
    PYTEALET_COLLECT_OMIT_MAIN = 1u << 0,
    PYTEALET_COLLECT_OMIT_CALLER = 1u << 1,
};

static tealet_t *pytealet_main(tealet_t *t_current, void *arg);

/* ===================================================================== */
/* Type and Module Access Helpers                                        */
/* ===================================================================== */

static PyTealetModuleState *GetModuleStateFromClass(PyTypeObject *cls) {
#if defined(Py311P)
    PyObject *module = PyType_GetModuleByDef(cls, &_tealet_module);
    if (module) {
        PyTealetModuleState *mstate = (PyTealetModuleState *)PyModule_GetState(module);
        if (!mstate) {
            PyErr_SetString(PyExc_RuntimeError, "failed to get _tealet module state");
            return NULL;
        }
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
            if (PyErr_ExceptionMatches(PyExc_TypeError)) {
                PyErr_Clear();
                if (cur != NULL)
                    continue;
                break;
            }
            return NULL;
        }
    }
    if (!PyErr_Occurred())
        PyErr_SetString(PyExc_RuntimeError, "failed to get _tealet module state");
    return NULL;
#endif
}

static int PyTealet_Check(PyObject *op, PyTealetModuleState *mstate) {
    return mstate && mstate->tealet_type && PyObject_TypeCheck(op, mstate->tealet_type);
}

static int PyTealet_CheckExact(PyObject *op, PyTealetModuleState *mstate) {
    return mstate && mstate->tealet_type && (Py_TYPE(op) == mstate->tealet_type);
}

static int PyTealet_SetPanicErrorWithValue(PyTealetModuleState *mstate, const char *what, PyObject *value,
                                           PyObject *exception) {
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
    if (PyObject_SetAttrString(exc_obj, "_result", value) < 0) {
        Py_DECREF(exc_obj);
        return -1;
    }

    if (!exception)
        exception = Py_None;
    if (PyObject_SetAttrString(exc_obj, "_exception", exception) < 0) {
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
static int PyTealet_TranslateTealetError(PyTealetModuleState *mstate, int err, const char *what, PyObject *panic_value,
                                         PyObject *panic_exception) {
    const char *msg = what ? what : "tealet operation failed";
    if (err != TEALET_ERR_PANIC && panic_value) {
        Py_DECREF(panic_value);
        panic_value = NULL;
    }
    if (err != TEALET_ERR_PANIC && panic_exception) {
        Py_DECREF(panic_exception);
        panic_exception = NULL;
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
        int tr = PyTealet_SetPanicErrorWithValue(mstate, msg, panic_value ? panic_value : Py_None, panic_exception);
        Py_XDECREF(panic_exception);
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

static int pytealet_track_wrapper(PyTealetMainData *mdata, PyTealetObject *wrapper, int lock_held) {
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

    if (!lock_held)
        pytealet_domain_lock(mdata);
    if (PySet_Add(mdata->wrappers, wref) < 0) {
        if (!lock_held)
            pytealet_domain_unlock(mdata);
        Py_DECREF(wref);
        return -1;
    }
    if (!lock_held)
        pytealet_domain_unlock(mdata);
    wrapper->tracking_ref = wref;
    return 0;
}

/* this function is called, in two cases:
 * 1. with lock held, when on wrapper's own thread, when the run and the tealet
 *    have exited.
 * 2. without lock held, when called from the python deallocator, which can
 *    happen from a different thread.  In the case of the deallocator, no
 *    one can be messing with wrapper->tealet so accessing it is safe.
 */
static void pytealet_untrack_wrapper(PyTealetObject *wrapper, int lock_held) {
    PyTealetMainData *mdata = NULL;
    assert(wrapper);
    if (!wrapper->tracking_ref)
        return;

    /* if the tealet has been deleted, we can't get at the correct main
     * data, so we just let it go
     */
    if (wrapper->tealet) {
        int discard_rc;
        mdata = (PyTealetMainData *)*tealet_main_userpointer(wrapper->tealet->main);
        assert(mdata);
        if (!lock_held)
            pytealet_domain_lock(mdata);
        assert(mdata->wrappers);
        discard_rc = PySet_Discard(mdata->wrappers, wrapper->tracking_ref);
        if (!lock_held)
            pytealet_domain_unlock(mdata);
        if (discard_rc < 0) {
            PyErr_WriteUnraisable(Py_None);
            PyErr_Clear();
        }
    }
    /* weakrefs don't cause side effects when deleted */
    Py_CLEAR(wrapper->tracking_ref);
}

int PyTealet_ErrorWasRemote(PyTealetModuleState *mstate) {
    PyTealetMainData *mdata = NULL;

    if (!mstate)
        return 0;

    (void)TryGetMain(mstate, &mdata);
    return mdata ? (mdata->last_error_remote != 0) : 0;
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
    PyTealetObject *result;
    PyTealetMainData *lineage_mdata = NULL;
    PyTealetModuleState *mstate = GetModuleStateFromClass(subtype);
    unsigned long current_tid;
    if (!mstate)
        return NULL;

    /* Every non-main tealet object is bound to an existing thread-main. */
    if (!creating_main) {
        if (!PyTealet_GetOrCreateMain(mstate, &lineage_mdata))
            return NULL;
    }
    current_tid = PyThread_get_thread_ident();

    /* Keep exact tealet() strict, but allow subclass constructors to accept
     * custom arguments in Python __init__ without requiring __new__ override.
     */
    if (!creating_main && subtype == mstate->tealet_type &&
        ((args && PyTuple_GET_SIZE(args) > 0) || (kwds && PyDict_GET_SIZE(kwds) > 0))) {
        PyErr_SetString(PyExc_TypeError, "tealet() takes no arguments");
        return NULL;
    }

    result = (PyTealetObject *)PyType_GenericAlloc(subtype, 0);
    if (!result)
        return NULL;
    result->state = STATE_NEW;
    result->tealet = NULL;
    result->owner_tid = current_tid;
    result->domain_lock_obj = NULL;
    result->tracking_ref = NULL;
    result->prepared_func = NULL;
    result->inflight_throw_token = 0;
    PyTealetTstate_Init(&result->tstate);
    PyTealetFrameInfo_Init(&result->frame_info);
#if !defined(Py312P)
    result->weakreflist = NULL;
#endif

    if (lineage_mdata && lineage_mdata->domain_lock_obj) {
        result->domain_lock_obj = Py_NewRef(lineage_mdata->domain_lock_obj);
    }
    result->prepared_cfunc = NULL;
    return (PyObject *)result;
}

static PyObject *pytealet_duplicate_impl(PyTealetModuleState *mstate, PyTealetObject *src) {
    PyTealetObject *result;

    assert(mstate);
    assert(src);

    if (src->state != STATE_NEW && src->state != STATE_STUB) {
        PyErr_SetString(mstate->state_error, "state must be new or stub");
        return NULL;
    }

    result = (PyTealetObject *)PyType_GenericAlloc(Py_TYPE(src), 0);
    if (!result)
        return NULL;

    result->state = STATE_NEW;
    result->tealet = NULL;
    result->owner_tid = src->owner_tid;
    result->domain_lock_obj = NULL;
    result->tracking_ref = NULL;
    result->prepared_func = NULL;
    result->inflight_throw_token = 0;
    PyTealetTstate_Init(&result->tstate);
    PyTealetFrameInfo_Init(&result->frame_info);
#if !defined(Py312P)
    result->weakreflist = NULL;
#endif

    if (src->state == STATE_STUB) {
        PyTealetMainData *lineage_mdata;

        result->tealet = tealet_duplicate(src->tealet);
        if (!result->tealet) {
            Py_DECREF(result);
            return PyErr_NoMemory();
        }
        TEALET_SET_PYOBJECT(result->tealet, result);
        lineage_mdata = (PyTealetMainData *)*tealet_main_userpointer(result->tealet->main);
        if (pytealet_track_wrapper(lineage_mdata, result, 0) < 0) {
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
    if (src->domain_lock_obj)
        result->domain_lock_obj = Py_NewRef(src->domain_lock_obj);
    if (src->prepared_func)
        result->prepared_func = Py_NewRef(src->prepared_func);
    result->prepared_cfunc = src->prepared_cfunc;

    return (PyObject *)result;
}

static PyObject *pytealet_duplicate(PyObject *self, PyTypeObject *defining_class, PyObject *const *args,
                                    Py_ssize_t nargs, PyObject *kwnames) {
    PyTealetObject *src = (PyTealetObject *)self;
    PyTealetModuleState *mstate = GetModuleStateFromClass(defining_class);

    if (!mstate)
        return NULL;
    if (nargs != 0 || (kwnames && PyTuple_GET_SIZE(kwnames) > 0)) {
        PyErr_SetString(PyExc_TypeError, "duplicate() takes no arguments");
        return NULL;
    }

    return pytealet_duplicate_impl(mstate, src);
}

/* Duplication entrypoint for external C clients via the _tealet capsule API.
 * Equivalent to source.duplicate().
 */
PyObject *PyTealetApi_Duplicate(PyTealetModuleState *mstate, PyObject *source_obj) {
    if (!mstate || !mstate->tealet_type) {
        PyErr_SetString(PyExc_RuntimeError, "_tealet module state unavailable");
        return NULL;
    }
    if (!source_obj) {
        PyErr_SetString(PyExc_TypeError, "source must not be NULL");
        return NULL;
    }
    if (!PyObject_TypeCheck(source_obj, mstate->tealet_type)) {
        PyErr_SetString(PyExc_TypeError, "source must be a _tealet.tealet instance");
        return NULL;
    }

    return pytealet_duplicate_impl(mstate, (PyTealetObject *)source_obj);
}

static PyObject *pytealet_new(PyTypeObject *subtype, PyObject *args, PyObject *kwds) {
    return pytealet_new_impl(subtype, args, kwds, 0);
}

/* Creation entrypoint for external C clients via the _tealet capsule API.
 * Equivalent to _tealet.tealet().
 */
PyObject *PyTealetApi_Create(PyTealetModuleState *mstate) {
    if (!mstate || !mstate->tealet_type) {
        PyErr_SetString(PyExc_RuntimeError, "_tealet module state unavailable");
        return NULL;
    }
    return PyObject_CallNoArgs((PyObject *)mstate->tealet_type);
}

static int pytealet_traverse(PyObject *obj, visitproc visit, void *arg) {
    PyTealetObject *tealet = (PyTealetObject *)obj;
    int visit_rc;

    Py_VISIT(tealet->domain_lock_obj);
    Py_VISIT(tealet->tracking_ref);
    Py_VISIT(tealet->prepared_func);

    visit_rc = PyTealetTstate_Visit(&tealet->tstate, visit, arg);
    if (visit_rc)
        return visit_rc;
    visit_rc = PyTealetFrameInfo_Visit(&tealet->frame_info, visit, arg);
    if (visit_rc)
        return visit_rc;
    return 0;
}

static int pytealet_clear(PyObject *obj) {
    /* tracking-ref is cleared in the dealloc it isn't part of the GC cycle */
    PyTealetObject *tealet = (PyTealetObject *)obj;
    Py_CLEAR(tealet->prepared_func);
    tealet->prepared_cfunc = NULL;
    PyTealetTstate_Drop(&tealet->tstate, NULL, 1);
    PyTealetFrameInfo_Release(&tealet->frame_info, NULL);
    return 0;
}

static void pytealet_dealloc(PyObject *obj) {
    PyTealetObject *tealet = (PyTealetObject *)obj;
    PyObject_GC_UnTrack(obj);
    /* warn if we have an active tealet that is not a stub */
    if (tealet->tealet && tealet_status(tealet->tealet) == TEALET_STATUS_ACTIVE && tealet->state != STATE_STUB) {
        int err = PyErr_WarnEx(PyExc_RuntimeWarning, "freeing an active tealet leaks memory", 1);
        if (err) {
            PyErr_WriteUnraisable(Py_None);
        }
    }
    pytealet_untrack_wrapper(tealet, 0);
    tealet->inflight_throw_token = 0;
    PyObject_ClearWeakRefs(obj);
    /* Release GC-managed references. */
    (void)pytealet_clear(obj);
    PyTealetFrameInfo_Fini(&tealet->frame_info);
    if (tealet->tealet)
        tealet_delete(tealet->tealet);
    Py_CLEAR(tealet->domain_lock_obj);
    Py_TYPE(obj)->tp_free(obj);
}

/* Thread policy:
 * - duplicate/new and deallocation are allowed cross-thread.
 * - volatile traversal/control APIs enforce owner-thread affinity.
 */
static int pytealet_require_owner_thread(PyTealetModuleState *mstate, PyTealetObject *tealet, const char *api) {
    PyTealetObject *main = TryGetMain(mstate, NULL);
    if (CheckTarget(mstate, tealet, main, api))
        return -1;
    return 0;
}

static int pytealet_stub_impl(PyTealetModuleState *mstate, PyTealetObject *pytealet, const char *operation) {
    PyTealetObject *main;
    tealet_t *tresult = NULL;
    PyThreadState *tstate = PyThreadState_GET();
    void *stack_far;
    PyTealetMainData *mdata;
    int tealet_attached = 0;
    int ok = 0;

    assert(mstate);
    assert(pytealet);

    main = PyTealet_GetOrCreateMain(mstate, &mdata);
    if (!main)
        return -1;
    if (CheckTarget(mstate, pytealet, main, operation))
        return -1;

    if (pytealet->state != STATE_NEW) {
        PyErr_SetString(mstate->state_error, "must be new");
        return -1;
    }
    assert(pytealet->tealet == NULL);

    stack_far = PyTealet_GetStackFar(PyThreadState_GET());
    if (tealet_stub_new(main->tealet, &tresult, stack_far)) {
        PyErr_NoMemory();
        goto out;
    }

    /* Copy the tstate, but leave the currently set "context" intact */
    PyTealetTstate_Copy(&pytealet->tstate, tstate, 1, 0); /* dst (new) belongs to the new tealet */
    /* Clear saved frame-like slots now; stub restore should not re-run frame setup. */
    PyTealetTstate_Frame_Setup(&pytealet->tstate, tstate, 0);

    pytealet->tealet = tresult;
    pytealet->state = STATE_STUB;
    TEALET_SET_PYOBJECT(tresult, pytealet);
    tealet_attached = 1;

    pytealet_domain_lock(mdata);
    if (pytealet_track_wrapper(mdata, pytealet, 1) < 0)
        goto out;

    ok = 1;

out:
    if (!ok && tresult) {
        if (tealet_attached) {
            TEALET_SET_PYOBJECT(tresult, NULL);
            pytealet->tealet = NULL;
            pytealet->state = STATE_NEW;
            PyTealetTstate_Drop(&pytealet->tstate, NULL, 1);
        }
    }
    pytealet_domain_unlock(mdata);
    if (!ok && tresult)
        tealet_delete(tresult);
    return ok ? 0 : -1;
}

static PyObject *pytealet_stub(PyObject *self, PyTypeObject *defining_class, PyObject *const *args, Py_ssize_t nargs,
                               PyObject *kwnames) {
    PyTealetObject *pytealet = (PyTealetObject *)self;
    PyTealetModuleState *mstate = GetModuleStateFromClass(defining_class);

    if (!mstate)
        return NULL;
    if (nargs != 0 || (kwnames && PyTuple_GET_SIZE(kwnames) > 0)) {
        PyErr_SetString(PyExc_TypeError, "stub() takes no arguments");
        return NULL;
    }

    if (PyTealetApi_Stub(mstate, self) < 0)
        return NULL;
    return Py_NewRef((PyObject *)pytealet);
}

/* Stub-creation entrypoint for external C clients via the _tealet capsule API.
 * Equivalent to target.stub().
 */
int PyTealetApi_Stub(PyTealetModuleState *mstate, PyObject *target_obj) {
    PyTealetObject *target;

    if (!mstate || !mstate->tealet_type) {
        PyErr_SetString(PyExc_RuntimeError, "_tealet module state unavailable");
        return -1;
    }
    if (!target_obj) {
        PyErr_SetString(PyExc_TypeError, "target must not be NULL");
        return -1;
    }
    if (!PyObject_TypeCheck(target_obj, mstate->tealet_type)) {
        PyErr_SetString(PyExc_TypeError, "target must be a _tealet.tealet instance");
        return -1;
    }

    target = (PyTealetObject *)target_obj;
    return pytealet_stub_impl(mstate, target, "stub");
}

/* return the current tealet for this tealet lineage.
 * we require it to be called from the owning thread.
 * if we wish to relax this, we could acquire the domain lock before getting the wrapper
 */
static PyObject *pytealet_current(PyObject *self, PyTypeObject *defining_class, PyObject *const *args, Py_ssize_t nargs,
                                  PyObject *kwnames) {
    PyTealetModuleState *mstate = GetModuleStateFromClass(defining_class);
    PyTealetObject *current;
    PyTealetObject *base = (PyTealetObject *)self;
    if (!mstate)
        return NULL;
    if (nargs != 0 || (kwnames && PyTuple_GET_SIZE(kwnames) > 0)) {
        PyErr_SetString(PyExc_TypeError, "current() takes no arguments");
        return NULL;
    }
    current = TryGetCurrent(mstate, NULL);
    if (CheckTarget(mstate, base, current, "current()"))
        return NULL;

    if (!base->tealet) {
        PyErr_SetString(mstate->state_error, "must be active");
        return NULL;
    }
    return Py_NewRef((PyObject *)current);
}

/* return the previous tealet (the one that switched to this tealet lineage) */
static PyObject *pytealet_previous(PyObject *self, PyTypeObject *defining_class, PyObject *const *args,
                                   Py_ssize_t nargs, PyObject *kwnames) {
    PyTealetModuleState *mstate = GetModuleStateFromClass(defining_class);
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

PyObject *PyTealetApi_Previous(PyTealetModuleState *mstate) {
    PyTealetObject *current;

    if (!mstate || !mstate->tealet_type) {
        PyErr_SetString(PyExc_RuntimeError, "_tealet module state unavailable");
        return NULL;
    }

    current = PyTealet_GetOrCreateCurrent(mstate, NULL);
    if (!current)
        return NULL;

    return pytealet_previous((PyObject *)current, mstate->tealet_type, NULL, 0, NULL);
}

/* return the main tealet for this tealet lineage */
static PyObject *pytealet_main_method(PyObject *self, PyTypeObject *defining_class, PyObject *const *args,
                                      Py_ssize_t nargs, PyObject *kwnames) {
    PyTealetModuleState *mstate = GetModuleStateFromClass(defining_class);
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

static PyObject *pytealet_is_foreign(PyObject *self, PyTypeObject *defining_class, PyObject *const *args,
                                     Py_ssize_t nargs, PyObject *kwnames) {
    PyTealetObject *base = (PyTealetObject *)self;
    (void)defining_class;
    if (nargs != 0 || (kwnames && PyTuple_GET_SIZE(kwnames) > 0)) {
        PyErr_SetString(PyExc_TypeError, "is_foreign() takes no arguments");
        return NULL;
    }
    return PyBool_FromLong(base->owner_tid != PyThread_get_thread_ident());
}

static int pytealet_prepare_dispatch(PyTealetModuleState *mstate, PyTealetObject *target, PyTealetObject *current,
                                     PyTealetMainData *mdata, PyObject *func, PyTealetApi_RunCFunc cfunc,
                                     const char *operation) {
    if (CheckTarget(mstate, target, current, operation))
        return -1;

    if (target->state != STATE_NEW && target->state != STATE_STUB) {
        PyErr_SetString(mstate->state_error, "must be new or stub");
        return -1;
    }

    if ((func == NULL) == (cfunc == NULL)) {
        PyErr_SetString(PyExc_TypeError, "exactly one of python callable or C callable is required");
        return -1;
    }

    if (func != NULL) {
        if (!PyCallable_Check(func)) {
            PyErr_SetString(PyExc_TypeError, "prepare() argument 'function' must be callable");
            return -1;
        }
        Py_XSETREF(target->prepared_func, Py_NewRef(func));
        target->prepared_cfunc = NULL;
    } else {
        Py_CLEAR(target->prepared_func);
        target->prepared_cfunc = cfunc;
    }

    if (mdata)
        mdata->last_error_remote = 0;
    return 0;
}

static PyObject *pytealet_prepare(PyObject *self, PyTypeObject *defining_class, PyObject *const *args,
                                  Py_ssize_t nargs, PyObject *kwnames) {
    PyTealetModuleState *mstate = GetModuleStateFromClass(defining_class);
    PyObject *func = NULL;

    if (!mstate)
        return NULL;

    if (nargs > 1) {
        PyErr_Format(PyExc_TypeError, "prepare() takes 1 argument (%zd given)", nargs);
        return NULL;
    }
    if (nargs == 1)
        func = args[0];

    if (kwnames && PyTuple_GET_SIZE(kwnames) > 1) {
        PyErr_SetString(PyExc_TypeError, "prepare() takes at most 1 keyword argument");
        return NULL;
    }
    if (kwnames && PyTuple_GET_SIZE(kwnames) > 0) {
        PyObject *key = PyTuple_GET_ITEM(kwnames, 0);
        PyObject *val = args[nargs];
        if (!PyUnicode_Check(key)) {
            PyErr_SetString(PyExc_TypeError, "prepare() keyword names must be strings");
            return NULL;
        }
        if (PyUnicode_CompareWithASCIIString(key, "function") == 0) {
            if (func != NULL) {
                PyErr_SetString(PyExc_TypeError, "prepare() got multiple values for argument 'function'");
                return NULL;
            }
            func = val;
        } else {
            PyErr_Format(PyExc_TypeError, "prepare() got an unexpected keyword argument '%U'", key);
            return NULL;
        }
    }

    if (func == NULL) {
        PyErr_SetString(PyExc_TypeError, "prepare() missing required argument 'function' (pos 1)");
        return NULL;
    }
    if (PyTealetApi_Prepare(mstate, self, func, NULL) < 0)
        return NULL;

    return Py_NewRef(self);
}

int PyTealetApi_Prepare(PyTealetModuleState *mstate, PyObject *target_obj, PyObject *func,
                        PyTealetApi_RunCFunc cfunc) {
    PyTealetObject *target;
    PyTealetObject *current;
    PyTealetMainData *mdata = NULL;

    if (!mstate || !mstate->tealet_type) {
        PyErr_SetString(PyExc_RuntimeError, "_tealet module state unavailable");
        return -1;
    }
    if (!target_obj) {
        PyErr_SetString(PyExc_TypeError, "target must not be NULL");
        return -1;
    }
    if (!PyObject_TypeCheck(target_obj, mstate->tealet_type)) {
        PyErr_SetString(PyExc_TypeError, "target must be a _tealet.tealet instance");
        return -1;
    }

    target = (PyTealetObject *)target_obj;
    current = TryGetCurrent(mstate, &mdata);
    return pytealet_prepare_dispatch(mstate, target, current, mdata, func, cfunc, "prepare()");
}

static PyObject *pytealet_run_dispatch(PyTealetModuleState *mstate, PyTealetObject *target, PyTealetObject *current,
                                       PyTealetMainData *mdata, PyObject *func, PyObject *farg,
                                       PyTealetApi_RunCFunc cfunc) {
    int fail;
    tealet_t *tealet;
    PyThreadState *tstate = PyThreadState_GET();
    PyObject *result;
    int created_from_new;
    int frame_introspection_enabled;
    PyTealetNewArg *ptarg;
    void *switch_arg;

    if (!mdata) {
        PyErr_SetString(PyExc_RuntimeError, "current tealet lineage unavailable");
        return NULL;
    }
    assert((func != NULL) != (cfunc != NULL));

    created_from_new = (target->state == STATE_NEW);
    ptarg = &mdata->new_arg;
    switch_arg = (void *)ptarg;

    ptarg->dest = target;
    ptarg->mstate = mstate;
    ptarg->func = func;
    ptarg->cfunc = cfunc;
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
        /* copy the tstate including context into the old testate */
        PyTealetTstate_Copy(&current->tstate, tstate, 0, 1); /* src (current) belongs to new tealet */

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
        PyObject *panic_exception = NULL;
        if (fail != TEALET_ERR_PANIC)
            PyTealetThrow_ClearPendingException(mdata);
        else
            panic_exception = PyTealetThrow_TakePendingException(mdata);
        PyTealet_TranslateTealetError(mstate, fail, "tealet run failed",
                                      fail == TEALET_ERR_PANIC ? (PyObject *)switch_arg : NULL, panic_exception);
        result = NULL;
    } else {
        result = (PyObject *)switch_arg;
        result = PyTealetThrow_MaybeRaisePending(mdata, current, result);
    }
    dustbin_clear(current->tealet);
    return result;
}

/* run a tealet and optinonally run */
static PyObject *pytealet_run(PyObject *self, PyTypeObject *defining_class, PyObject *const *args, Py_ssize_t nargs,
                              PyObject *kwnames) {
    PyTealetModuleState *mstate = GetModuleStateFromClass(defining_class);
    PyObject *func;
    PyObject *farg;
    PyTealetMainData *mdata = NULL;

    if (!mstate)
        return NULL;

    (void)TryGetCurrent(mstate, &mdata);

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
        if (mdata && mdata->pending_throw_token != 0) {
            /* When an injected exception is already queued for this lineage,
             * worker call arguments are never reached. Allow run() to proceed
             * without a real callable.
             */
            func = Py_None;
        } else {
            PyErr_SetString(PyExc_TypeError, "run() missing required argument 'function' (pos 1)");
            return NULL;
        }
    }
    if (farg == NULL)
        farg = Py_None;

    return PyTealetApi_Run(mstate, self, func, NULL, farg);
}

/* Unified run entrypoint for external C clients via the _tealet capsule API.
 * Accepts exactly one callable mode: a Python callable or a native C callback.
 */
PyObject *PyTealetApi_Run(PyTealetModuleState *mstate, PyObject *target_obj, PyObject *func,
                          PyTealetApi_RunCFunc cfunc, PyObject *arg) {
    PyTealetObject *target;
    PyTealetObject *current;
    PyTealetMainData *mdata = NULL;

    if (!mstate || !mstate->tealet_type) {
        PyErr_SetString(PyExc_RuntimeError, "_tealet module state unavailable");
        return NULL;
    }
    if (!target_obj) {
        PyErr_SetString(PyExc_TypeError, "target must not be NULL");
        return NULL;
    }
    if (!PyObject_TypeCheck(target_obj, mstate->tealet_type)) {
        PyErr_SetString(PyExc_TypeError, "target must be a _tealet.tealet instance");
        return NULL;
    }
    target = (PyTealetObject *)target_obj;
    current = TryGetCurrent(mstate, &mdata);
    if (mdata)
        mdata->last_error_remote = 0;
    if (CheckTarget(mstate, target, current, "run()"))
        return NULL;

    if (target->state != STATE_NEW && target->state != STATE_STUB) {
        PyErr_SetString(mstate->state_error, "must be new or stub");
        return NULL;
    }

    if ((func == NULL) == (cfunc == NULL)) {
        PyErr_SetString(PyExc_TypeError, "exactly one of python callable or C callable is required");
        return NULL;
    }
    if (func != NULL && cfunc == NULL && !PyCallable_Check(func)) {
        if (!mdata || mdata->pending_throw_token == 0) {
            PyErr_SetString(PyExc_TypeError, "function must be callable");
            return NULL;
        }
    }

    if (!arg)
        arg = Py_None;

    /* Any explicit run consumes a previously prepared callable. */
    Py_CLEAR(target->prepared_func);
    target->prepared_cfunc = NULL;

    return pytealet_run_dispatch(mstate, target, current, mdata, func, arg, cfunc);
}

/* switch to a different tealet */
static PyObject *pytealet_switch(PyObject *self, PyTypeObject *defining_class, PyObject *const *args, Py_ssize_t nargs,
                                 PyObject *kwnames) {
    PyTealetModuleState *mstate = GetModuleStateFromClass(defining_class);
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
    PyTealetMainData *mdata;
    Py_ssize_t i;
    if (!mstate)
        return NULL;

    current = TryGetCurrent(mstate, &mdata);
    if (mdata)
        mdata->last_error_remote = 0;

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

    if (CheckTarget(mstate, target, current, "switch()"))
        return NULL;

    if ((target->state == STATE_NEW || target->state == STATE_STUB) &&
        (target->prepared_func || target->prepared_cfunc)) {
        PyObject *prepared_func;
        PyTealetApi_RunCFunc prepared_cfunc;
        PyObject *run_result;

        if (switch_flags & TEALET_XFER_PANIC) {
            PyErr_SetString(PyExc_TypeError, "switch(panic=True) is not supported for prepared tealets");
            return NULL;
        }

        prepared_func = target->prepared_func;
        prepared_cfunc = target->prepared_cfunc;
        target->prepared_func = NULL; /* transfer ownership to the new tealet */
        target->prepared_cfunc = NULL;

        if (prepared_func) {
            run_result = pytealet_run_dispatch(mstate, target, current, mdata, prepared_func, pyarg, NULL);
            Py_DECREF(prepared_func);
        } else {
            run_result = pytealet_run_dispatch(mstate, target, current, mdata, NULL, pyarg, prepared_cfunc);
        }

        return run_result;
    }

    if (target->state != STATE_RUN) {
        PyErr_SetString(mstate->state_error, "must be active");
        return NULL;
    }
    assert(target->tealet);

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
        PyObject *panic_exception = NULL;
        if (fail != TEALET_ERR_PANIC) {
            PyTealetThrow_ClearPendingException(mdata);
            Py_DECREF(pyarg);
            switch_arg = NULL; /* non-panic errors don't return a value */
        } else {
            panic_exception = PyTealetThrow_TakePendingException(mdata);
        }
        PyTealet_TranslateTealetError(mstate, fail, "tealet switch failed", (PyObject *)switch_arg, panic_exception);
        return NULL;
    }
    result = (PyObject *)switch_arg;
    result = PyTealetThrow_MaybeRaisePending(mdata, current, result);
    return result;
}

static PyObject *pytealet_switch_with_flags(PyObject *target_obj, PyObject *arg, uint32_t flags) {
    PyObject *argv[2];
    PyObject *kwnames = NULL;
    PyObject *panic_key = NULL;
    PyObject *result;
    Py_ssize_t nargs = 0;
    uint32_t unknown_flags = flags & ~(uint32_t)PYTEALET_SWITCH_PANIC;

    if (unknown_flags) {
        PyErr_Format(PyExc_ValueError, "unsupported switch flags: 0x%x", (unsigned int)unknown_flags);
        return NULL;
    }

    if (arg)
        argv[nargs++] = arg;

    if (flags & PYTEALET_SWITCH_PANIC) {
        panic_key = PyUnicode_FromString("panic");
        if (!panic_key)
            return NULL;
        kwnames = PyTuple_Pack(1, panic_key);
        Py_DECREF(panic_key);
        if (!kwnames)
            return NULL;
        argv[nargs] = Py_True;
    }

    result = pytealet_switch(target_obj, Py_TYPE(target_obj),
                             (nargs > 0 || kwnames) ? argv : NULL,
                             nargs, kwnames);
    Py_XDECREF(kwnames);
    return result;
}

/* Minimal switch entrypoint for external C clients via the _tealet capsule API.
 * This keeps validation and exception behavior aligned with the Python switch()
 * method by reusing the same internal implementation.
 */
PyObject *PyTealetApi_Switch(PyTealetModuleState *mstate, PyObject *target_obj, PyObject *arg, uint32_t flags) {

    if (!mstate || !mstate->tealet_type) {
        PyErr_SetString(PyExc_RuntimeError, "_tealet module state unavailable");
        return NULL;
    }
    if (!target_obj) {
        PyErr_SetString(PyExc_TypeError, "target must not be NULL");
        return NULL;
    }
    if (!PyObject_TypeCheck(target_obj, mstate->tealet_type)) {
        PyErr_SetString(PyExc_TypeError, "target must be a _tealet.tealet instance");
        return NULL;
    }

    return pytealet_switch_with_flags(target_obj, arg, flags);
}

/* Inner C API for exception injection. This performs all validation and
 * token bookkeeping, while the Python API wrapper handles argument parsing.
 * target is required.
 *
 * Callers should not assume any particular resume path once the exception is
 * delivered to the target: target code may catch and switch elsewhere.
 */
static int pytealet_set_exception_inner(PyTealetModuleState *mstate, PyTealetObject *target, PyTealetObject *current,
                                        PyTealetMainData *mdata, PyObject *exc, PyObject *fallback) {
    uint64_t token;

    assert(mstate);
    assert(target);
    assert(current);
    assert(mdata);
    assert(exc);

    if (!PyExceptionInstance_Check(exc)) {
        PyErr_SetString(PyExc_TypeError, "exception must be a BaseException instance");
        return -1;
    }

    if (target->state == STATE_RUN) {
        if (!target->tealet) {
            PyErr_SetString(mstate->state_error, "target tealet must be active");
            return -1;
        }
    } else if (target->state != STATE_NEW && target->state != STATE_STUB) {
        PyErr_SetString(mstate->state_error, "target tealet must be active, new, or stub");
        return -1;
    }
    if (CheckTarget(mstate, target, current, "set_exception()"))
        return -1;

    if (!fallback)
        fallback = Py_None;

    /* Self-target injections should not install a fallback switch target.
     * If the exception is delivered back into the same tealet, fallback-based
     * rerouting is nonsensical and can create confusing self-redirect paths.
     */
    if (fallback == (PyObject *)target)
        fallback = Py_None;

    if (fallback != Py_None) {
        PyTealetObject *fallback_t;
        if (!PyTealet_Check(fallback, mstate)) {
            PyErr_SetString(PyExc_TypeError, "fallback must be a tealet or None");
            return -1;
        }
        fallback_t = (PyTealetObject *)fallback;
        if (fallback_t->state != STATE_RUN || !fallback_t->tealet) {
            PyErr_SetString(mstate->state_error, "fallback tealet must be active");
            return -1;
        }
        if (CheckTarget(mstate, fallback_t, target, "set_exception(fallback)"))
            return -1;
    }

    /* any new exception overrides any pending throw */
    if (mdata->pending_throw_token != 0) {
        PyObject *old_exc = NULL;
        PyObject *old_fallback = NULL;
        int old_pop_rc =
            PyTealetThrow_RegistryPop(mdata, mdata->pending_throw_token, &old_exc, &old_fallback);
        if (old_pop_rc < 0)
            return -1;
        Py_XDECREF(old_exc);
        Py_XDECREF(old_fallback);
        mdata->pending_throw_token = 0;
    }

    /* any new exception for the target overrides any in-flight handling of a previous one.*/
    if (target->inflight_throw_token != 0) {
        PyObject *old_exc = NULL;
        PyObject *old_fallback = NULL;
        int old_pop_rc =
            PyTealetThrow_RegistryPop(mdata, target->inflight_throw_token, &old_exc, &old_fallback);
        if (old_pop_rc < 0)
            return -1;
        Py_XDECREF(old_exc);
        Py_XDECREF(old_fallback);
        target->inflight_throw_token = 0;
    }

    token = PyTealetThrow_NextToken(mdata);
    if (PyTealetThrow_RegistrySet(mdata, token, exc, fallback == Py_None ? NULL : fallback) < 0)
        return -1;

    mdata->pending_throw_token = token;
    return 0;
}

static PyObject *pytealet_set_exception(PyObject *self, PyTypeObject *defining_class, PyObject *const *args,
                                        Py_ssize_t nargs, PyObject *kwnames) {
    PyTealetModuleState *mstate = GetModuleStateFromClass(defining_class);
    PyTealetObject *target = (PyTealetObject *)self;
    PyTealetObject *current;
    PyTealetMainData *mdata;
    PyObject *exc = NULL;
    PyObject *fallback = Py_None;

    if (!mstate)
        return NULL;

    if (nargs < 1 || nargs > 2) {
        PyErr_Format(PyExc_TypeError, "set_exception() takes 1 or 2 arguments (%zd given)", nargs);
        return NULL;
    }
    exc = args[0];
    if (nargs == 2)
        fallback = args[1];

    if (kwnames && PyTuple_GET_SIZE(kwnames) > 0) {
        Py_ssize_t i;
        for (i = 0; i < PyTuple_GET_SIZE(kwnames); i++) {
            PyObject *key = PyTuple_GET_ITEM(kwnames, i);
            PyObject *val = args[nargs + i];
            if (!PyUnicode_Check(key)) {
                PyErr_SetString(PyExc_TypeError, "set_exception() keyword names must be strings");
                return NULL;
            }
            if (PyUnicode_CompareWithASCIIString(key, "exception") == 0) {
                exc = val;
            } else if (PyUnicode_CompareWithASCIIString(key, "fallback") == 0) {
                fallback = val;
            } else {
                PyErr_Format(PyExc_TypeError, "set_exception() got an unexpected keyword argument '%U'", key);
                return NULL;
            }
        }
    }

    if (!exc) {
        PyErr_SetString(PyExc_TypeError, "set_exception() missing required argument 'exception'");
        return NULL;
    }

    current = TryGetCurrent(mstate, &mdata);
    if (CheckTarget(mstate, target, current, "set_exception()"))
        return NULL;

    if (pytealet_set_exception_inner(mstate, target, current, mdata, exc, fallback) < 0)
        return NULL;
    Py_RETURN_NONE;
}

int PyTealetApi_SetException(PyTealetModuleState *mstate, PyObject *target_obj, PyObject *exc, PyObject *fallback) {
    PyTealetObject *target;
    PyTealetObject *current;
    PyTealetMainData *mdata = NULL;

    if (!mstate || !mstate->tealet_type) {
        PyErr_SetString(PyExc_RuntimeError, "_tealet module state unavailable");
        return -1;
    }
    if (!target_obj) {
        PyErr_SetString(PyExc_TypeError, "target must not be NULL");
        return -1;
    }
    if (!exc) {
        PyErr_SetString(PyExc_TypeError, "exception must not be NULL");
        return -1;
    }
    if (!PyObject_TypeCheck(target_obj, mstate->tealet_type)) {
        PyErr_SetString(PyExc_TypeError, "target must be a _tealet.tealet instance");
        return -1;
    }

    target = (PyTealetObject *)target_obj;
    current = TryGetCurrent(mstate, &mdata);
    if (!current || !mdata) {
        PyErr_SetString(PyExc_RuntimeError, "current tealet unavailable");
        return -1;
    }
    if (!fallback)
        fallback = Py_None;

    if (CheckTarget(mstate, target, current, "set_exception()"))
        return -1;

    return pytealet_set_exception_inner(mstate, target, current, mdata, exc, fallback);
}

/* Convenience API: schedule exception for target and transfer immediately.
 * - RUN target: inject then switch.
 * - NEW/STUB target: inject then run.
 *
 * This does not guarantee a switch back to the caller: target code may catch
 * the injected exception and switch to a different tealet.
 */
static PyObject *pytealet_throw(PyObject *self, PyTypeObject *defining_class, PyObject *const *args, Py_ssize_t nargs,
                                PyObject *kwnames) {
    PyTealetModuleState *mstate = GetModuleStateFromClass(defining_class);
    PyObject *exc = NULL;

    if (!mstate)
        return NULL;

    if (nargs != 1) {
        PyErr_Format(PyExc_TypeError, "throw() takes 1 argument (%zd given)", nargs);
        return NULL;
    }
    exc = args[0];

    if (kwnames && PyTuple_GET_SIZE(kwnames) > 0) {
        Py_ssize_t i;
        for (i = 0; i < PyTuple_GET_SIZE(kwnames); i++) {
            PyObject *key = PyTuple_GET_ITEM(kwnames, i);
            PyObject *val = args[nargs + i];
            if (!PyUnicode_Check(key)) {
                PyErr_SetString(PyExc_TypeError, "throw() keyword names must be strings");
                return NULL;
            }
            if (PyUnicode_CompareWithASCIIString(key, "exception") == 0) {
                exc = val;
            } else {
                PyErr_Format(PyExc_TypeError, "throw() got an unexpected keyword argument '%U'", key);
                return NULL;
            }
        }
    }

    if (!exc) {
        PyErr_SetString(PyExc_TypeError, "throw() missing required argument 'exception'");
        return NULL;
    }

    return PyTealetApi_Throw(mstate, self, exc, PYTEALET_THROW_FLAGS_DEFAULT);
}

/* C API throw primitive: inject exception and transfer immediately.
 * - RUN target: inject then switch (flags may request panic transfer).
 * - NEW/STUB target: inject then run (panic throw flag is not supported).
 */
PyObject *PyTealetApi_Throw(PyTealetModuleState *mstate, PyObject *target_obj, PyObject *exc, uint32_t flags) {
    PyTealetObject *target;
    PyTealetObject *current;
    PyTealetMainData *mdata = NULL;
    uint32_t unknown_flags = flags & ~(uint32_t)PYTEALET_THROW_PANIC;

    if (!mstate || !mstate->tealet_type) {
        PyErr_SetString(PyExc_RuntimeError, "_tealet module state unavailable");
        return NULL;
    }
    if (!target_obj) {
        PyErr_SetString(PyExc_TypeError, "target must not be NULL");
        return NULL;
    }
    if (!exc) {
        PyErr_SetString(PyExc_TypeError, "exception must not be NULL");
        return NULL;
    }
    if (unknown_flags) {
        PyErr_Format(PyExc_ValueError, "unsupported throw flags: 0x%x", (unsigned int)unknown_flags);
        return NULL;
    }
    if (!PyObject_TypeCheck(target_obj, mstate->tealet_type)) {
        PyErr_SetString(PyExc_TypeError, "target must be a _tealet.tealet instance");
        return NULL;
    }

    target = (PyTealetObject *)target_obj;
    current = TryGetCurrent(mstate, &mdata);
    if (mdata)
        mdata->last_error_remote = 0;

    if (CheckTarget(mstate, target, current, "throw()"))
        return NULL;

    if (target->state == STATE_RUN) {
        uint32_t switch_flags = (flags & PYTEALET_THROW_PANIC) ? PYTEALET_SWITCH_PANIC : PYTEALET_SWITCH_FLAGS_DEFAULT;
        if (pytealet_set_exception_inner(mstate, target, current, mdata, exc, (PyObject *)current) < 0)
            return NULL;
        return PyTealetApi_Switch(mstate, target_obj, NULL, switch_flags);
    }

    if (target->state == STATE_NEW || target->state == STATE_STUB) {
        PyObject *prepared_func = target->prepared_func;
        PyObject *run_result;

        if (flags & PYTEALET_THROW_PANIC) {
            PyErr_SetString(PyExc_TypeError, "throw(panic) is not supported for new or stub tealets");
            return NULL;
        }

        if (pytealet_set_exception_inner(mstate, target, current, mdata, exc, (PyObject *)current) < 0)
            return NULL;

        /* consume the prepared func.  We don't actually need it because the
         * exception gets raised even before running it.
         */
        target->prepared_func = NULL;
        target->prepared_cfunc = NULL;
        run_result = pytealet_run(target_obj, mstate->tealet_type, NULL, 0, NULL);
        Py_XDECREF(prepared_func);
        return run_result;
    }

    PyErr_SetString(mstate->state_error, "throw() target must be active, new, or stub");
    return NULL;
}

/* Context is thread-affine only while a tealet is actively running.
 * For suspended/new/exit tealets, context lives in tstate storage and can be
 * accessed cross-thread under the lineage lock.
 */
static int pytealet_context_is_running(PyTealetObject *tealet) {
    assert(tealet);
    if (!tealet->tealet)
        return 0;
    /* Caller holds the lineage domain lock, so this identity check is stable
     * with respect to switching.
     */
    return tealet_current(tealet->tealet) == tealet->tealet;
}

static PyObject *pytealet_get_context(PyObject *self, PyObject *Py_UNUSED(_ignored)) {
    PyTealetObject *tealet = (PyTealetObject *)self;
    PyTealetModuleState *mstate = GetModuleStateFromClass(Py_TYPE(tealet));
    PyObject *ctx = NULL;
    int running;

    if (!mstate)
        return NULL;

    pytealet_domain_lock_obj_lock(tealet->domain_lock_obj);

    running = pytealet_context_is_running(tealet);
    if (running && tealet->owner_tid != PyThread_get_thread_ident()) {
        pytealet_domain_lock_obj_unlock(tealet->domain_lock_obj);
        PyErr_SetString(mstate->invalid_error, "cannot access context of a running tealet from a different thread");
        return NULL;
    }

    if (running) {
        assert(tealet->owner_tid == PyThread_get_thread_ident());
        /* get the active context */
        ctx = PyThreadState_GET()->context;
    } else {
        ctx = tealet->tstate.context;
    }
    Py_XINCREF(ctx);

    pytealet_domain_lock_obj_unlock(tealet->domain_lock_obj);

    if (!ctx)
        Py_RETURN_NONE;
    return ctx;
}

static PyObject *pytealet_set_context(PyObject *self, PyObject *value) {
    PyTealetObject *tealet = (PyTealetObject *)self;
    PyTealetModuleState *mstate = GetModuleStateFromClass(Py_TYPE(tealet));
    PyObject *new_ctx = (value == Py_None) ? NULL : value;
    PyObject *old_ctx = NULL;
    int running;

    if (!mstate)
        return NULL;

    if (new_ctx && !PyContext_CheckExact(new_ctx)) {
        PyErr_SetString(PyExc_TypeError, "context must be a contextvars.Context or None");
        return NULL;
    }

    pytealet_domain_lock_obj_lock(tealet->domain_lock_obj);

    running = pytealet_context_is_running(tealet);
    if (running && tealet->owner_tid != PyThread_get_thread_ident()) {
        pytealet_domain_lock_obj_unlock(tealet->domain_lock_obj);
        PyErr_SetString(mstate->invalid_error, "cannot access context of a running tealet from a different thread");
        return NULL;
    }

    if (running) {
        assert(tealet->owner_tid == PyThread_get_thread_ident());
        /* set the active context */
        PyThreadState *tstate = PyThreadState_GET();
        Py_XINCREF(new_ctx);
        old_ctx = tstate->context;
        tstate->context = new_ctx;
        tstate->context_ver++;
    } else {
        Py_XINCREF(new_ctx);
        old_ctx = tealet->tstate.context;
        tealet->tstate.context = new_ctx;
    }

    pytealet_domain_lock_obj_unlock(tealet->domain_lock_obj);

    Py_XDECREF(old_ctx);

    Py_RETURN_NONE;
}

static struct PyMethodDef pytealet_methods[] = {
    {"stub", (PyCFunction)(void (*)(void))pytealet_stub, METH_METHOD | METH_FASTCALL | METH_KEYWORDS, ""},
    {"duplicate", (PyCFunction)(void (*)(void))pytealet_duplicate, METH_METHOD | METH_FASTCALL | METH_KEYWORDS,
    "duplicate() -> tealet\n\n"
    "Create a duplicate wrapper from a NEW or STUB tealet."},
    {"current", (PyCFunction)(void (*)(void))pytealet_current, METH_METHOD | METH_FASTCALL | METH_KEYWORDS, ""},
    {"previous", (PyCFunction)(void (*)(void))pytealet_previous, METH_METHOD | METH_FASTCALL | METH_KEYWORDS, ""},
    {"main", (PyCFunction)(void (*)(void))pytealet_main_method, METH_METHOD | METH_FASTCALL | METH_KEYWORDS, ""},
    {"is_foreign", (PyCFunction)(void (*)(void))pytealet_is_foreign,
     METH_METHOD | METH_FASTCALL | METH_KEYWORDS, ""},
    {"prepare", (PyCFunction)(void (*)(void))pytealet_prepare, METH_METHOD | METH_FASTCALL | METH_KEYWORDS,
        "prepare(function) -> tealet\n\n"
     "Store a callable to be used by the first switch(arg) on this NEW/STUB tealet."},
    {"run", (PyCFunction)(void (*)(void))pytealet_run, METH_METHOD | METH_FASTCALL | METH_KEYWORDS, ""},
    {"switch", (PyCFunction)(void (*)(void))pytealet_switch, METH_METHOD | METH_FASTCALL | METH_KEYWORDS, ""},
    {"throw", (PyCFunction)(void (*)(void))pytealet_throw, METH_METHOD | METH_FASTCALL | METH_KEYWORDS,
     "throw(exception) -> object\n\n"
     "Inject exception into target and switch to it.\n"
     "No guarantee the caller resumes: target may catch and switch elsewhere."},
    {"set_exception", (PyCFunction)(void (*)(void))pytealet_set_exception, METH_METHOD | METH_FASTCALL | METH_KEYWORDS,
     "set_exception(exception, fallback=None) -> None\n\n"
     "Queue exception for delivery when target next runs.\n"
     "No guarantee about which tealet runs after delivery; target may catch and switch elsewhere."},
    {NULL, NULL} /* sentinel */
};

/* ===================================================================== */
/* Properties                                                            */
/* ===================================================================== */
static PyObject *pytealet_get_context_prop(PyObject *_self, void *_closure) {
    (void)_closure;
    return pytealet_get_context(_self, NULL);
}

static int pytealet_set_context_prop(PyObject *_self, PyObject *value, void *_closure) {
    PyObject *result;

    (void)_closure;
    if (!value) {
        PyErr_SetString(PyExc_AttributeError, "can't delete context attribute");
        return -1;
    }
    result = pytealet_set_context(_self, value);
    if (!result)
        return -1;
    Py_DECREF(result);
    return 0;
}

/* get main tealet for this lineage */
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
        return Py_XNewRef((PyObject *)TryGetMain(mstate, NULL));
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
        if (self == TryGetCurrent(mstate, NULL)) {
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

static struct PyGetSetDef pytealet_getset[] = {
    {"state", pytealet_get_state, NULL, "", NULL},
    {"frame", pytealet_get_frame, NULL, "", NULL},
    {"context", pytealet_get_context_prop, pytealet_set_context_prop, "", NULL},
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
                                            {Py_tp_traverse, pytealet_traverse},
                                            {Py_tp_clear, pytealet_clear},
                                            {Py_tp_methods, pytealet_methods},
                                            {Py_tp_getset, pytealet_getset},
                                            {Py_tp_new, pytealet_new},
                                            {0, NULL}};
#if defined(__GNUC__)
#pragma GCC diagnostic pop
#endif

PyType_Spec pytealet_type_spec = {"_tealet.tealet", sizeof(PyTealetObject), 0,
                                  Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE | Py_TPFLAGS_HAVE_GC
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
static void pytealet_domain_lock_cb(void *arg) { pytealet_domain_lock_obj_lock((PyObject *)arg); }

static void pytealet_domain_unlock_cb(void *arg) { pytealet_domain_lock_obj_unlock((PyObject *)arg); }

static int pytealet_configure_domain_locking(tealet_t *main_tealet, PyTealetMainData *mdata) {
    tealet_lock_t locking;
    assert(mdata);
    assert(mdata->domain_lock_obj);

    locking.mode = TEALET_LOCK_AUTO;
    locking.lock = pytealet_domain_lock_cb;
    locking.unlock = pytealet_domain_unlock_cb;
    locking.arg = (void *)mdata->domain_lock_obj;
    if (tealet_configure_set_locking(main_tealet, &locking) < 0) {
        return -1;
    }
    return 0;
}

static void pytealet_free_domain_lock(PyTealetMainData *mdata) {
    if (mdata)
        Py_CLEAR(mdata->domain_lock_obj);
}

/* Access to mdata->wrappers must be externally synchronized in free-threaded
 * builds. Set operations are not a safe atomic boundary for complex entries:
 * they can invoke hashing/equality/weakref machinery and race with concurrent
 * mutation from other threads.
 */
static void pytealet_domain_lock(PyTealetMainData *mdata) {
    assert(mdata);
    assert(mdata->domain_lock_obj);
    pytealet_domain_lock_obj_lock(mdata->domain_lock_obj);
}

static void pytealet_domain_unlock(PyTealetMainData *mdata) {
    assert(mdata);
    assert(mdata->domain_lock_obj);
    pytealet_domain_lock_obj_unlock(mdata->domain_lock_obj);
}
#else
static int pytealet_configure_domain_locking(tealet_t *main_tealet, PyTealetMainData *mdata) {
    (void)main_tealet;
    (void)mdata;
    return 0;
}

static void pytealet_free_domain_lock(PyTealetMainData *mdata) {
    if (mdata)
        Py_CLEAR(mdata->domain_lock_obj);
}

static void pytealet_domain_lock(PyTealetMainData *mdata) { (void)mdata; }

static void pytealet_domain_unlock(PyTealetMainData *mdata) { (void)mdata; }
#endif

/* return a borrowed reference to this thread's main tealet.
 * create it if missing.  returns NULL and sets an exception on error.
 */
PyTealetObject *PyTealet_GetOrCreateMain(PyTealetModuleState *mstate, PyTealetMainData **mdata_out) {
    /* Get the thread's main tealet */
    PyTealetMainData *mdata;
    PyTealetObject *t_main = NULL;
    tealet_t *tmain = NULL;
    int tss_registered = 0;
    if (mdata_out)
        *mdata_out = NULL;
    assert(mstate);
    mdata = (PyTealetMainData *)PyThread_tss_get(&mstate->tls_key);

    /* tls and main tealet doesn't exist yet.  create it. */
    if (!mdata) {
        tealet_alloc_t talloc;
        /* Use PyMem allocators for libtealet heap allocations. */
        talloc.malloc_p = tealet_malloc_wrapper;
        talloc.free_p = tealet_free_wrapper;
        talloc.context = NULL;
        tmain = tealet_initialize(&talloc, sizeof(PyTealetExtra));
        if (!tmain) {
            PyErr_NoMemory();
            goto fail;
        }
        {
            const char *check_stack_env = getenv("PYTEALET_CHECK_STACK");
            if (check_stack_env && *check_stack_env && *check_stack_env != '0') {
                if (tealet_configure_check_stack(tmain, 0) < 0) {
                    PyErr_SetString(PyExc_RuntimeError, "tealet_configure_check_stack failed");
                    goto fail;
                }
            }
        }
        mdata = (PyTealetMainData *)PyMem_Malloc(sizeof(*mdata));
        if (!mdata) {
            PyErr_NoMemory();
            goto fail;
        }
        memset(mdata, 0, sizeof(*mdata));
        mdata->tid = PyThread_get_thread_ident();
        mdata->domain_lock_obj = pytealet_domain_lock_obj_new();
        if (!mdata->domain_lock_obj)
            goto fail;
        mdata->dustbin = PyList_New(DUSTBIN_PREALLOC);
        if (!mdata->dustbin) {
            PyErr_NoMemory();
            goto fail;
        }
        mdata->wrappers = PySet_New(NULL);
        if (!mdata->wrappers) {
            PyErr_NoMemory();
            goto fail;
        }
        mdata->throw_records = PyDict_New();
        if (!mdata->throw_records) {
            PyErr_NoMemory();
            goto fail;
        }
        mdata->throw_next_token = 0;
        mdata->pending_throw_token = 0;
        mdata->last_error_remote = 0;
        if (PyList_SetSlice(mdata->dustbin, 0, DUSTBIN_PREALLOC, NULL) < 0) {
            goto fail;
        }
        if (pytealet_configure_domain_locking(tmain, mdata) < 0) {
            PyErr_SetString(PyExc_RuntimeError, "failed to configure tealet domain lock callbacks");
            goto fail;
        }
        *tealet_main_userpointer(tmain) = (void *)mdata;

        /* create the main tealet */
        t_main = (PyTealetObject *)pytealet_new_impl(mstate->tealet_type, NULL, NULL, 1);
        if (!t_main)
            goto fail;

        t_main->domain_lock_obj = Py_NewRef(mdata->domain_lock_obj);

        t_main->tealet = tmain;
        t_main->state = STATE_RUN;
        TEALET_SET_PYOBJECT(tmain, t_main); /* back link */
        mdata->main_wrapper = (PyObject *)t_main;
        if (PyThread_tss_set(&mstate->tls_key, (void *)mdata) != 0) {
            PyErr_SetString(PyExc_RuntimeError, "failed to set thread-local main tealet");
            goto fail;
        }
        tss_registered = 1;

        if (PyTealet_LineageLinkThreadData(mstate, mdata) < 0) {
            if (!PyErr_Occurred())
                PyErr_SetString(PyExc_RuntimeError, "failed to register thread main data");
            goto fail;
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

fail:
    if (tss_registered) {
        (void)PyThread_tss_set(&mstate->tls_key, NULL);
    }
    if (mdata) {
        if (mdata->main_wrapper) {
            if (t_main && t_main->tealet)
                TEALET_SET_PYOBJECT(t_main->tealet, NULL);
            if (t_main)
                t_main->tealet = NULL;
            if (t_main)
                t_main->state = STATE_EXIT;
        }
        Py_CLEAR(mdata->main_wrapper);
        Py_CLEAR(mdata->dustbin);
        Py_CLEAR(mdata->wrappers);
        Py_CLEAR(mdata->throw_records);
        pytealet_free_domain_lock(mdata);
        PyMem_Free(mdata);
    }
    if (tmain)
        tealet_finalize(tmain);
    return NULL;
}

/* Helper function to get main and mdata if it exists.  returns NULL if no main exists, without setting error. */
PyTealetObject *TryGetMain(PyTealetModuleState *mstate, PyTealetMainData **mdata_out) {
    if (mdata_out)
        *mdata_out = NULL;
    PyTealetMainData *mdata = (PyTealetMainData *)PyThread_tss_get(&mstate->tls_key);
    if (!mdata) {
        return NULL;
    }
    assert(mdata->main_wrapper);
    if (mdata_out)
        *mdata_out = mdata;
    return (PyTealetObject *)mdata->main_wrapper;
}

/* return a borrowed ref to this threads current tealet, or NULL if none exists, without setting error */
PyTealetObject *TryGetCurrent(PyTealetModuleState *mstate, PyTealetMainData **mdata_out) {
    PyTealetMainData *mdata;
    if (mdata_out)
        *mdata_out = NULL;
    PyTealetObject *main = TryGetMain(mstate, &mdata);
    if (main) {
        if (mdata_out)
            *mdata_out = mdata;
        tealet_t *t_current = tealet_current(main->tealet);
        return TEALET_PYOBJECT(t_current);
    }
    return NULL;
}

/* return this thread's current tealet, creating main/current if missing */
PyTealetObject *PyTealet_GetOrCreateCurrent(PyTealetModuleState *mstate, PyTealetMainData **mdata_out) {
    PyTealetObject *current = TryGetCurrent(mstate, mdata_out);
    if (current)
        return current;
    return PyTealet_GetOrCreateMain(mstate, mdata_out);
}

/* Collect active non-main tealet wrappers for the current lineage without
 * mutating runtime state.
 */
static int pytealet_collect_active_wrappers(PyTealetModuleState *mstate, PyTealetMainData *mdata, PyObject *active_out,
                                            PyTealetObject *caller, unsigned int collect_flags) {
    PyObject *snapshot = NULL;
    Py_ssize_t i;

    assert(mstate);
    assert(mdata);
    assert(active_out && PyList_Check(active_out));
    assert(mdata->wrappers && PySet_Check(mdata->wrappers));

    pytealet_domain_lock(mdata);
    snapshot = PySequence_List(mdata->wrappers);
    pytealet_domain_unlock(mdata);
    if (!snapshot)
        return -1;

    for (i = 0; i < PyList_GET_SIZE(snapshot); i++) {
        PyObject *wref = PyList_GET_ITEM(snapshot, i); /* borrowed */
        PyObject *obj = NULL;
        int weak_status = pytealet_weakref_get_live(wref, &obj);
        if (weak_status < 0) {
            Py_DECREF(snapshot);
            return -1;
        }
        if (weak_status == 0)
            continue;

        assert(PyTealet_Check(obj, mstate));
        {
            PyTealetObject *wrapper = (PyTealetObject *)obj;
            int add_to_list = 0;

            if ((collect_flags & PYTEALET_COLLECT_OMIT_CALLER) && caller && wrapper == caller) {
                Py_DECREF(obj);
                continue;
            }

            if ((collect_flags & PYTEALET_COLLECT_OMIT_MAIN) && wrapper->tealet && TEALET_IS_MAIN(wrapper->tealet)) {
                Py_DECREF(obj);
                continue;
            }

            if (wrapper->tealet && wrapper->state != STATE_STUB)
                add_to_list = (tealet_status(wrapper->tealet) == TEALET_STATUS_ACTIVE);

            if (add_to_list && PyList_Append(active_out, obj) < 0) {
                Py_DECREF(obj);
                Py_DECREF(snapshot);
                return -1;
            }
        }
        Py_DECREF(obj);
    }

    Py_DECREF(snapshot);
    return 0;
}

/* Best-effort kill of each tealet in a snapshot of active wrappers.
 * Any per-target failure is reported as unraisable and processing continues.
 */
static PyObject *pytealet_make_kill_exception(PyTealetModuleState *mstate, PyObject *kill_exc_spec) {
    PyObject *exc;

    if (!kill_exc_spec || kill_exc_spec == Py_None)
        return PyObject_CallNoArgs(mstate->tealet_exit_error);

    if (!PyCallable_Check(kill_exc_spec)) {
        PyErr_SetString(PyExc_TypeError, "kill_exc must be callable or None");
        return NULL;
    }

    exc = PyObject_CallNoArgs(kill_exc_spec);
    if (!exc)
        return NULL;
    if (!PyExceptionInstance_Check(exc)) {
        Py_DECREF(exc);
        PyErr_SetString(PyExc_TypeError, "kill_exc callable must return an exception instance");
        return NULL;
    }
    return exc;
}

static int pytealet_validate_kill_exception(PyObject *kill_exc_spec) {
    if (!kill_exc_spec || kill_exc_spec == Py_None)
        return 0;
    if (PyCallable_Check(kill_exc_spec))
        return 0;

    PyErr_SetString(PyExc_TypeError, "kill_exc must be callable or None");
    return -1;
}

static int pytealet_kill_active_snapshot(PyTealetModuleState *mstate, PyObject *active_snapshot,
                                         PyObject *kill_exc_spec) {
    Py_ssize_t i;

    assert(mstate);
    assert(active_snapshot && PyList_Check(active_snapshot));

    for (i = 0; i < PyList_GET_SIZE(active_snapshot); i++) {
        PyObject *obj = PyList_GET_ITEM(active_snapshot, i); /* borrowed */
        PyTealetObject *target;
        PyObject *exc = NULL;
        PyObject *throw_result = NULL;
        PyObject *throw_args[1];

        if (!PyTealet_Check(obj, mstate))
            continue;
        target = (PyTealetObject *)obj;

        if (target->state != STATE_RUN || !target->tealet)
            continue;

        exc = pytealet_make_kill_exception(mstate, kill_exc_spec);
        if (!exc)
            return -1;

        throw_args[0] = exc;
        throw_result = pytealet_throw((PyObject *)target, mstate->tealet_type, throw_args, 1, NULL);
        Py_DECREF(exc);

        if (!throw_result) {
            PyErr_WriteUnraisable(obj);
            PyErr_Clear();
            continue;
        }
        Py_DECREF(throw_result);
    }

    return 0;
}

/* Internal helper used by thread_kill() and thread_reap().
 * Repeatedly throws the configured kill exception into active wrappers and
 * returns any remaining active wrappers after cleanup_passes attempts.
 */
static PyObject *pytealet_thread_kill_inner(PyTealetModuleState *mstate, PyTealetMainData *mdata,
                                            Py_ssize_t cleanup_passes, PyTealetObject *caller,
                                            PyObject *kill_exc_spec) {
    Py_ssize_t pass_idx;

    assert(mstate);
    assert(mdata);

    if (cleanup_passes < 1) {
        PyErr_SetString(PyExc_ValueError, "cleanup_passes must be >= 1");
        return NULL;
    }

    if (pytealet_validate_kill_exception(kill_exc_spec) < 0)
        return NULL;

    for (pass_idx = 0; pass_idx < cleanup_passes; pass_idx++) {
        PyObject *active = PyList_New(0);
        if (!active)
            return NULL;

        if (pytealet_collect_active_wrappers(mstate, mdata, active, caller,
                                             PYTEALET_COLLECT_OMIT_MAIN | PYTEALET_COLLECT_OMIT_CALLER) < 0) {
            Py_DECREF(active);
            return NULL;
        }
        if (PyList_GET_SIZE(active) == 0)
            return active;

        if (pytealet_kill_active_snapshot(mstate, active, kill_exc_spec) < 0) {
            Py_DECREF(active);
            return NULL;
        }
        Py_DECREF(active);
    }

    {
        PyObject *active = PyList_New(0);
        if (!active)
            return NULL;
        if (pytealet_collect_active_wrappers(mstate, mdata, active, caller,
                                             PYTEALET_COLLECT_OMIT_MAIN | PYTEALET_COLLECT_OMIT_CALLER) < 0) {
            Py_DECREF(active);
            return NULL;
        }
        return active;
    }
}

/* Explicitly clean up this thread's tealet lineage.
 * Phase 1: run thread_kill semantics for cleanup_passes attempts.
 * Phase 2: force-teardown remaining handles and return wrappers that were
 * still active at force-teardown time.
 */
PyObject *PyTealet_ThreadReap(PyTealetModuleState *mstate, Py_ssize_t cleanup_passes, PyObject *kill_exc_spec) {
    PyTealetObject *current;
    PyTealetMainData *mdata;
    PyObject *nerfed;
    PyObject *remaining;

    assert(mstate);
    nerfed = PyList_New(0);
    if (!nerfed)
        return NULL;

    current = TryGetCurrent(mstate, &mdata);
    if (!current) {
        /* no current tealet, idempotent result (cleanup non-existing) */
        return nerfed;
    }
    if (!TEALET_IS_MAIN(current->tealet)) {
        PyErr_SetString(mstate->state_error, "thread_reap() must be called from this thread's main tealet");
        Py_DECREF(nerfed);
        return NULL;
    }

    remaining = pytealet_thread_kill_inner(mstate, mdata, cleanup_passes, current, kill_exc_spec);
    if (!remaining) {
        Py_DECREF(nerfed);
        return NULL;
    }
    Py_DECREF(remaining);

    if (PyTealet_LineageReapInner(mstate, mdata, nerfed, 1, 0) < 0) {
        Py_DECREF(nerfed);
        return NULL;
    }
    return nerfed;
}

/* Return active non-main tealet wrappers for this thread's lineage.
 * Unlike thread_reap(), this can be called from any tealet in the
 * current lineage.
 */
PyObject *PyTealet_ThreadActive(PyTealetModuleState *mstate) {
    PyTealetObject *current;
    PyTealetMainData *mdata;
    PyObject *active;

    assert(mstate);
    active = PyList_New(0);
    if (!active)
        return NULL;

    current = TryGetCurrent(mstate, &mdata);
    if (!current) {
        /* no current tealet, idempotent result */
        return active;
    }

    if (pytealet_collect_active_wrappers(mstate, mdata, active, current, PYTEALET_COLLECT_OMIT_MAIN) < 0) {
        Py_DECREF(active);
        return NULL;
    }
    return active;
}

/* Try to kill active non-main tealets by throwing a configured exception
 * repeatedly, up to cleanup_passes attempts. Can be called from any tealet in
 * the current lineage, and never targets the caller tealet itself.
 *
 * thread_kill() is not guaranteed to return control to the caller tealet:
 * a target may catch the injected exception and switch to a different
 * scheduling point.
 */
PyObject *PyTealet_ThreadKill(PyTealetModuleState *mstate, Py_ssize_t cleanup_passes, PyObject *kill_exc_spec) {
    PyTealetObject *current;
    PyTealetMainData *mdata;
    PyObject *active;

    assert(mstate);

    current = TryGetCurrent(mstate, &mdata);
    if (!current) {
        /* no current tealet, idempotent result */
        active = PyList_New(0);
        return active;
    }

    return pytealet_thread_kill_inner(mstate, mdata, cleanup_passes, current, kill_exc_spec);
}

PyObject *PyTealetApi_ThreadReap(PyTealetModuleState *mstate, Py_ssize_t cleanup_passes, PyObject *kill_exc_spec) {
    if (!mstate) {
        PyErr_SetString(PyExc_RuntimeError, "_tealet module state unavailable");
        return NULL;
    }
    if (!kill_exc_spec)
        kill_exc_spec = Py_None;
    return PyTealet_ThreadReap(mstate, cleanup_passes, kill_exc_spec);
}

PyObject *PyTealetApi_ThreadActive(PyTealetModuleState *mstate) {
    if (!mstate) {
        PyErr_SetString(PyExc_RuntimeError, "_tealet module state unavailable");
        return NULL;
    }
    return PyTealet_ThreadActive(mstate);
}

PyObject *PyTealetApi_ThreadKill(PyTealetModuleState *mstate, Py_ssize_t cleanup_passes, PyObject *kill_exc_spec) {
    if (!mstate) {
        PyErr_SetString(PyExc_RuntimeError, "_tealet module state unavailable");
        return NULL;
    }
    if (!kill_exc_spec)
        kill_exc_spec = Py_None;
    return PyTealet_ThreadKill(mstate, cleanup_passes, kill_exc_spec);
}

int PyTealetApi_ErrorWasRemote(PyTealetModuleState *mstate) {
    if (!mstate) {
        PyErr_SetString(PyExc_RuntimeError, "_tealet module state unavailable");
        return -1;
    }
    return PyTealet_ErrorWasRemote(mstate) ? 1 : 0;
}

int PyTealetApi_FrameIntrospectionGet(PyTealetModuleState *mstate) {
    if (!mstate) {
        PyErr_SetString(PyExc_RuntimeError, "_tealet module state unavailable");
        return -1;
    }
    return mstate->frame_introspection_enabled != 0;
}

int PyTealetApi_FrameIntrospectionSet(PyTealetModuleState *mstate, int enabled) {
    if (!mstate) {
        PyErr_SetString(PyExc_RuntimeError, "_tealet module state unavailable");
        return -1;
    }

    enabled = enabled ? 1 : 0;

#if !PYTEALET_WITH_PENDING_FRAME_INTROSPECTION
    if (enabled) {
        PyErr_SetString(PyExc_RuntimeError, "pending frame introspection is compile-time disabled in this build");
        return -1;
    }
#endif

    mstate->frame_introspection_enabled = enabled;
    return mstate->frame_introspection_enabled != 0;
}

int PyTealetApi_IsForeign(PyTealetModuleState *mstate, PyObject *target_obj) {
    PyTealetObject *target;

    if (!mstate || !mstate->tealet_type) {
        PyErr_SetString(PyExc_RuntimeError, "_tealet module state unavailable");
        return -1;
    }
    if (!target_obj) {
        PyErr_SetString(PyExc_TypeError, "target must not be NULL");
        return -1;
    }
    if (!PyObject_TypeCheck(target_obj, mstate->tealet_type)) {
        PyErr_SetString(PyExc_TypeError, "target must be a _tealet.tealet instance");
        return -1;
    }

    target = (PyTealetObject *)target_obj;
    return target->owner_tid != PyThread_get_thread_ident();
}

int PyTealetApi_StateGet(PyTealetModuleState *mstate, PyObject *target_obj, PyTealet_State *state_out) {
    PyTealetObject *target;

    if (!mstate || !mstate->tealet_type) {
        PyErr_SetString(PyExc_RuntimeError, "_tealet module state unavailable");
        return -1;
    }
    if (!target_obj) {
        PyErr_SetString(PyExc_TypeError, "target must not be NULL");
        return -1;
    }
    if (!state_out) {
        PyErr_SetString(PyExc_TypeError, "state_out must not be NULL");
        return -1;
    }
    if (!PyObject_TypeCheck(target_obj, mstate->tealet_type)) {
        PyErr_SetString(PyExc_TypeError, "target must be a _tealet.tealet instance");
        return -1;
    }

    target = (PyTealetObject *)target_obj;
    *state_out = (PyTealet_State)target->state;
    return 0;
}

int PyTealetApi_ThreadIdGet(PyTealetModuleState *mstate, PyObject *target_obj, unsigned long *thread_id_out) {
    PyTealetObject *target;

    if (!mstate || !mstate->tealet_type) {
        PyErr_SetString(PyExc_RuntimeError, "_tealet module state unavailable");
        return -1;
    }
    if (!target_obj) {
        PyErr_SetString(PyExc_TypeError, "target must not be NULL");
        return -1;
    }
    if (!thread_id_out) {
        PyErr_SetString(PyExc_TypeError, "thread_id_out must not be NULL");
        return -1;
    }
    if (!PyObject_TypeCheck(target_obj, mstate->tealet_type)) {
        PyErr_SetString(PyExc_TypeError, "target must be a _tealet.tealet instance");
        return -1;
    }

    target = (PyTealetObject *)target_obj;
    *thread_id_out = target->owner_tid;
    return 0;
}

/* Raise a structured thread-mismatch exception that includes owner metadata.
 * If exception construction fails, propagate the underlying failure.
 */
static int pytealet_raise_thread_mismatch(PyTealetModuleState *mstate, const char *operation, unsigned long current_tid,
                                          unsigned long target_tid) {
    PyObject *err_type;
    PyObject *msg = NULL;
    PyObject *exc = NULL;
    PyObject *attr = NULL;
    int target_alive = 0;
    const char *op_name = operation ? operation : "operation";

    assert(mstate);

    (void)PyTealet_LineageThreadIdentIsAlive(target_tid, &target_alive);
    err_type = mstate->thread_mismatch_error;
    if (!err_type) {
        PyErr_SetString(PyExc_RuntimeError, "ThreadMismatchError is not initialized");
        return -1;
    }

    msg = PyUnicode_FromFormat("thread mismatch: %s not allowed from a different thread "
                               "(current=%lu, target=%lu, target_alive=%s)",
                               op_name, current_tid, target_tid, target_alive ? "True" : "False");
    if (!msg)
        return -1;

    exc = PyObject_CallOneArg(err_type, msg);
    Py_DECREF(msg);
    msg = NULL;
    if (!exc)
        return -1;

    attr = PyLong_FromUnsignedLong(current_tid);
    if (!attr || PyObject_SetAttrString(exc, "current_tid", attr) < 0)
        goto error;
    Py_DECREF(attr);
    attr = NULL;

    attr = PyLong_FromUnsignedLong(target_tid);
    if (!attr || PyObject_SetAttrString(exc, "target_tid", attr) < 0)
        goto error;
    Py_DECREF(attr);
    attr = NULL;

    attr = PyBool_FromLong(target_alive);
    if (!attr || PyObject_SetAttrString(exc, "target_alive", attr) < 0)
        goto error;
    Py_DECREF(attr);
    attr = NULL;

    attr = PyUnicode_FromString(op_name);
    if (!attr || PyObject_SetAttrString(exc, "operation", attr) < 0)
        goto error;
    Py_DECREF(attr);
    attr = NULL;

    PyErr_SetObject(err_type, exc);
    Py_DECREF(exc);
    return -1;

error:
    Py_XDECREF(attr);
    Py_XDECREF(exc);
    return -1;
}

/* check if a target tealet is valid, compared to a reference one.
 * we primarily use the thread_ids stored on the objects but
 * also assert the main line relationship
 */
static int CheckTarget(PyTealetModuleState *mstate, PyTealetObject *target, PyTealetObject *ref,
                       const char *operation) {
    assert(target);

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
    return pytealet_raise_thread_mismatch(mstate, operation, (unsigned long)PyThread_get_thread_ident(),
                                          target->owner_tid);
}

/* ===================================================================== */
/* Core Runtime Switching Callback                                       */
/* ===================================================================== */

/* process return argument from callable and convert.  We unpack arguments
 * and validate, and treat validation errors as an exception
 * return new references to return_to and return_arg.
 * This runs before the exit-switch safety boundary, so direct DECREF is fine.
 */
static void pytealet_process_return_arg(PyTealetModuleState *mstate, PyTealetObject *current, PyObject *result,
                                        PyTealetObject **return_to, PyObject **return_arg, PyObject **return_exc) {
    int err = 0;
    *return_to = NULL;
    *return_arg = NULL;
    *return_exc = NULL;
    if (result) {
        if (PyTuple_Check(result)) {
            /* arg and return_to are borrowed refs */
            if (PyTuple_GET_SIZE(result) > 0)
                *return_to = (PyTealetObject *)Py_NewRef(PyTuple_GET_ITEM(result, 0));
            if (PyTuple_GET_SIZE(result) > 1)
                *return_arg = Py_NewRef(PyTuple_GET_ITEM(result, 1));
        } else {
            *return_to = (PyTealetObject *)Py_NewRef(result);
        }

        /* perform sanity checks on the return_to target */

        if (!*return_to) {
            PyErr_SetString(PyExc_TypeError, "tealet object expected");
            err = -1;
        } else if (!PyTealet_Check((PyObject *)(*return_to), mstate)) {
            PyErr_SetString(PyExc_TypeError, "tealet object expected");
            err = -1;
        } else if ((*return_to)->state != STATE_RUN) {
            PyErr_SetString(mstate->state_error, "must be 'run'");
            err = -1;
        } else if (CheckTarget(mstate, *return_to, current, "return")) {
            err = -1;
        }
        if (err) {
            Py_XDECREF(*return_arg);
            *return_arg = NULL;
            Py_XDECREF((PyObject *)(*return_to));
            *return_to = NULL;
        }
    } else {
        err = -1;
    }
    if (!*return_to) {
        *return_to = (PyTealetObject *)Py_NewRef(TryGetMain(mstate, NULL));
        assert(*return_to);
    }
    if (!*return_arg) {
        *return_arg = Py_NewRef(Py_None);
    }
    if (err) {
        *return_exc = PyTealetThrow_GetRaisedException();
    }
}

/* Handle uncaught top-level exceptions from a tealet worker.
 * - TealetExit is swallowed.
 * - SystemExit/KeyboardInterrupt are redirected to main and left in *exc_io
 *   so caller can schedule deferred re-raise after switch.
 * - all other exceptions are reported as unhandled.
 */
static void pytealet_handle_top_level_exception(PyTealetModuleState *mstate, PyTealetObject **return_to_io,
                                                PyObject **exc_io) {
    PyObject *exc;
    assert(mstate);
    assert(return_to_io && *return_to_io != NULL);
    assert(exc_io);

    exc = *exc_io;
    if (!exc)
        return;

    if (mstate->tealet_exit_error && PyErr_GivenExceptionMatches(exc, mstate->tealet_exit_error)) {
        Py_DECREF(exc);
        *exc_io = NULL;
        return;
    }

    if (PyErr_GivenExceptionMatches(exc, PyExc_SystemExit) ||
        PyErr_GivenExceptionMatches(exc, PyExc_KeyboardInterrupt)) {
        PyTealetObject *main_t;

        main_t = TryGetMain(mstate, NULL);
        assert(main_t);
        if (*return_to_io != main_t) {
            Py_DECREF(*return_to_io);
            *return_to_io = (PyTealetObject *)Py_NewRef((PyObject *)main_t);
        }
        return;
    }

    PyTealetThrow_SetRaisedException(exc);
    *exc_io = NULL;
    PyErr_WriteUnraisable(NULL);
    PyErr_Clear();
}

/* The main function.  Invoked either from tealet.new or tealet.run */
static tealet_t *pytealet_main(tealet_t *t_current, void *arg) {
    PyTealetNewArg *targ = (PyTealetNewArg *)arg;
    PyTealetModuleState *mstate = targ->mstate;
    PyTealetObject *tealet = targ->dest;
    PyObject *func = targ->func;
    PyTealetApi_RunCFunc cfunc = targ->cfunc;
    PyObject *farg = targ->arg;
    PyTealetObject *return_to;
    PyObject *result, *return_arg;
    PyObject *return_exc;
    tealet_t *t_return;
    PyTealetMainData *mdata;
    int exit_mode = TEALET_EXIT_DELETE;
    PyThreadState *tstate = PyThreadState_GET();

    mdata = (PyTealetMainData *)*tealet_main_userpointer(t_current->main);
    assert(mdata);

    if (tealet->state == STATE_STUB) {
        assert(t_current == tealet->tealet);
        assert(TEALET_PYOBJECT(t_current) == tealet);

        /* set the tstate from our own copy.  This includes the context. */
        PyTealetTstate_Restore(&tealet->tstate, tstate);
    } else {
        assert(tealet->state == STATE_NEW);
        /* set up initial frame data for the tealet.  This must happen before any allocations
         * that can cause, e.g. via garbage collection, python code to run in this context.*/
        PyTealetTstate_Frame_Setup(&tealet->tstate, tstate, 1);
        
        /* Publish wrapper<->tealet linkage under lineage lock. */
        pytealet_domain_lock(mdata);
        tealet->tealet = t_current;
        TEALET_SET_PYOBJECT(t_current, tealet);
        if (pytealet_track_wrapper(mdata, tealet, 1) < 0) {
            PyErr_WriteUnraisable(Py_None);
            PyErr_Clear();
        }
        pytealet_domain_unlock(mdata);

        /* set the context of the freshly running tealet by moving it out
         * of the tealet's tstate and into the python thread state.
         */
        PyObject *new_ctx = tealet->tstate.context;
        PyObject *old_ctx = tstate->context;
        tstate->context = new_ctx;
        tealet->tstate.context = NULL; /* ownership transferred to tstate */
        tstate->context_ver++;
        Py_XDECREF(old_ctx);
    }

    /* We only have borrowed references from the calling tealet.
     * Keep tealet alive for the full callback lifetime because caller teardown
     * can drop borrowed owners. Hold explicit refs to worker args around the
     * callback dispatch regardless of Python-callable or native C mode.
     */
    Py_INCREF(tealet);

    /* run the tealet function */
    tealet->state = STATE_RUN;
    /* Deliver any pending injected exception at run entry, before worker call. */
    result = PyTealetThrow_MaybeRaisePending(mdata, tealet, Py_NewRef(Py_None));
    if (result) {
        Py_DECREF(result);
        if (cfunc == NULL) {
            assert(func != NULL);
            Py_INCREF(func);
            Py_INCREF(farg);
            result = PyObject_CallFunctionObjArgs(func, tealet, farg, NULL);
            Py_DECREF(func);
            Py_DECREF(farg);
        } else {
            assert(func == NULL);
            Py_INCREF(farg);
            result = cfunc((PyObject *)tealet, farg);
            Py_DECREF(farg);
        }
    }

    pytealet_process_return_arg(mstate, tealet, result, &return_to, &return_arg, &return_exc);
    Py_XDECREF(result);

    /* see if we should redirect the exit switch due to an exception, and clear up the pending token.
     * return_to is null on entry if there was an exception, on exit we own an reference
     * to the target, possibly main
     */
    (void)PyTealetThrow_RedirectUncaught(mstate, mdata, tealet, return_exc, &return_to);

    /* Classify top-level worker exceptions; fatal ones are deferred and
     * injected into the return target after this switch.
     */
    pytealet_handle_top_level_exception(mstate, &return_to, &return_exc);
    if (return_exc) {
        if (pytealet_set_exception_inner(mstate, return_to, tealet, mdata, return_exc, Py_None) < 0) {
            /* If deferred delivery setup fails, at least report the original
             * fatal exception rather than dropping it silently.
             */
            PyErr_Clear();
            PyTealetThrow_SetRaisedException(return_exc);
            PyErr_WriteUnraisable(NULL);
            PyErr_Clear();
        }
        Py_DECREF(return_exc);
        return_exc = NULL;
    }

    /* Now we have started the exit process, already possibly setting an exception on the target.
     * we must be careful not to do anything that might cause arbitrary control flow, such as
     * triggering object deletion (which can invoke __del__ methods.), so all decrefs must
     * be via the dustbin at this point.*/

    /* clear the old tealet */
    tealet->state = STATE_EXIT;
    /* Stop tracking this wrapper while lineage pointers are still valid.
     * If we wait until object dealloc, tealet may already be NULL and the
     * weakref can remain stranded in mdata->wrappers until thread_reap().
     * This call is safe, we drop weakref objects and no arbitrary __del__ code can run.
     */
    pytealet_domain_lock(mdata);
    pytealet_untrack_wrapper(tealet, 1);
    if (PYTEALET_DEFER_DELETE)
        exit_mode = TEALET_XFER_DEFAULT;
    if (exit_mode == TEALET_EXIT_DELETE) {
        tealet->tealet = NULL; /* will be auto-deleted on return */
        TEALET_SET_PYOBJECT(t_current, NULL);
    }
    pytealet_domain_unlock(mdata);
    t_return = return_to->tealet;

    /* decref the objects after the switch */
    PyTealet_dustbin_push(t_return, (PyObject *)tealet);
    PyTealet_dustbin_push(t_return, (PyObject *)return_to);

    /* Tealet is exiting permanently: clear active PyThreadState for the switch,
     * then drop saved refs immediately so frame locals (including 'current')
     * do not keep the Python tealet object alive until GC.
     * keep the context object valid after drop *
     */
    PyTealetTstate_Frame_Cleanup(tstate, t_return);
    PyTealetTstate_Save(&tealet->tstate, tstate);
    PyTealetTstate_Drop(&tealet->tstate, t_return, 0);

    {
        int exit_fail;
        exit_fail = tealet_exit(t_return, (void *)return_arg, exit_mode | TEALET_XFER_NOFAIL);
        if (exit_fail) {
            /* we have hit an impossible error. restore the frame for the machinery. */
            PyTealetTstate_Frame_Setup(&tealet->tstate, tstate, 1);
            PyTealet_TranslateTealetError(mstate, exit_fail, "tealet exit failed", NULL, NULL);
            PyErr_WriteUnraisable(NULL);
            abort();
        }
    }
    /* never reach here */
    return 0;
}

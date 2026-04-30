
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
#include "pytealet.h"
#include "frame_info.h"
#include "tstate_state.h"

/* ===================================================================== */
/* Compile-Time Version Feature Flags                                    */
/* ===================================================================== */

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


/* The python tealet object */
struct PyTealetObject {
    PyObject_HEAD int state;
    tealet_t *tealet;
    PyObject *weakreflist; /* List of weak references */

    /* thread state information */
    PyTealetTstate tstate;
    /* Dormant frame snapshot and (3.12+) reversible frame rewrites. */
    PyTealetFrameInfo frame_info;
};

/* helpers for getting main and current and checking relationship */
static PyTealetModuleState *GetModuleStateFromClass(PyTypeObject *cls);
static PyTealetObject *GetMain(PyTealetModuleState *mstate, int create);
static PyTealetObject *GetCurrent(PyTealetModuleState *mstate, PyTealetObject *main, int create_main);
static int CheckTarget(PyTealetModuleState *mstate, PyTealetObject *target, PyTealetObject *main);

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

/* get the far pointer that we need at least ot store any stack based data
 * currently in the python tstate.  this varies by python version
 */

static void *PyTealet_GetStackFar(const PyThreadState *py_tstate) {
#if defined(PY_HAS_CFRAME) && !defined(Py311P)
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
    PyTealetFrameInfo_Init(&result->frame_info);
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
            PyTealetTstate_Duplicate(&result->tstate, &src->tstate);
            /* We don't capture frame info for stubs. */
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
    PyTealetFrameInfo_Release(&tealet->frame_info, NULL);
    PyTealetFrameInfo_Fini(&tealet->frame_info);
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

    PyTealetFrameInfo_Capture(&current->frame_info, 1);
    if (!created_from_new) {
        PyTealetTstate_Save(&current->tstate, tstate);
        fail = tealet_stub_run(target->tealet, pytealet_main, &switch_arg);
        PyTealetTstate_Restore(&current->tstate, tstate);
    } else {
        void *stack_limit = PyTealet_GetStackFar(tstate);
        PyTealetTstate_Copy(&current->tstate, tstate);
        tealet = tealet_new(current->tealet, pytealet_main, &switch_arg, stack_limit);
        fail = (tealet == NULL);
        if (fail) {
            PyTealetTstate_Drop(&current->tstate, NULL);
        } else {
            PyTealetTstate_Restore(&current->tstate, tstate);
        }
    }
    PyTealetFrameInfo_Release(&current->frame_info, NULL);
    if (fail) {
        PyErr_NoMemory();
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
    PyTealetFrameInfo_Capture(&current->frame_info, 1);
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
    PyObject *frame;
    PyTealetModuleState *mstate = GetModuleStateFromClass(Py_TYPE(self));
    if (!mstate)
        return NULL;
#if defined(PY_HAS_TSTATE_FRAME)
    frame = self->tstate.has_state ? (PyObject *)self->tstate.frame : NULL;
#else
    frame = PyTealetFrameInfo_GetFrame(&self->frame_info);
#endif
    if (!frame) {
        /* is it the current tealet of the current thread? */
        if (self == GetCurrent(mstate, NULL, 0)) {
            frame = (PyObject *)PyEval_GetFrame();
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
#if defined(Py311P)
    PyTealetCFrame top_frame;
#endif

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

#if defined(Py311P)
    /* Entering tealet code must not inherit parent eval/datastack links from
     * another C stack.  We copy the cframe into a local variable and reset it so that
     * it has no parents.
     */
    top_frame = tstate->root_cframe;
    tstate->cframe = &top_frame;
    tstate->cframe->previous = &tstate->root_cframe;
    tstate->cframe->current_frame = NULL;
    tstate->datastack_chunk = NULL;
    tstate->datastack_top = NULL;
    tstate->datastack_limit = NULL;
#endif
#if defined(PY_HAS_TSTATE_FRAME)
    /* 3.10: start tealet execution with no inherited Python frame chain. */
    tstate->frame = NULL;
#endif

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
    PyTealet_dustbin_push(t_return, func);
    PyTealet_dustbin_push(t_return, (PyObject *)tealet);
    PyTealet_dustbin_push(t_return, result);

    Py_INCREF(return_arg);

    /* Tealet is exiting permanently: clear active PyThreadState for the switch,
     * then drop saved refs immediately so frame locals (including 'current')
     * do not keep the Python tealet object alive until GC.
     */
    PyTealetFrameInfo_Capture(&tealet->frame_info, 1);
    PyTealetTstate_Save(&tealet->tstate, tstate);
    PyTealetFrameInfo_Release(&tealet->frame_info, t_return);
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

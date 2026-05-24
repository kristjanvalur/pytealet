/* pytealet_module.c - CPython module lifecycle for the _tealet extension.
 *
 * Implements module-level functions and module init/exec/traverse/clear/free
 * hooks, while delegating active runtime behavior to pytealet.c.
 */

#include "pytealet_module.h"

#include "descrobject.h"

#include <assert.h>
#include <string.h>

typedef struct PyTealetDomainLockObject {
    PyObject_HEAD
#if PYTEALET_FREE_THREADED
    PyThread_type_lock lock;
#endif
} PyTealetDomainLockObject;

static void pytealet_domain_lock_obj_dealloc(PyObject *obj) {
    PyTealetDomainLockObject *self = (PyTealetDomainLockObject *)obj;
#if PYTEALET_FREE_THREADED
    if (self->lock) {
        PyThread_free_lock(self->lock);
        self->lock = NULL;
    }
#endif
    Py_TYPE(obj)->tp_free(obj);
}

static PyTypeObject pytealet_domain_lock_type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "_tealet._DomainLock",
    .tp_basicsize = sizeof(PyTealetDomainLockObject),
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_dealloc = pytealet_domain_lock_obj_dealloc,
};

PyObject *pytealet_domain_lock_obj_new(void) {
    PyTealetDomainLockObject *obj;

    assert((pytealet_domain_lock_type.tp_flags & Py_TPFLAGS_READY) != 0);

    obj = (PyTealetDomainLockObject *)PyObject_New(PyTealetDomainLockObject, &pytealet_domain_lock_type);
    if (!obj)
        return NULL;
#if PYTEALET_FREE_THREADED
    obj->lock = PyThread_allocate_lock();
    if (!obj->lock) {
        Py_DECREF((PyObject *)obj);
        return PyErr_NoMemory();
    }
#endif
    return (PyObject *)obj;
}

void pytealet_domain_lock_obj_lock(PyObject *domain_lock_obj) {
    assert(domain_lock_obj != NULL);
#if PYTEALET_FREE_THREADED
    PyTealetDomainLockObject *lock_obj = (PyTealetDomainLockObject *)domain_lock_obj;
    assert(lock_obj);
    assert(lock_obj->lock);
    PyThread_acquire_lock(lock_obj->lock, WAIT_LOCK);
#else
    (void)domain_lock_obj;
#endif
}

void pytealet_domain_lock_obj_unlock(PyObject *domain_lock_obj) {
    assert(domain_lock_obj != NULL);
#if PYTEALET_FREE_THREADED
    PyTealetDomainLockObject *lock_obj = (PyTealetDomainLockObject *)domain_lock_obj;
    assert(lock_obj);
    assert(lock_obj->lock);
    PyThread_release_lock(lock_obj->lock);
#else
    (void)domain_lock_obj;
#endif
}

static PyObject *panic_error_get_slot(PyObject *self, const char *name) {
    PyObject *value = PyObject_GetAttrString(self, name);
    if (!value) {
        if (PyErr_ExceptionMatches(PyExc_AttributeError)) {
            PyErr_Clear();
            Py_RETURN_NONE;
        }
        return NULL;
    }
    return value;
}

static PyObject *panic_error_exception(PyObject *self, PyObject *Py_UNUSED(_ignored)) {
    return panic_error_get_slot(self, "_exception");
}

static PyObject *panic_error_result(PyObject *self, PyObject *Py_UNUSED(_ignored)) {
    PyObject *exc = panic_error_get_slot(self, "_exception");
    PyObject *result;

    if (!exc)
        return NULL;

    if (exc != Py_None) {
        if (!PyExceptionInstance_Check(exc)) {
            Py_DECREF(exc);
            PyErr_SetString(PyExc_TypeError, "PanicError internal _exception must be an exception instance or None");
            return NULL;
        }
        PyErr_SetObject((PyObject *)Py_TYPE(exc), exc);
        Py_DECREF(exc);
        return NULL;
    }
    Py_DECREF(exc);

    result = panic_error_get_slot(self, "_result");
    if (!result)
        return NULL;
    return result;
}

static PyMethodDef panic_error_result_method = {
    "result",
    (PyCFunction)panic_error_result,
    METH_NOARGS,
    "Return panic payload, or raise stored remote exception if present.",
};

static PyMethodDef panic_error_exception_method = {
    "exception",
    (PyCFunction)panic_error_exception,
    METH_NOARGS,
    "Return stored remote exception instance or None.",
};

static int panic_error_install_methods(PyObject *panic_error_type_obj) {
    PyTypeObject *panic_type;
    PyObject *descriptor;

    if (!PyType_Check(panic_error_type_obj)) {
        PyErr_SetString(PyExc_TypeError, "PanicError object is not a type");
        return -1;
    }
    panic_type = (PyTypeObject *)panic_error_type_obj;

    descriptor = PyDescr_NewMethod(panic_type, &panic_error_result_method);
    if (!descriptor)
        return -1;
    if (PyObject_SetAttrString(panic_error_type_obj, "result", descriptor) < 0) {
        Py_DECREF(descriptor);
        return -1;
    }
    Py_DECREF(descriptor);

    descriptor = PyDescr_NewMethod(panic_type, &panic_error_exception_method);
    if (!descriptor)
        return -1;
    if (PyObject_SetAttrString(panic_error_type_obj, "exception", descriptor) < 0) {
        Py_DECREF(descriptor);
        return -1;
    }
    Py_DECREF(descriptor);

    return 0;
}

static PyObject *module_current(PyObject *mod, PyObject *Py_UNUSED(_ignored)) {
    PyTealetModuleState *mstate = (PyTealetModuleState *)PyModule_GetState(mod);
    if (!mstate) {
        PyErr_SetString(PyExc_RuntimeError, "_tealet module state unavailable");
        return NULL;
    }
    /* get the current.  if there is no main tealet at this time, create it. */
    return Py_XNewRef((PyObject *)GetCurrent(mstate, NULL, 1, NULL));
}

static PyObject *module_main(PyObject *mod, PyObject *Py_UNUSED(_ignored)) {
    PyTealetModuleState *mstate = (PyTealetModuleState *)PyModule_GetState(mod);
    if (!mstate) {
        PyErr_SetString(PyExc_RuntimeError, "_tealet module state unavailable");
        return NULL;
    }
    /* create main if it doesn't already exist for this thread */
    return Py_XNewRef((PyObject *)GetMain(mstate, 1, NULL));
}

static PyObject *module_thread_cleanup(PyObject *mod, PyObject *args) {
    PyTealetModuleState *mstate = (PyTealetModuleState *)PyModule_GetState(mod);
    Py_ssize_t cleanup_passes = 3;
    PyObject *kill_exc = Py_None;
    if (!mstate) {
        PyErr_SetString(PyExc_RuntimeError, "_tealet module state unavailable");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "|nO:thread_cleanup", &cleanup_passes, &kill_exc))
        return NULL;
    return PyTealet_ThreadCleanup(mstate, cleanup_passes, kill_exc);
}

static PyObject *module_active_tealets(PyObject *mod, PyObject *Py_UNUSED(_ignored)) {
    PyTealetModuleState *mstate = (PyTealetModuleState *)PyModule_GetState(mod);
    if (!mstate) {
        PyErr_SetString(PyExc_RuntimeError, "_tealet module state unavailable");
        return NULL;
    }
    return PyTealet_ActiveTealets(mstate);
}

static PyObject *module_thread_kill(PyObject *mod, PyObject *args) {
    PyTealetModuleState *mstate = (PyTealetModuleState *)PyModule_GetState(mod);
    Py_ssize_t cleanup_passes = 3;
    PyObject *kill_exc = Py_None;

    if (!mstate) {
        PyErr_SetString(PyExc_RuntimeError, "_tealet module state unavailable");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "|nO:thread_kill", &cleanup_passes, &kill_exc))
        return NULL;

    return PyTealet_ThreadKill(mstate, cleanup_passes, kill_exc);
}

static PyObject *module_error_was_remote(PyObject *mod, PyObject *Py_UNUSED(_ignored)) {
    PyTealetModuleState *mstate = (PyTealetModuleState *)PyModule_GetState(mod);
    if (!mstate) {
        PyErr_SetString(PyExc_RuntimeError, "_tealet module state unavailable");
        return NULL;
    }
    return PyBool_FromLong(PyTealet_ErrorWasRemote(mstate));
}

static PyObject *module_hide_frame(PyObject *mod, PyObject *args) {
    PyObject *func;
    PyObject *func_args = NULL;
    PyObject *kwds = NULL;
    PyObject *result;
    int created_empty_args = 0;
    PyThreadState *tstate = PyThreadState_GET();
#if defined(PY_HAS_TSTATE_FRAME)
    PyFrameObject *saved_frame = tstate->frame;
#endif
#if defined(PY_HAS_TSTATE_CFRAME) && defined(PY_HAS_TSTATE_DATASTACK)
    void *saved_current_frame = tstate->cframe ? (void *)tstate->cframe->current_frame : NULL;
#elif defined(PY_HAS_TSTATE_CURRENT_FRAME)
    void *saved_current_frame = (void *)tstate->current_frame;
#endif

    (void)mod;

    /* Calls callable(*args, **kwds) while hiding trampoline/caller frames to
     * improve greenlet traceback compatibility. We always restore the parent
     * frame linkage before returning, including when PyObject_Call fails and
     * propagates an exception.
     */
    if (!PyArg_ParseTuple(args, "O|OO:hide_frame", &func, &func_args, &kwds))
        return NULL;

    if (!func_args) {
        func_args = PyTuple_New(0);
        if (!func_args)
            return NULL;
        created_empty_args = 1;
    } else if (!PyTuple_Check(func_args)) {
        PyErr_SetString(PyExc_TypeError, "hide_frame() args must be a tuple");
        return NULL;
    }

#if defined(PY_HAS_TSTATE_FRAME)
    tstate->frame = NULL;
#endif
#if defined(PY_HAS_TSTATE_CFRAME) && defined(PY_HAS_TSTATE_DATASTACK)
    if (tstate->cframe)
        tstate->cframe->current_frame = NULL;
#elif defined(PY_HAS_TSTATE_CURRENT_FRAME)
    tstate->current_frame = NULL;
#endif

    result = PyObject_Call(func, func_args, kwds);

#if defined(PY_HAS_TSTATE_FRAME)
    tstate->frame = saved_frame;
#endif
#if defined(PY_HAS_TSTATE_CFRAME) && defined(PY_HAS_TSTATE_DATASTACK)
    if (tstate->cframe)
        tstate->cframe->current_frame = saved_current_frame;
#elif defined(PY_HAS_TSTATE_CURRENT_FRAME)
    tstate->current_frame = saved_current_frame;
#endif

    if (created_empty_args)
        Py_DECREF(func_args);
    return result;
}

/* Get/set dormant tealet frame introspection at runtime.
 * - frame_introspection() -> bool
 * - frame_introspection(enabled) -> bool
 */
static PyObject *module_frame_introspection(PyObject *mod, PyObject *args) {
    PyTealetModuleState *mstate = (PyTealetModuleState *)PyModule_GetState(mod);
    Py_ssize_t nargs;
    int enabled;

    if (!mstate) {
        PyErr_SetString(PyExc_RuntimeError, "_tealet module state unavailable");
        return NULL;
    }

    nargs = args ? PyTuple_GET_SIZE(args) : 0;
    if (nargs == 0)
        return PyBool_FromLong(mstate->frame_introspection_enabled != 0);
    if (nargs != 1) {
        PyErr_SetString(PyExc_TypeError, "frame_introspection() takes at most 1 argument");
        return NULL;
    }

    enabled = PyObject_IsTrue(PyTuple_GET_ITEM(args, 0));
    if (enabled < 0)
        return NULL;

#if !PYTEALET_WITH_PENDING_FRAME_INTROSPECTION
    if (enabled) {
        PyErr_SetString(PyExc_RuntimeError, "pending frame introspection is compile-time disabled in this build");
        return NULL;
    }
#endif

    mstate->frame_introspection_enabled = enabled;
    return PyBool_FromLong(mstate->frame_introspection_enabled != 0);
}

static PyMethodDef module_methods[] = {
    {"current", (PyCFunction)module_current, METH_NOARGS, ""},
    {"main", (PyCFunction)module_main, METH_NOARGS, ""},
    {"thread_cleanup", (PyCFunction)module_thread_cleanup, METH_VARARGS, ""},
    {"active_tealets", (PyCFunction)module_active_tealets, METH_NOARGS, ""},
    {"thread_kill", (PyCFunction)module_thread_kill, METH_VARARGS, ""},
    {"error_was_remote", (PyCFunction)module_error_was_remote, METH_NOARGS, ""},
    {"hide_frame", (PyCFunction)module_hide_frame, METH_VARARGS, ""},
    {"frame_introspection", (PyCFunction)module_frame_introspection, METH_VARARGS, ""},
    {NULL, NULL, 0, NULL} /* Sentinel */
};

static int pytealet_module_exec(PyObject *m) {
    PyTealetModuleState *mstate = (PyTealetModuleState *)PyModule_GetState(m);
    PyObject *type_obj;

    if (!mstate) {
        PyErr_SetString(PyExc_RuntimeError, "failed to get _tealet module state");
        return -1;
    }

    memset(&mstate->tls_key, 0, sizeof(mstate->tls_key));
    mstate->thread_data_lock = NULL;
    mstate->thread_data_ring = NULL;
    mstate->frame_introspection_enabled = PYTEALET_WITH_PENDING_FRAME_INTROSPECTION;
    mstate->tealet_type = NULL;
    mstate->tealet_error = NULL;
    mstate->invalid_error = NULL;
    mstate->state_error = NULL;
    mstate->defunct_error = NULL;
    mstate->panic_error = NULL;
    mstate->tealet_exit_error = NULL;

    /* Ready static helper types during module init so runtime allocation
     * paths remain lock-free and race-free under free-threaded execution.
     */
    if (PyType_Ready(&pytealet_domain_lock_type) < 0)
        return -1;

    if (!PyThread_tss_is_created(&mstate->tls_key)) {
        if (PyThread_tss_create(&mstate->tls_key) != 0) {
            PyErr_SetString(PyExc_RuntimeError, "failed to create thread-local key");
            return -1;
        }
    }

    mstate->thread_data_lock = PyThread_allocate_lock();
    if (!mstate->thread_data_lock) {
        PyErr_SetString(PyExc_RuntimeError, "failed to allocate thread-data lock");
        return -1;
    }

    type_obj = PyType_FromModuleAndSpec(m, &pytealet_type_spec, NULL);
    if (!type_obj)
        return -1;
    mstate->tealet_type = (PyTypeObject *)type_obj;
#if !defined(Py312P)
    mstate->tealet_type->tp_weaklistoffset = PyTealet_WeaklistOffset();
#endif
    if (PyModule_AddObjectRef(m, "tealet", type_obj) < 0) {
        Py_DECREF(type_obj);
        return -1;
    }
    Py_DECREF(type_obj);

    if (!GetMain(mstate, 1, NULL))
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

    mstate->panic_error = PyErr_NewException("_tealet.PanicError", mstate->tealet_error, NULL);
    if (!mstate->panic_error)
        return -1;
    if (panic_error_install_methods(mstate->panic_error) < 0)
        return -1;
    Py_INCREF(mstate->panic_error);
    if (PyModule_AddObject(m, "PanicError", mstate->panic_error) < 0)
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

    /* Control-flow exception for clean tealet termination from worker code. */
    mstate->tealet_exit_error = PyErr_NewException("_tealet.TealetExit", PyExc_BaseException, NULL);
    if (!mstate->tealet_exit_error)
        return -1;
    Py_INCREF(mstate->tealet_exit_error);
    if (PyModule_AddObject(m, "TealetExit", mstate->tealet_exit_error) < 0)
        return -1;

    PyModule_AddIntMacro(m, STATE_NEW);
    PyModule_AddIntMacro(m, STATE_STUB);
    PyModule_AddIntMacro(m, STATE_RUN);
    PyModule_AddIntMacro(m, STATE_EXIT);
    if (PyModule_AddIntConstant(m, "PYTEALET_DEFER_DELETE", PYTEALET_DEFER_DELETE) < 0)
        return -1;
    if (PyModule_AddIntConstant(m, "PYTEALET_WITH_PENDING_FRAME_INTROSPECTION",
                                PYTEALET_WITH_PENDING_FRAME_INTROSPECTION) < 0)
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
    Py_VISIT(mstate->panic_error);
    Py_VISIT(mstate->tealet_exit_error);
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
    Py_CLEAR(mstate->panic_error);
    Py_CLEAR(mstate->tealet_exit_error);
    mstate->tealet_type = NULL;
    return 0;
}

static void pytealet_module_free(void *m) {
    PyTealetModuleState *mstate = (PyTealetModuleState *)PyModule_GetState((PyObject *)m);
    PyTealetMainData *mdata;
    if (!mstate)
        return;

    /* Best-effort drain of per-thread lineages registered in this module.
     * We fetch one node at a time without holding the lock across cleanup,
     * because cleanup unlinks using the same module lock.
     */
    while (mstate->thread_data_lock) {
        PyThread_acquire_lock(mstate->thread_data_lock, WAIT_LOCK);
        mdata = mstate->thread_data_ring;
        PyThread_release_lock(mstate->thread_data_lock);
        if (!mdata)
            break;
        (void)PyTealet_ThreadCleanupMdataForTeardown(mstate, mdata);
    }

    if (PyThread_tss_is_created(&mstate->tls_key))
        PyThread_tss_delete(&mstate->tls_key);
    if (mstate->thread_data_lock) {
        PyThread_free_lock(mstate->thread_data_lock);
        mstate->thread_data_lock = NULL;
    }
    mstate->thread_data_ring = NULL;
}

/* CPython API uses void* in module slots; this conversion is intentional. */
#if defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#endif
static PyModuleDef_Slot _tealet_module_slots[] = {
    {Py_mod_exec, pytealet_module_exec},
#if defined(Py_mod_gil)
    {Py_mod_gil, Py_MOD_GIL_NOT_USED},
#endif
    {0, NULL}};
#if defined(__GNUC__)
#pragma GCC diagnostic pop
#endif

struct PyModuleDef _tealet_module = {PyModuleDef_HEAD_INIT,
                                     "_tealet", /* name of module */
                                     NULL,      /* module documentation, may be NULL */
                                     sizeof(PyTealetModuleState),
                                     module_methods,
                                     _tealet_module_slots,
                                     pytealet_module_traverse,
                                     pytealet_module_clear,
                                     pytealet_module_free};

PyMODINIT_FUNC PyInit__tealet(void) { return PyModuleDef_Init(&_tealet_module); }

/* pytealet_module.c - CPython module lifecycle for the _tealet extension.
 *
 * Implements module-level functions and module init/exec/traverse/clear/free
 * hooks, while delegating active runtime behavior to pytealet.c.
 */

#include "pytealet_module.h"

#include <string.h>

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

static PyObject *module_thread_cleanup(PyObject *mod, PyObject *Py_UNUSED(_ignored)) {
    PyTealetModuleState *mstate = (PyTealetModuleState *)PyModule_GetState(mod);
    if (!mstate) {
        PyErr_SetString(PyExc_RuntimeError, "_tealet module state unavailable");
        return NULL;
    }
    return PyTealet_ThreadCleanup(mstate);
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
    {"thread_cleanup", (PyCFunction)module_thread_cleanup, METH_NOARGS, ""},
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

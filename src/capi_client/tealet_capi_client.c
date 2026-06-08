/* pytealet_capi_client.c - validation client for the _tealet capsule C API.
 *
 * This extension acts as a downstream consumer of _tealet._C_API and is used
 * by tests to validate import/version checks and basic call paths.
 */

#include <Python.h>

#include "pytealet_capi.h"

#include <string.h>

typedef struct {
    const PyTealet_CAPI *api;
    PyTealet_CAPI_Context *ctx;
} PyTealetCapiClientState;

static PyTealetCapiClientState *client_get_state(PyObject *module) {
    PyTealetCapiClientState *state;

    if (!module || !PyModule_Check(module)) {
        PyErr_SetString(PyExc_RuntimeError, "invalid module object");
        return NULL;
    }

    state = (PyTealetCapiClientState *)PyModule_GetState(module);
    if (!state) {
        PyErr_SetString(PyExc_RuntimeError, "client module state unavailable");
        return NULL;
    }
    if (!state->api) {
        PyErr_SetString(PyExc_RuntimeError, "pytealet C API unavailable");
        return NULL;
    }

    return state;
}

static int client_ensure_ctx(PyTealetCapiClientState *state) {
    if (state->ctx)
        return 0;

    if (!state->api->ctx_new) {
        PyErr_SetString(PyExc_RuntimeError, "ctx_new missing from pytealet C API");
        return -1;
    }

    state->ctx = state->api->ctx_new();
    if (!state->ctx)
        return -1;

    return 0;
}

static PyObject *client_api_info(PyObject *module, PyObject *Py_UNUSED(_ignored)) {
    PyTealetCapiClientState *state = client_get_state(module);
    PyObject *d;

    if (!state)
        return NULL;

    d = PyDict_New();
    if (!d)
        return NULL;

    if (PyDict_SetItemString(d, "abi_version", PyLong_FromUnsignedLong(state->api->abi_version)) < 0)
        goto error;
    if (PyDict_SetItemString(d, "struct_size", PyLong_FromUnsignedLong(state->api->struct_size)) < 0)
        goto error;
    if (PyDict_SetItemString(d, "feature_flags", PyLong_FromUnsignedLongLong(state->api->feature_flags)) < 0)
        goto error;
    if (PyDict_SetItemString(d, "has_base",
                             PyBool_FromLong((state->api->feature_flags & PYTEALET_CAPI_FEATURE_BASE) != 0)) < 0)
        goto error;
    if (PyDict_SetItemString(d, "has_create",
                             PyBool_FromLong(state->api->create != NULL)) < 0)
        goto error;
    if (PyDict_SetItemString(d, "has_stub",
                             PyBool_FromLong(state->api->stub != NULL)) < 0)
        goto error;
    if (PyDict_SetItemString(d, "has_prepare",
                             PyBool_FromLong(state->api->prepare != NULL)) < 0)
        goto error;
    if (PyDict_SetItemString(d, "has_duplicate",
                             PyBool_FromLong(state->api->duplicate != NULL)) < 0)
        goto error;
    if (PyDict_SetItemString(d, "has_run",
                             PyBool_FromLong(state->api->run != NULL)) < 0)
        goto error;
    if (PyDict_SetItemString(d, "has_switch",
                             PyBool_FromLong(state->api->switch_ != NULL)) < 0)
        goto error;

    return d;

error:
    Py_DECREF(d);
    return NULL;
}

static PyObject *client_current_is_main(PyObject *module, PyObject *Py_UNUSED(_ignored)) {
    PyTealetCapiClientState *state = client_get_state(module);
    PyObject *current;
    PyObject *main;
    PyObject *result;

    if (!state)
        return NULL;
    if (client_ensure_ctx(state) < 0)
        return NULL;

    current = state->api->current(state->ctx);
    if (!current)
        return NULL;
    main = state->api->main(state->ctx);
    if (!main) {
        Py_DECREF(current);
        return NULL;
    }

    result = PyBool_FromLong(current == main);
    Py_DECREF(main);
    Py_DECREF(current);
    return result;
}

static PyObject *client_check_tealet(PyObject *module, PyObject *obj) {
    PyTealetCapiClientState *state = client_get_state(module);
    int rc;

    if (!state)
        return NULL;
    if (client_ensure_ctx(state) < 0)
        return NULL;

    rc = state->api->check_tealet(state->ctx, obj);
    if (rc < 0)
        return NULL;

    return PyBool_FromLong(rc != 0);
}

static PyObject *client_capi_switch(PyObject *module, PyObject *args) {
    PyTealetCapiClientState *state = client_get_state(module);
    Py_ssize_t nargs;
    PyObject *target;
    PyObject *arg = NULL;

    if (!state)
        return NULL;
    if (client_ensure_ctx(state) < 0)
        return NULL;

    nargs = PyTuple_GET_SIZE(args);
    if (nargs < 1 || nargs > 2) {
        PyErr_SetString(PyExc_TypeError, "capi_switch() takes 1 or 2 positional arguments");
        return NULL;
    }

    target = PyTuple_GET_ITEM(args, 0);
    if (nargs == 2)
        arg = PyTuple_GET_ITEM(args, 1);

    return state->api->switch_(state->ctx, target, arg);
}

static PyObject *client_capi_run(PyObject *module, PyObject *args) {
    PyTealetCapiClientState *state = client_get_state(module);
    Py_ssize_t nargs;
    PyObject *target;
    PyObject *func;
    PyObject *arg = NULL;

    if (!state)
        return NULL;
    if (client_ensure_ctx(state) < 0)
        return NULL;

    nargs = PyTuple_GET_SIZE(args);
    if (nargs < 2 || nargs > 3) {
        PyErr_SetString(PyExc_TypeError, "capi_run() takes 2 or 3 positional arguments");
        return NULL;
    }

    target = PyTuple_GET_ITEM(args, 0);
    func = PyTuple_GET_ITEM(args, 1);
    if (nargs == 3)
        arg = PyTuple_GET_ITEM(args, 2);

    return state->api->run(state->ctx, target, func, NULL, arg);
}

static PyObject *client_capi_prepare(PyObject *module, PyObject *args) {
    PyTealetCapiClientState *state = client_get_state(module);
    Py_ssize_t nargs;
    PyObject *target;
    PyObject *func;
    int rc;

    if (!state)
        return NULL;
    if (client_ensure_ctx(state) < 0)
        return NULL;

    nargs = PyTuple_GET_SIZE(args);
    if (nargs != 2) {
        PyErr_SetString(PyExc_TypeError, "capi_prepare() takes exactly 2 positional arguments");
        return NULL;
    }

    target = PyTuple_GET_ITEM(args, 0);
    func = PyTuple_GET_ITEM(args, 1);

    rc = state->api->prepare(state->ctx, target, func, NULL);
    if (rc < 0)
        return NULL;
    Py_RETURN_NONE;
}

static PyObject *client_capi_stub(PyObject *module, PyObject *target) {
    PyTealetCapiClientState *state = client_get_state(module);
    int rc;

    if (!state)
        return NULL;
    if (client_ensure_ctx(state) < 0)
        return NULL;

    rc = state->api->stub(state->ctx, target);
    if (rc < 0)
        return NULL;
    Py_RETURN_NONE;
}

static PyObject *client_capi_create(PyObject *module, PyObject *Py_UNUSED(_ignored)) {
    PyTealetCapiClientState *state = client_get_state(module);

    if (!state)
        return NULL;
    if (client_ensure_ctx(state) < 0)
        return NULL;

    return state->api->create(state->ctx);
}

static PyObject *client_capi_duplicate(PyObject *module, PyObject *source) {
    PyTealetCapiClientState *state = client_get_state(module);

    if (!state)
        return NULL;
    if (client_ensure_ctx(state) < 0)
        return NULL;

    return state->api->duplicate(state->ctx, source);
}

static PyObject *client_run_c_callback(PyObject *current, PyObject *arg) {
    PyObject *main_method;
    PyObject *main;
    PyObject *tag;
    PyObject *payload;
    PyObject *result;

    if (!current) {
        PyErr_SetString(PyExc_RuntimeError, "current tealet must not be NULL");
        return NULL;
    }
    if (!arg)
        arg = Py_None;

    main_method = PyObject_GetAttrString(current, "main");
    if (!main_method)
        return NULL;
    main = PyObject_CallNoArgs(main_method);
    Py_DECREF(main_method);
    if (!main)
        return NULL;

    tag = PyUnicode_FromString("via-capi-run-c");
    if (!tag) {
        Py_DECREF(main);
        return NULL;
    }

    payload = PyTuple_Pack(2, tag, arg);
    Py_DECREF(tag);
    if (!payload) {
        Py_DECREF(main);
        return NULL;
    }

    result = PyTuple_Pack(2, main, payload);
    Py_DECREF(payload);
    Py_DECREF(main);
    return result;
}

static PyObject *client_capi_run_c(PyObject *module, PyObject *args) {
    PyTealetCapiClientState *state = client_get_state(module);
    Py_ssize_t nargs;
    PyObject *target;
    PyObject *arg = NULL;

    if (!state)
        return NULL;
    if (client_ensure_ctx(state) < 0)
        return NULL;

    nargs = PyTuple_GET_SIZE(args);
    if (nargs < 1 || nargs > 2) {
        PyErr_SetString(PyExc_TypeError, "capi_run_c() takes 1 or 2 positional arguments");
        return NULL;
    }

    target = PyTuple_GET_ITEM(args, 0);
    if (nargs == 2)
        arg = PyTuple_GET_ITEM(args, 1);

    return state->api->run(state->ctx, target, NULL, client_run_c_callback, arg);
}

static PyObject *client_capi_prepare_c(PyObject *module, PyObject *target) {
    PyTealetCapiClientState *state = client_get_state(module);
    int rc;

    if (!state)
        return NULL;
    if (client_ensure_ctx(state) < 0)
        return NULL;

    rc = state->api->prepare(state->ctx, target, NULL, client_run_c_callback);
    if (rc < 0)
        return NULL;
    Py_RETURN_NONE;
}

static PyMethodDef client_methods[] = {
    {"api_info", (PyCFunction)client_api_info, METH_NOARGS, "Return imported pytealet C API metadata."},
    {"current_is_main", (PyCFunction)client_current_is_main, METH_NOARGS,
     "Return whether C API current() and main() refer to the same object."},
    {"check_tealet", (PyCFunction)client_check_tealet, METH_O,
     "Return True if object is a _tealet.tealet instance according to C API."},
    {"capi_create", (PyCFunction)client_capi_create, METH_NOARGS,
     "Create a brand-new tealet using the imported C API."},
    {"capi_stub", (PyCFunction)client_capi_stub, METH_O,
     "Stub a tealet using the imported C API."},
    {"capi_duplicate", (PyCFunction)client_capi_duplicate, METH_O,
     "Duplicate a tealet using the imported C API."},
    {"capi_prepare", (PyCFunction)client_capi_prepare, METH_VARARGS,
     "Prepare a tealet with a Python callable using the imported C API."},
    {"capi_prepare_c", (PyCFunction)client_capi_prepare_c, METH_O,
     "Prepare a tealet with a native C callback using the imported C API."},
    {"capi_run", (PyCFunction)client_capi_run, METH_VARARGS,
     "Run a tealet using the imported C API."},
    {"capi_run_c", (PyCFunction)client_capi_run_c, METH_VARARGS,
     "Run a tealet using a native C callback via the imported C API."},
    {"capi_switch", (PyCFunction)client_capi_switch, METH_VARARGS,
     "Switch to a tealet using the imported C API."},
    {NULL, NULL, 0, NULL},
};

static int client_exec(PyObject *module) {
    PyTealetCapiClientState *state = (PyTealetCapiClientState *)PyModule_GetState(module);

    if (!state) {
        PyErr_SetString(PyExc_RuntimeError, "client module state unavailable");
        return -1;
    }

    memset(state, 0, sizeof(*state));

    state->api = PyTealetApi_Import();
    if (!state->api)
        return -1;

    if (state->api->abi_version != PYTEALET_CAPI_ABI_VERSION) {
        PyErr_Format(PyExc_ImportError,
                     "pytealet C API ABI mismatch: expected %u, got %u",
                     (unsigned int)PYTEALET_CAPI_ABI_VERSION,
                     (unsigned int)state->api->abi_version);
        return -1;
    }

    if (state->api->struct_size < sizeof(PyTealet_CAPI)) {
        PyErr_SetString(PyExc_ImportError, "pytealet C API table is too small");
        return -1;
    }

    if (!state->api->ctx_new || !state->api->ctx_free || !state->api->current || !state->api->main ||
        !state->api->thread_sweep || !state->api->check_tealet || !state->api->create || !state->api->duplicate ||
        !state->api->stub || !state->api->prepare || !state->api->run || !state->api->switch_) {
        PyErr_SetString(PyExc_ImportError, "pytealet C API missing required functions");
        return -1;
    }

    state->ctx = state->api->ctx_new();
    if (!state->ctx)
        return -1;

    return 0;
}

static int client_clear(PyObject *module) {
    PyTealetCapiClientState *state = (PyTealetCapiClientState *)PyModule_GetState(module);

    if (!state)
        return 0;

    if (state->ctx && state->api && state->api->ctx_free) {
        state->api->ctx_free(state->ctx);
        state->ctx = NULL;
    }

    state->api = NULL;
    return 0;
}

static void client_free(void *module) {
    (void)client_clear((PyObject *)module);
}

/* CPython API uses void* in module slots; this conversion is intentional. */
#if defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#endif
static PyModuleDef_Slot client_slots[] = {{Py_mod_exec, client_exec},
#if defined(Py_mod_gil)
                                          {Py_mod_gil, Py_MOD_GIL_NOT_USED},
#endif
                                          {0, NULL}};
#if defined(__GNUC__)
#pragma GCC diagnostic pop
#endif

static struct PyModuleDef client_module = {
    PyModuleDef_HEAD_INIT,
    "_tealet_capi_client",
    NULL,
    sizeof(PyTealetCapiClientState),
    client_methods,
    client_slots,
    NULL,
    client_clear,
    client_free,
};

PyMODINIT_FUNC PyInit__tealet_capi_client(void) {
    return PyModuleDef_Init(&client_module);
}

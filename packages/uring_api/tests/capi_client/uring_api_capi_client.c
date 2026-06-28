/* uring_api_capi_client.c - validation client for the _uring_api capsule C API.
 *
 * This extension acts as a downstream consumer of _uring_api._C_API and is used
 * by tests to validate that native clients can call the public API.
 */

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include "uring_api_capi.h"

#ifndef _PyCFunction_CAST
#define _PyCFunction_CAST(func) ((PyCFunction)(void (*)(void))(func))
#endif

static const UringApi_CAPI *api = NULL;
static PyObject *callback_sink = NULL;

static int client_c_callback(PyObject *ring, PyObject *completion, void *user_data) {
    PyObject *sink = (PyObject *)user_data;

    (void)ring;
    if (!sink) {
        PyErr_SetString(PyExc_RuntimeError, "C callback sink is not set");
        return -1;
    }
    return PyList_Append(sink, completion);
}

static PyObject *client_metadata(PyObject *module, PyObject *Py_UNUSED(ignored)) {
    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    return Py_BuildValue("IIKII", api->abi_version, api->struct_size, (unsigned long long)api->feature_flags,
                         api->compiled_liburing_major, api->compiled_liburing_minor);
}

static PyObject *client_probe(PyObject *module, PyObject *Py_UNUSED(ignored)) {
    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    return api->probe(2, 0);
}

static PyObject *client_ring_summary(PyObject *module, PyObject *Py_UNUSED(ignored)) {
    PyObject *ring;
    PyObject *result;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    ring = api->ring_new(2, 0);
    if (!ring) {
        return NULL;
    }
    result = Py_BuildValue("iIIIIii", api->ring_check(ring), api->ring_fd(ring), api->ring_features(ring),
                           api->ring_sq_entries(ring), api->ring_cq_entries(ring), api->ring_closed(ring),
                           api->ring_running(ring));
    if (api->ring_close(ring) < 0) {
        Py_XDECREF(result);
        Py_DECREF(ring);
        return NULL;
    }
    Py_DECREF(ring);
    return result;
}

static PyObject *client_completion_summary(PyObject *module, PyObject *completion) {
    unsigned long long user_data;
    int res;
    unsigned int flags;
    PyObject *result;
    PyObject *summary;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (api->completion_check(completion) <= 0) {
        return NULL;
    }
    if (api->completion_user_data(completion, &user_data) < 0 || api->completion_res(completion, &res) < 0 ||
        api->completion_flags(completion, &flags) < 0) {
        return NULL;
    }
    result = api->completion_result(completion);
    if (!result) {
        return NULL;
    }
    summary = Py_BuildValue("KiIO", user_data, res, flags, result);
    Py_DECREF(result);
    return summary;
}

static PyObject *client_set_c_callback(PyObject *module, PyObject *args) {
    PyObject *ring;
    PyObject *sink;
    PyObject *old_sink;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "OO:set_c_callback", &ring, &sink)) {
        return NULL;
    }
    if (!PyList_Check(sink)) {
        PyErr_SetString(PyExc_TypeError, "sink must be a list");
        return NULL;
    }
    Py_INCREF(sink);
    old_sink = callback_sink;
    callback_sink = sink;
    if (api->ring_set_c_callback(ring, client_c_callback, callback_sink) < 0) {
        callback_sink = old_sink;
        Py_DECREF(sink);
        return NULL;
    }
    Py_XDECREF(old_sink);
    Py_RETURN_NONE;
}

static PyObject *client_clear_c_callback(PyObject *module, PyObject *ring) {
    PyObject *old_sink;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (api->ring_set_c_callback(ring, NULL, NULL) < 0) {
        return NULL;
    }
    old_sink = callback_sink;
    callback_sink = NULL;
    Py_XDECREF(old_sink);
    Py_RETURN_NONE;
}

static PyMethodDef client_methods[] = {
    {"metadata", (PyCFunction)client_metadata, METH_NOARGS, NULL},
    {"probe", (PyCFunction)client_probe, METH_NOARGS, NULL},
    {"ring_summary", (PyCFunction)client_ring_summary, METH_NOARGS, NULL},
    {"completion_summary", (PyCFunction)client_completion_summary, METH_O, NULL},
    {"set_c_callback", _PyCFunction_CAST(client_set_c_callback), METH_VARARGS, NULL},
    {"clear_c_callback", (PyCFunction)client_clear_c_callback, METH_O, NULL},
    {NULL, NULL, 0, NULL},
};

static int client_exec(PyObject *module) {
    (void)module;
    api = UringApi_Import();
    if (!api) {
        return -1;
    }
    if (api->abi_version != URING_API_CAPI_ABI_VERSION) {
        PyErr_SetString(PyExc_RuntimeError, "unexpected uring-api C API ABI version");
        return -1;
    }
    if ((api->feature_flags & URING_API_CAPI_FEATURE_PROBE) == 0 ||
        (api->feature_flags & URING_API_CAPI_FEATURE_RING) == 0 ||
        (api->feature_flags & URING_API_CAPI_FEATURE_C_CALLBACK) == 0 ||
        (api->feature_flags & URING_API_CAPI_FEATURE_COMPLETION) == 0) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API feature set is incomplete");
        return -1;
    }
    if (!api->probe || !api->ring_new || !api->ring_set_c_callback || !api->completion_result) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API function table is incomplete");
        return -1;
    }
    return 0;
}

static void client_free(void *module) {
    (void)module;
    Py_CLEAR(callback_sink);
}

static PyModuleDef_Slot client_slots[] = {
    {Py_mod_exec, client_exec},
#if defined(Py_mod_gil)
    {Py_mod_gil, Py_MOD_GIL_NOT_USED},
#endif
    {0, NULL},
};

static struct PyModuleDef client_module = {
    PyModuleDef_HEAD_INIT,
    "_uring_api_capi_test_client",
    "Test client for the uring-api C API.",
    0,
    client_methods,
    client_slots,
    NULL,
    NULL,
    client_free,
};

PyMODINIT_FUNC PyInit__uring_api_capi_test_client(void) { return PyModuleDef_Init(&client_module); }

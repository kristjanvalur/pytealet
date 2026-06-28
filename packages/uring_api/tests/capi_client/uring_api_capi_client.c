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

static PyObject *client_ring_summary(PyObject *module, PyObject *args) {
    PyObject *ring;
    PyObject *result;
    unsigned int flags = 0;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "|I:ring_summary", &flags)) {
        return NULL;
    }
    ring = api->ring_new(2, flags);
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
    PyObject *user_data;
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
    user_data = api->completion_user_data(completion);
    if (!user_data) {
        return NULL;
    }
    if (api->completion_res(completion, &res) < 0 || api->completion_flags(completion, &flags) < 0) {
        Py_DECREF(user_data);
        return NULL;
    }
    result = api->completion_result(completion);
    if (!result) {
        Py_DECREF(user_data);
        return NULL;
    }
    summary = Py_BuildValue("OiIO", user_data, res, flags, result);
    Py_DECREF(user_data);
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

static PyObject *client_serve_completions(PyObject *module, PyObject *ring) {
    (void)module;
    if (api->ring_serve_completions(ring) < 0) {
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *client_stop_serving(PyObject *module, PyObject *ring) {
    (void)module;
    if (api->ring_stop_serving(ring) < 0) {
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *client_reset_serving(PyObject *module, PyObject *ring) {
    (void)module;
    if (api->ring_reset_serving(ring) < 0) {
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *client_submit_recvmsg(PyObject *module, PyObject *args) {
    PyObject *ring;
    PyObject *buf;
    PyObject *user_data;
    int fd;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "OiOO:submit_recvmsg", &ring, &fd, &buf, &user_data)) {
        return NULL;
    }
    if (api->ring_submit_recvmsg(ring, fd, buf, user_data) < 0) {
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *client_submit_sendto(PyObject *module, PyObject *args) {
    PyObject *ring;
    PyObject *data;
    PyObject *address;
    PyObject *user_data;
    int fd;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "OiOOO:submit_sendto", &ring, &fd, &data, &address, &user_data)) {
        return NULL;
    }
    if (api->ring_submit_sendto(ring, fd, data, address, user_data) < 0) {
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *client_submit_accept(PyObject *module, PyObject *args) {
    PyObject *ring;
    PyObject *user_data;
    int fd;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "OiO:submit_accept", &ring, &fd, &user_data)) {
        return NULL;
    }
    if (api->ring_submit_accept(ring, fd, user_data) < 0) {
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *client_submit_connect(PyObject *module, PyObject *args) {
    PyObject *ring;
    PyObject *address;
    PyObject *user_data;
    int fd;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "OiOO:submit_connect", &ring, &fd, &address, &user_data)) {
        return NULL;
    }
    if (api->ring_submit_connect(ring, fd, address, user_data) < 0) {
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyMethodDef client_methods[] = {
    {"metadata", (PyCFunction)client_metadata, METH_NOARGS, NULL},
    {"probe", (PyCFunction)client_probe, METH_NOARGS, NULL},
    {"ring_summary", (PyCFunction)client_ring_summary, METH_VARARGS, NULL},
    {"completion_summary", (PyCFunction)client_completion_summary, METH_O, NULL},
    {"set_c_callback", _PyCFunction_CAST(client_set_c_callback), METH_VARARGS, NULL},
    {"clear_c_callback", (PyCFunction)client_clear_c_callback, METH_O, NULL},
    {"serve_completions", (PyCFunction)client_serve_completions, METH_O, NULL},
    {"stop_serving", (PyCFunction)client_stop_serving, METH_O, NULL},
    {"reset_serving", (PyCFunction)client_reset_serving, METH_O, NULL},
    {"submit_recvmsg", _PyCFunction_CAST(client_submit_recvmsg), METH_VARARGS, NULL},
    {"submit_sendto", _PyCFunction_CAST(client_submit_sendto), METH_VARARGS, NULL},
    {"submit_accept", _PyCFunction_CAST(client_submit_accept), METH_VARARGS, NULL},
    {"submit_connect", _PyCFunction_CAST(client_submit_connect), METH_VARARGS, NULL},
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
    if ((api->feature_flags & URING_API_CAPI_FEATURE_CORE) == 0) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API feature set is incomplete");
        return -1;
    }
    if (!api->probe || !api->ring_new || !api->ring_set_c_callback || !api->ring_serve_completions ||
        !api->ring_stop_serving || !api->ring_reset_serving || !api->completion_result ||
        !api->ring_submit_recvmsg || !api->ring_submit_sendto || !api->ring_submit_accept ||
        !api->ring_submit_connect) {
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

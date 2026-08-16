/* uring_api_capi_client.c - validation client for the _uring_api capsule C API.
 *
 * This extension acts as a downstream consumer of _uring_api._C_API and is used
 * by tests to validate that native clients can call the public API.
 */

#define PY_SSIZE_T_CLEAN
#include "uring_api_capi.h"
#include <Python.h>
#include <stddef.h>

#ifndef _PyCFunction_CAST
#define _PyCFunction_CAST(func) ((PyCFunction)(void (*)(void))(func))
#endif

/* Test helper: mirrors fdsize completion.result parsing in completion.c. */
extern int uring_api_statx_try_read_st_size(const void *buf, Py_ssize_t buflen, unsigned long long *size_out);

static const UringApi_CAPI *api = NULL;
static PyObject *callback_sink = NULL;

static PyObject *prepare_and_drop(PyObject *ring, PyObject *completion) {
    int prepared = 0;

    if (!completion) {
        return NULL;
    }
    if (api->ring_prepare(ring, completion, &prepared) < 0) {
        Py_DECREF(completion);
        return NULL;
    }
    Py_DECREF(completion);
    Py_RETURN_NONE;
}

static PyObject *prepare_nowait_and_drop(PyObject *ring, PyObject *completion) {
    if (!completion) {
        return NULL;
    }
    if (api->completion_set_nowait(completion, 1) < 0) {
        Py_DECREF(completion);
        return NULL;
    }
    return prepare_and_drop(ring, completion);
}

static int client_c_callback(PyObject *ring, PyObject *completions, void *user_data) {
    PyObject *sink = (PyObject *)user_data;
    Py_ssize_t index;
    Py_ssize_t count;

    (void)ring;
    count = PyList_GET_SIZE(completions);
    for (index = 0; index < count; index++) {
        PyObject *completion = PyList_GET_ITEM(completions, index);
        if (PyList_Append(sink, completion) < 0) {
            return -1;
        }
    }
    return 0;
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
    int kind;
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
    if (api->completion_res(completion, &res) < 0 || api->completion_flags(completion, &flags) < 0 ||
        api->completion_kind(completion, &kind) < 0) {
        Py_DECREF(user_data);
        return NULL;
    }
    result = api->completion_result(completion);
    if (!result) {
        Py_DECREF(user_data);
        return NULL;
    }
    summary = Py_BuildValue("OiiIO", user_data, kind, res, flags, result);
    Py_DECREF(user_data);
    Py_DECREF(result);
    return summary;
}

static PyObject *client_completion_sequence(PyObject *module, PyObject *completion) {
    unsigned long long sequence;
    int ret;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    ret = api->completion_sequence(completion, &sequence);
    if (ret < 0) {
        return NULL;
    }
    return PyLong_FromUnsignedLongLong(sequence);
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

static PyObject *client_prepare_recvmsg(PyObject *module, PyObject *args) {
    PyObject *ring;
    PyObject *buf;
    PyObject *user_data;
    unsigned int flags;
    int fd;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "OiOIO:prepare_recvmsg", &ring, &fd, &buf, &flags, &user_data)) {
        return NULL;
    }
    return prepare_and_drop(ring, api->ring_construct_recvmsg(ring, fd, buf, flags, user_data));
}

static PyObject *client_prepare_recv_multishot(PyObject *module, PyObject *args) {
    PyObject *ring;
    PyObject *buf_group;
    PyObject *user_data;
    int fd;
    unsigned int flags;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "OiOOI:prepare_recv_multishot", &ring, &fd, &buf_group, &user_data, &flags)) {
        return NULL;
    }
    return prepare_and_drop(ring, api->ring_construct_recv_multishot(ring, fd, buf_group, flags, user_data));
}

static PyObject *client_prepare_recv_buf(PyObject *module, PyObject *args) {
    PyObject *ring;
    PyObject *buf_group;
    PyObject *user_data;
    int fd;
    unsigned int flags;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "OiOOI:prepare_recv_buf", &ring, &fd, &buf_group, &user_data, &flags)) {
        return NULL;
    }
    return prepare_and_drop(ring, api->ring_construct_recv_buf(ring, fd, buf_group, flags, user_data));
}

static PyObject *client_prepare_sendto(PyObject *module, PyObject *args) {
    PyObject *ring;
    PyObject *data;
    PyObject *address;
    PyObject *user_data;
    int fd;
    unsigned int flags;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "OiOOIO:prepare_sendto", &ring, &fd, &data, &address, &flags, &user_data)) {
        return NULL;
    }
    return prepare_and_drop(ring, api->ring_construct_sendto(ring, fd, data, address, flags, user_data));
}

static PyObject *client_prepare_sendmsg(PyObject *module, PyObject *args) {
    PyObject *ring;
    PyObject *data;
    PyObject *address;
    PyObject *user_data;
    int fd;
    unsigned int flags;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "OiOOIO:prepare_sendmsg", &ring, &fd, &data, &address, &flags, &user_data)) {
        return NULL;
    }
    return prepare_and_drop(ring, api->ring_construct_sendmsg(ring, fd, data, address, flags, user_data));
}

static PyObject *client_prepare_sendmsg_zc(PyObject *module, PyObject *args) {
    PyObject *ring;
    PyObject *data;
    PyObject *address;
    PyObject *user_data;
    int fd;
    unsigned int flags;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "OiOOIO:prepare_sendmsg_zc", &ring, &fd, &data, &address, &flags, &user_data)) {
        return NULL;
    }
    return prepare_and_drop(ring, api->ring_construct_sendmsg_zc(ring, fd, data, address, flags, user_data));
}

static PyObject *client_prepare_send_zc(PyObject *module, PyObject *args) {
    PyObject *ring;
    PyObject *data;
    PyObject *user_data;
    int fd;
    unsigned int flags;
    unsigned int zc_flags;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "OiOIIO:prepare_send_zc", &ring, &fd, &data, &flags, &zc_flags, &user_data)) {
        return NULL;
    }
    return prepare_and_drop(ring, api->ring_construct_send_zc(ring, fd, data, flags, zc_flags, user_data));
}

static PyObject *client_prepare_accept(PyObject *module, PyObject *args) {
    PyObject *ring;
    PyObject *user_data;
    int fd;
    unsigned int flags = 0;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "OiO|I:prepare_accept", &ring, &fd, &user_data, &flags)) {
        return NULL;
    }
    return prepare_and_drop(ring, api->ring_construct_accept(ring, fd, flags, user_data));
}

static PyObject *client_prepare_accept_multishot(PyObject *module, PyObject *args) {
    PyObject *ring;
    PyObject *user_data;
    int fd;
    unsigned int flags = 0;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "OiO|I:prepare_accept_multishot", &ring, &fd, &user_data, &flags)) {
        return NULL;
    }
    return prepare_and_drop(ring, api->ring_construct_accept_multishot(ring, fd, flags, user_data));
}

static PyObject *client_prepare_connect(PyObject *module, PyObject *args) {
    PyObject *ring;
    PyObject *address;
    PyObject *user_data;
    int fd;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "OiOO:prepare_connect", &ring, &fd, &address, &user_data)) {
        return NULL;
    }
    return prepare_and_drop(ring, api->ring_construct_connect(ring, fd, address, user_data));
}

static PyObject *client_prepare_shutdown(PyObject *module, PyObject *args) {
    PyObject *ring;
    PyObject *user_data;
    int fd;
    int how;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "OiiO:prepare_shutdown", &ring, &fd, &how, &user_data)) {
        return NULL;
    }
    return prepare_and_drop(ring, api->ring_construct_shutdown(ring, fd, how, user_data));
}

static PyObject *client_prepare_close(PyObject *module, PyObject *args) {
    PyObject *ring;
    PyObject *user_data;
    int fd;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "OiO:prepare_close", &ring, &fd, &user_data)) {
        return NULL;
    }
    return prepare_and_drop(ring, api->ring_construct_close(ring, fd, user_data));
}

static PyObject *client_prepare_close_nowait(PyObject *module, PyObject *args) {
    PyObject *ring;
    int fd;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "Oi:prepare_close_nowait", &ring, &fd)) {
        return NULL;
    }
    return prepare_nowait_and_drop(ring, api->ring_construct_close(ring, fd, Py_None));
}

static PyObject *client_prepare_shutdown_nowait(PyObject *module, PyObject *args) {
    PyObject *ring;
    int fd;
    int how;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "Oii:prepare_shutdown_nowait", &ring, &fd, &how)) {
        return NULL;
    }
    return prepare_nowait_and_drop(ring, api->ring_construct_shutdown(ring, fd, how, Py_None));
}

static PyObject *client_prepare_cancel_nowait(PyObject *module, PyObject *args) {
    PyObject *ring;
    PyObject *target;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "OO:prepare_cancel_nowait", &ring, &target)) {
        return NULL;
    }
    return prepare_nowait_and_drop(ring, api->ring_construct_cancel(ring, target, Py_None));
}

static PyObject *client_prepare_poll_remove_nowait(PyObject *module, PyObject *args) {
    PyObject *ring;
    PyObject *target;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "OO:prepare_poll_remove_nowait", &ring, &target)) {
        return NULL;
    }
    return prepare_nowait_and_drop(ring, api->ring_construct_poll_remove(ring, target, Py_None));
}

static PyObject *client_prepare_read(PyObject *module, PyObject *args) {
    PyObject *ring;
    PyObject *buf;
    PyObject *user_data;
    int fd;
    unsigned long long offset;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "OiKOO:prepare_read", &ring, &fd, &offset, &buf, &user_data)) {
        return NULL;
    }
    return prepare_and_drop(ring, api->ring_construct_read(ring, fd, buf, offset, user_data));
}

static PyObject *client_prepare_write(PyObject *module, PyObject *args) {
    PyObject *ring;
    PyObject *data;
    PyObject *user_data;
    int fd;
    unsigned long long offset;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "OiKOO:prepare_write", &ring, &fd, &offset, &data, &user_data)) {
        return NULL;
    }
    return prepare_and_drop(ring, api->ring_construct_write(ring, fd, data, offset, user_data));
}

static PyObject *client_statx_st_size(PyObject *module, PyObject *buf) {
    unsigned long long size;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!api->statx_st_size) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API statx_st_size is unavailable");
        return NULL;
    }
    if (api->statx_st_size(buf, &size) < 0) {
        return NULL;
    }
    return PyLong_FromUnsignedLongLong(size);
}

static PyObject *client_statx_try_read_st_size(PyObject *module, PyObject *buf) {
    Py_buffer view;
    unsigned long long size;

    (void)module;
    if (PyObject_GetBuffer(buf, &view, PyBUF_SIMPLE) < 0) {
        return NULL;
    }
    if (!uring_api_statx_try_read_st_size(view.buf, view.len, &size)) {
        PyBuffer_Release(&view);
        Py_RETURN_NONE;
    }
    PyBuffer_Release(&view);
    return PyLong_FromUnsignedLongLong(size);
}

static PyObject *client_prepare_statx_fdsize(PyObject *module, PyObject *args) {
    PyObject *ring;
    PyObject *user_data;
    int fd;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "OiO:prepare_statx_fdsize", &ring, &fd, &user_data)) {
        return NULL;
    }
    return prepare_and_drop(ring, api->ring_construct_statx_fdsize(ring, fd, user_data));
}

static PyObject *client_prepare_statx(PyObject *module, PyObject *args) {
    PyObject *ring;
    PyObject *path;
    PyObject *buf;
    PyObject *user_data;
    int dfd;
    int flags;
    unsigned int mask;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "OiOiIOO:prepare_statx", &ring, &dfd, &path, &flags, &mask, &buf, &user_data)) {
        return NULL;
    }
    return prepare_and_drop(ring, api->ring_construct_statx(ring, dfd, path, flags, mask, buf, user_data));
}

static PyObject *client_prepare_openat(PyObject *module, PyObject *args) {
    PyObject *ring;
    PyObject *path;
    PyObject *user_data;
    int dfd;
    int flags;
    unsigned int mode;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "OiOiIO:prepare_openat", &ring, &dfd, &path, &flags, &mode, &user_data)) {
        return NULL;
    }
    return prepare_and_drop(ring, api->ring_construct_openat(ring, dfd, path, flags, mode, user_data));
}

static PyObject *client_prepare_socket(PyObject *module, PyObject *args) {
    PyObject *ring;
    PyObject *user_data;
    int domain;
    int type;
    int protocol;
    unsigned int flags;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "OiiiIO:prepare_socket", &ring, &domain, &type, &protocol, &flags, &user_data)) {
        return NULL;
    }
    return prepare_and_drop(ring, api->ring_construct_socket(ring, domain, type, protocol, flags, user_data));
}

static PyObject *client_prepare_poll(PyObject *module, PyObject *args) {
    PyObject *ring;
    PyObject *user_data;
    int fd;
    unsigned int mask;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "OiIO:prepare_poll", &ring, &fd, &mask, &user_data)) {
        return NULL;
    }
    return prepare_and_drop(ring, api->ring_construct_poll(ring, fd, mask, user_data));
}

static PyObject *client_prepare_poll_multishot(PyObject *module, PyObject *args) {
    PyObject *ring;
    PyObject *user_data;
    int fd;
    unsigned int mask;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "OiIO:prepare_poll_multishot", &ring, &fd, &mask, &user_data)) {
        return NULL;
    }
    return prepare_and_drop(ring, api->ring_construct_poll_multishot(ring, fd, mask, user_data));
}

static PyObject *client_prepare_poll_remove(PyObject *module, PyObject *args) {
    PyObject *ring;
    PyObject *target_completion;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "OO:prepare_poll_remove", &ring, &target_completion)) {
        return NULL;
    }
    return prepare_and_drop(ring, api->ring_construct_poll_remove(ring, target_completion, Py_None));
}

static PyObject *client_construct_send(PyObject *module, PyObject *args) {
    PyObject *ring;
    PyObject *data;
    PyObject *user_data;
    unsigned int flags;
    int fd;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!api->ring_construct_send) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API ring_construct_send is unavailable");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "OiOIO:construct_send", &ring, &fd, &data, &flags, &user_data)) {
        return NULL;
    }
    return api->ring_construct_send(ring, fd, data, flags, user_data);
}

static PyObject *client_construct_recv(PyObject *module, PyObject *args) {
    PyObject *ring;
    PyObject *buf;
    PyObject *user_data;
    unsigned int flags;
    int fd;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!api->ring_construct_recv) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API ring_construct_recv is unavailable");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "OiOIO:construct_recv", &ring, &fd, &buf, &flags, &user_data)) {
        return NULL;
    }
    return api->ring_construct_recv(ring, fd, buf, flags, user_data);
}

static PyObject *client_construct_read(PyObject *module, PyObject *args) {
    PyObject *ring;
    PyObject *buf;
    PyObject *user_data;
    int fd;
    unsigned long long offset;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!api->ring_construct_read) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API ring_construct_read is unavailable");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "OiKOO:construct_read", &ring, &fd, &offset, &buf, &user_data)) {
        return NULL;
    }
    return api->ring_construct_read(ring, fd, buf, offset, user_data);
}

static PyObject *client_construct_write(PyObject *module, PyObject *args) {
    PyObject *ring;
    PyObject *data;
    PyObject *user_data;
    int fd;
    unsigned long long offset;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!api->ring_construct_write) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API ring_construct_write is unavailable");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "OiKOO:construct_write", &ring, &fd, &offset, &data, &user_data)) {
        return NULL;
    }
    return api->ring_construct_write(ring, fd, data, offset, user_data);
}

static PyObject *client_construct_sendto(PyObject *module, PyObject *args) {
    PyObject *ring;
    PyObject *data;
    PyObject *address;
    PyObject *user_data;
    unsigned int flags;
    int fd;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!api->ring_construct_sendto) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API ring_construct_sendto is unavailable");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "OiOOIO:construct_sendto", &ring, &fd, &data, &address, &flags, &user_data)) {
        return NULL;
    }
    return api->ring_construct_sendto(ring, fd, data, address, flags, user_data);
}

static PyObject *client_construct_recvmsg(PyObject *module, PyObject *args) {
    PyObject *ring;
    PyObject *buf;
    PyObject *user_data;
    unsigned int flags;
    int fd;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!api->ring_construct_recvmsg) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API ring_construct_recvmsg is unavailable");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "OiOIO:construct_recvmsg", &ring, &fd, &buf, &flags, &user_data)) {
        return NULL;
    }
    return api->ring_construct_recvmsg(ring, fd, buf, flags, user_data);
}

static PyObject *client_construct_sendmsg(PyObject *module, PyObject *args) {
    PyObject *ring;
    PyObject *data;
    PyObject *address;
    PyObject *user_data;
    unsigned int flags;
    int fd;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!api->ring_construct_sendmsg) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API ring_construct_sendmsg is unavailable");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "OiOOIO:construct_sendmsg", &ring, &fd, &data, &address, &flags, &user_data)) {
        return NULL;
    }
    return api->ring_construct_sendmsg(ring, fd, data, address, flags, user_data);
}

static PyObject *client_construct_connect(PyObject *module, PyObject *args) {
    PyObject *ring;
    PyObject *address;
    PyObject *user_data;
    int fd;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!api->ring_construct_connect) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API ring_construct_connect is unavailable");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "OiOO:construct_connect", &ring, &fd, &address, &user_data)) {
        return NULL;
    }
    return api->ring_construct_connect(ring, fd, address, user_data);
}

static PyObject *client_construct_cancel(PyObject *module, PyObject *args) {
    PyObject *ring;
    PyObject *target;
    PyObject *user_data;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!api->ring_construct_cancel) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API ring_construct_cancel is unavailable");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "OOO:construct_cancel", &ring, &target, &user_data)) {
        return NULL;
    }
    return api->ring_construct_cancel(ring, target, user_data);
}

static PyObject *client_completion_nowait(PyObject *module, PyObject *completion) {
    int nowait = 0;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!api->completion_nowait) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API completion_nowait is unavailable");
        return NULL;
    }
    if (api->completion_nowait(completion, &nowait) < 0) {
        return NULL;
    }
    return PyBool_FromLong(nowait);
}

static PyObject *client_set_nowait(PyObject *module, PyObject *args) {
    PyObject *completion;
    int nowait;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!api->completion_set_nowait) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API completion_set_nowait is unavailable");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "Oi:set_nowait", &completion, &nowait)) {
        return NULL;
    }
    if (api->completion_set_nowait(completion, nowait) < 0) {
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *client_construct_recv_buf(PyObject *module, PyObject *args) {
    PyObject *ring;
    PyObject *buf_group;
    PyObject *user_data;
    unsigned int flags;
    int fd;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!api->ring_construct_recv_buf) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API ring_construct_recv_buf is unavailable");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "OiOIO:construct_recv_buf", &ring, &fd, &buf_group, &flags, &user_data)) {
        return NULL;
    }
    return api->ring_construct_recv_buf(ring, fd, buf_group, flags, user_data);
}

static PyObject *client_prepare(PyObject *module, PyObject *args) {
    PyObject *ring;
    PyObject *completions;
    int prepared = 0;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!api->ring_prepare) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API ring_prepare is unavailable");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "OO:prepare", &ring, &completions)) {
        return NULL;
    }
    if (api->ring_prepare(ring, completions, &prepared) < 0) {
        return NULL;
    }
    return PyLong_FromLong(prepared);
}

static PyObject *client_completion_prepared(PyObject *module, PyObject *completion) {
    int prepared = 0;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!api->completion_prepared) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API completion_prepared is unavailable");
        return NULL;
    }
    if (api->completion_prepared(completion, &prepared) < 0) {
        return NULL;
    }
    return PyBool_FromLong(prepared);
}

static PyObject *client_prepare_cancel(PyObject *module, PyObject *args) {
    PyObject *ring;
    PyObject *target_completion;

    (void)module;
    if (!api) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API was not imported");
        return NULL;
    }
    if (!PyArg_ParseTuple(args, "OO:prepare_cancel", &ring, &target_completion)) {
        return NULL;
    }
    return prepare_and_drop(ring, api->ring_construct_cancel(ring, target_completion, Py_None));
}

static PyMethodDef client_methods[] = {
    {"metadata", (PyCFunction)client_metadata, METH_NOARGS, NULL},
    {"probe", (PyCFunction)client_probe, METH_NOARGS, NULL},
    {"ring_summary", (PyCFunction)client_ring_summary, METH_VARARGS, NULL},
    {"completion_summary", (PyCFunction)client_completion_summary, METH_O, NULL},
    {"completion_sequence", (PyCFunction)client_completion_sequence, METH_O, NULL},
    {"set_c_callback", _PyCFunction_CAST(client_set_c_callback), METH_VARARGS, NULL},
    {"clear_c_callback", (PyCFunction)client_clear_c_callback, METH_O, NULL},
    {"serve_completions", (PyCFunction)client_serve_completions, METH_O, NULL},
    {"stop_serving", (PyCFunction)client_stop_serving, METH_O, NULL},
    {"reset_serving", (PyCFunction)client_reset_serving, METH_O, NULL},
    {"prepare_recv_multishot", _PyCFunction_CAST(client_prepare_recv_multishot), METH_VARARGS, NULL},
    {"prepare_recv_buf", _PyCFunction_CAST(client_prepare_recv_buf), METH_VARARGS, NULL},
    {"prepare_recvmsg", _PyCFunction_CAST(client_prepare_recvmsg), METH_VARARGS, NULL},
    {"prepare_sendto", _PyCFunction_CAST(client_prepare_sendto), METH_VARARGS, NULL},
    {"prepare_sendmsg", _PyCFunction_CAST(client_prepare_sendmsg), METH_VARARGS, NULL},
    {"prepare_sendmsg_zc", _PyCFunction_CAST(client_prepare_sendmsg_zc), METH_VARARGS, NULL},
    {"prepare_send_zc", _PyCFunction_CAST(client_prepare_send_zc), METH_VARARGS, NULL},
    {"prepare_accept", _PyCFunction_CAST(client_prepare_accept), METH_VARARGS, NULL},
    {"prepare_accept_multishot", _PyCFunction_CAST(client_prepare_accept_multishot), METH_VARARGS, NULL},
    {"prepare_connect", _PyCFunction_CAST(client_prepare_connect), METH_VARARGS, NULL},
    {"prepare_poll", _PyCFunction_CAST(client_prepare_poll), METH_VARARGS, NULL},
    {"prepare_poll_multishot", _PyCFunction_CAST(client_prepare_poll_multishot), METH_VARARGS, NULL},
    {"prepare_poll_remove", _PyCFunction_CAST(client_prepare_poll_remove), METH_VARARGS, NULL},
    {"prepare_cancel", _PyCFunction_CAST(client_prepare_cancel), METH_VARARGS, NULL},
    {"prepare_shutdown", _PyCFunction_CAST(client_prepare_shutdown), METH_VARARGS, NULL},
    {"prepare_close", _PyCFunction_CAST(client_prepare_close), METH_VARARGS, NULL},
    {"prepare_close_nowait", _PyCFunction_CAST(client_prepare_close_nowait), METH_VARARGS, NULL},
    {"prepare_shutdown_nowait", _PyCFunction_CAST(client_prepare_shutdown_nowait), METH_VARARGS, NULL},
    {"prepare_cancel_nowait", _PyCFunction_CAST(client_prepare_cancel_nowait), METH_VARARGS, NULL},
    {"prepare_poll_remove_nowait", _PyCFunction_CAST(client_prepare_poll_remove_nowait), METH_VARARGS, NULL},
    {"prepare_read", _PyCFunction_CAST(client_prepare_read), METH_VARARGS, NULL},
    {"prepare_write", _PyCFunction_CAST(client_prepare_write), METH_VARARGS, NULL},
    {"prepare_openat", _PyCFunction_CAST(client_prepare_openat), METH_VARARGS, NULL},
    {"prepare_statx", _PyCFunction_CAST(client_prepare_statx), METH_VARARGS, NULL},
    {"prepare_statx_fdsize", _PyCFunction_CAST(client_prepare_statx_fdsize), METH_VARARGS, NULL},
    {"statx_st_size", _PyCFunction_CAST(client_statx_st_size), METH_O, NULL},
    {"statx_try_read_st_size", _PyCFunction_CAST(client_statx_try_read_st_size), METH_O, NULL},
    {"prepare_socket", _PyCFunction_CAST(client_prepare_socket), METH_VARARGS, NULL},
    {"construct_send", _PyCFunction_CAST(client_construct_send), METH_VARARGS, NULL},
    {"construct_recv", _PyCFunction_CAST(client_construct_recv), METH_VARARGS, NULL},
    {"construct_read", _PyCFunction_CAST(client_construct_read), METH_VARARGS, NULL},
    {"construct_write", _PyCFunction_CAST(client_construct_write), METH_VARARGS, NULL},
    {"construct_sendto", _PyCFunction_CAST(client_construct_sendto), METH_VARARGS, NULL},
    {"construct_recvmsg", _PyCFunction_CAST(client_construct_recvmsg), METH_VARARGS, NULL},
    {"construct_sendmsg", _PyCFunction_CAST(client_construct_sendmsg), METH_VARARGS, NULL},
    {"construct_connect", _PyCFunction_CAST(client_construct_connect), METH_VARARGS, NULL},
    {"construct_recv_buf", _PyCFunction_CAST(client_construct_recv_buf), METH_VARARGS, NULL},
    {"construct_cancel", _PyCFunction_CAST(client_construct_cancel), METH_VARARGS, NULL},
    {"completion_nowait", (PyCFunction)client_completion_nowait, METH_O, NULL},
    {"set_nowait", _PyCFunction_CAST(client_set_nowait), METH_VARARGS, NULL},
    {"prepare", _PyCFunction_CAST(client_prepare), METH_VARARGS, NULL},
    {"completion_prepared", (PyCFunction)client_completion_prepared, METH_O, NULL},
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
    if (api->struct_size < sizeof(UringApi_CAPI)) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API struct_size is too small");
        return -1;
    }
    if ((api->feature_flags & URING_API_CAPI_FEATURE_CORE) == 0) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API feature set is incomplete");
        return -1;
    }
    if (!api->probe || !api->ring_new || !api->ring_check || !api->ring_close || !api->ring_fd || !api->ring_features ||
        !api->ring_sq_entries || !api->ring_cq_entries || !api->ring_closed || !api->ring_running ||
        !api->ring_construct_recv || !api->ring_construct_recv_buf || !api->ring_construct_recv_multishot ||
        !api->ring_construct_send || !api->ring_construct_send_zc || !api->ring_construct_recvmsg ||
        !api->ring_construct_sendto || !api->ring_construct_sendmsg || !api->ring_construct_sendmsg_zc ||
        !api->ring_construct_accept || !api->ring_construct_accept_multishot || !api->ring_construct_connect ||
        !api->ring_construct_poll || !api->ring_construct_poll_multishot || !api->ring_construct_poll_remove ||
        !api->ring_construct_cancel || !api->ring_construct_shutdown || !api->ring_construct_close ||
        !api->ring_construct_read || !api->ring_construct_write || !api->ring_construct_openat ||
        !api->ring_construct_statx || !api->ring_construct_statx_fdsize || !api->statx_st_size ||
        !api->ring_construct_socket || !api->ring_prepare || !api->completion_prepared || !api->completion_nowait ||
        !api->completion_set_nowait || !api->ring_break_wait || !api->ring_wait || !api->ring_set_callback ||
        !api->ring_set_exception_handler || !api->ring_set_c_callback || !api->ring_serve_completions ||
        !api->ring_stop_serving || !api->ring_reset_serving || !api->completion_check || !api->completion_user_data ||
        !api->completion_res || !api->completion_flags || !api->completion_sequence || !api->completion_result ||
        !api->completion_kind || !api->completion_set_user_data || !api->ring_set_nowait_error_handler ||
        !api->ring_submit || !api->ring_auto_submit || !api->ring_set_auto_submit || !api->ring_pending_count ||
        !api->completion_set_sequence || !api->completion_clear_user_data || !api->ring_wait_idle) {
        PyErr_SetString(PyExc_RuntimeError, "uring-api C API function table is incomplete");
        return -1;
    }
    return 0;
}

static void client_free(void *module) {
    (void)module;
    Py_CLEAR(callback_sink);
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

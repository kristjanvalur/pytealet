/*
 * statx buffer helpers for the _uring_api extension.
 */

#include "uring_api_statx.h"

#include "uring_api_statx_layout.h"

int uring_api_statx_try_read_st_size(const void *buf, Py_ssize_t buflen, unsigned long long *size_out) {
    const struct uring_api_statx_stx_size_prefix *statx;

    if (!size_out || buflen < URING_API_STATX_BUFFER_SIZE) {
        return 0;
    }
    statx = (const struct uring_api_statx_stx_size_prefix *)buf;
    if (!(statx->stx_mask & URING_API_STATX_SIZE_MASK)) {
        return 0;
    }
    *size_out = statx->stx_size;
    return 1;
}

int uring_api_statx_read_st_size(const void *buf, Py_ssize_t buflen, unsigned long long *size_out) {
    if (!size_out) {
        PyErr_SetString(PyExc_ValueError, "size_out must not be NULL");
        return -1;
    }
    if (buflen < URING_API_STATX_BUFFER_SIZE) {
        PyErr_SetString(PyExc_ValueError, "statx buffer must be at least STATX_BUFFER_SIZE bytes");
        return -1;
    }
    if (!uring_api_statx_try_read_st_size(buf, buflen, size_out)) {
        PyErr_SetString(PyExc_ValueError, "statx buffer does not contain STATX_SIZE fields");
        return -1;
    }
    return 0;
}

PyObject *UringApiStatx_st_size(PyObject *self, PyObject *arg) {
    Py_buffer view;
    unsigned long long size;

    (void)self;
    if (PyObject_GetBuffer(arg, &view, PyBUF_SIMPLE) < 0) {
        return NULL;
    }
    if (uring_api_statx_read_st_size(view.buf, view.len, &size) < 0) {
        PyBuffer_Release(&view);
        return NULL;
    }
    PyBuffer_Release(&view);
    return PyLong_FromUnsignedLongLong(size);
}

int UringApiCapi_StatxStSize(PyObject *buf, unsigned long long *value) {
    Py_buffer view;
    int status;

    if (!value) {
        PyErr_SetString(PyExc_ValueError, "value must not be NULL");
        return -1;
    }
    if (PyObject_GetBuffer(buf, &view, PyBUF_SIMPLE) < 0) {
        return -1;
    }
    status = uring_api_statx_read_st_size(view.buf, view.len, value);
    PyBuffer_Release(&view);
    return status;
}

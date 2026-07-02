/*
 * statx buffer helpers for the _uring_api extension.
 */

#include "uring_api_statx.h"

#include "uring_api_statx_layout.h"

int uring_api_statx_read_st_size(const void *buf, Py_ssize_t buflen, unsigned long long *size_out) {
    const struct uring_api_statx_stx_size_prefix *statx;

    if (!size_out) {
        PyErr_SetString(PyExc_ValueError, "size_out must not be NULL");
        return -1;
    }
    if (buflen < URING_API_STATX_BUFFER_SIZE) {
        PyErr_SetString(PyExc_ValueError, "statx buffer must be at least STATX_BUFFER_SIZE bytes");
        return -1;
    }
    statx = (const struct uring_api_statx_stx_size_prefix *)buf;
    if (!(statx->stx_mask & URING_API_STATX_SIZE_MASK)) {
        PyErr_SetString(PyExc_ValueError, "statx buffer does not contain STATX_SIZE fields");
        return -1;
    }
    *size_out = statx->stx_size;
    return 0;
}
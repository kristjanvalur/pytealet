#ifndef URING_API_STATX_H
#define URING_API_STATX_H

/* private implementation header; not part of the public C API. */

#include <Python.h>

#define URING_API_AT_EMPTY_PATH 0x1000
#define URING_API_STATX_SIZE_MASK 0x00000200u
#define URING_API_STATX_BUFFER_SIZE 256

int uring_api_statx_try_read_st_size(const void *buf, Py_ssize_t buflen, unsigned long long *size_out);
int uring_api_statx_read_st_size(const void *buf, Py_ssize_t buflen, unsigned long long *size_out);
PyObject *UringApiStatx_st_size(PyObject *self, PyObject *arg);
int UringApiCapi_StatxStSize(PyObject *buf, unsigned long long *value);

#endif
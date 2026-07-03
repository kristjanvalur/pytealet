#ifndef URING_API_PROBE_H
#define URING_API_PROBE_H

/* private implementation header; not part of the public C API. */

#include "uring_api_common.h"

PyObject *uring_api_probe(PyObject *self, PyObject *args, PyObject *kwargs);
int uring_api_export_capi(PyObject *module);

#endif
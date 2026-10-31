#ifndef URING_API_RING_H
#define URING_API_RING_H

/* private implementation header; not part of the public C API. */

#include "uring_api_common.h"

PyObject *UringApiRing_new(PyTypeObject *type, PyObject *args, PyObject *kwargs);
int UringApiRing_init(UringApiRing *self, PyObject *args, PyObject *kwargs);
void UringApiRing_dealloc(UringApiRing *self);
int UringApiRing_traverse(UringApiRing *self, visitproc visit, void *arg);
int UringApiRing_clear(UringApiRing *self);
PyObject *UringApiRing_close(UringApiRing *self, PyObject *ignored);
/* Flush prepared SQEs; returns a Python int count (may be 0). */
PyObject *UringApiRing_submit(UringApiRing *self, PyObject *ignored);
/* 0, and *cancelled is 1 when a send_all cancel was posted. 0 and
 * *cancelled is 0 when the completion is not on a pipeline or that
 * pipeline has no send_all in progress. The abortive mark is still set
 * when the completion was found. -1 with an exception on failure. */
int UringApiRing_abort_queue_impl(UringApiRing *self, PyObject *completion, int *cancelled);
PyObject *UringApiRing_enter(UringApiRing *self, PyObject *ignored);
PyObject *UringApiRing_exit(UringApiRing *self, PyObject *args);
int UringApiRing_set_callback(UringApiRing *self, PyObject *value, void *closure);
int UringApiRing_set_exception_handler(UringApiRing *self, PyObject *value, void *closure);
int UringApiRing_set_nowait_error_handler(UringApiRing *self, PyObject *value, void *closure);

#endif

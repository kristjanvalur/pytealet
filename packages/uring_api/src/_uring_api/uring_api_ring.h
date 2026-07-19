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
PyObject *UringApiRing_enter(UringApiRing *self, PyObject *ignored);
PyObject *UringApiRing_exit(UringApiRing *self, PyObject *args);
int UringApiRing_set_callback(UringApiRing *self, PyObject *value, void *closure);
int UringApiRing_set_exception_handler(UringApiRing *self, PyObject *value, void *closure);
int UringApiRing_set_pre_submit(UringApiRing *self, PyObject *value, void *closure);
int UringApiRing_set_c_pre_submit_impl(UringApiRing *self, UringApiPreSubmitCallback callback, void *user_data);

#endif

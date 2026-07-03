#ifndef URING_API_DISPATCH_H
#define URING_API_DISPATCH_H

/* private implementation header; not part of the public C API. */

#include "uring_api_common.h"

PyObject *UringApiRing_break_wait(UringApiRing *self, PyObject *ignored);
int UringApiRing_stop_delivery(UringApiRing *self);
PyObject *UringApiRing_stop_serving(UringApiRing *self, PyObject *ignored);
PyObject *UringApiRing_reset_serving(UringApiRing *self, PyObject *ignored);
PyObject *UringApiRing_wait_impl(UringApiRing *self, int timeout_kind, struct __kernel_timespec *timeout,
                                 bool from_delivery_thread);
PyObject *UringApiRing_serve_completions(UringApiRing *self, PyObject *ignored);
int UringApiRing_set_c_callback_impl(UringApiRing *self, UringApiCompletionCallback callback, void *user_data);
PyObject *UringApiRing_wait(UringApiRing *self, PyObject *args, PyObject *kwargs);

#endif
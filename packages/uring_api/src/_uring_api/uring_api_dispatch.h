#ifndef URING_API_DISPATCH_H
#define URING_API_DISPATCH_H

/* private implementation header; not part of the public C API. */

#include "uring_api_common.h"

enum {
    URING_API_WAIT_BLOCKING = 0,
    URING_API_WAIT_TIMEOUT = 1,
    URING_API_WAIT_PEEK = 2,
};

/*
 * Opens wait_idle immediately. Submits a wake NOP unless completion service is
 * active (workers already reap the CQ). force_nop=1 always submits (stop_serving).
 * Returns 0 or -1 with exception set.
 */
int UringApiRing_break_wait_impl(UringApiRing *self, int force_nop);
PyObject *UringApiRing_break_wait(UringApiRing *self, PyObject *ignored);
PyObject *UringApiRing_wait_idle(UringApiRing *self, PyObject *args, PyObject *kwargs);
int UringApiRing_stop_delivery(UringApiRing *self);
PyObject *UringApiRing_stop_serving(UringApiRing *self, PyObject *ignored);
PyObject *UringApiRing_reset_serving(UringApiRing *self, PyObject *ignored);
PyObject *UringApiRing_wait_impl(UringApiRing *self, int timeout_kind, struct __kernel_timespec *timeout,
                                 bool from_delivery_thread, UringApiStagingBuffer *staging);
/* If a delivery callback is set, invoke it for non-empty ``ready`` and return None.
 * Otherwise return ``ready`` (list, possibly empty). Consumes the ``ready`` ref when
 * delivering. */
PyObject *UringApiRing_wait_finish_with_optional_delivery(UringApiRing *self, PyObject *ready);
PyObject *UringApiRing_serve_completions(UringApiRing *self, PyObject *ignored);
int UringApiRing_set_c_callback_impl(UringApiRing *self, UringApiCompletionCallback callback, void *user_data);
PyObject *UringApiRing_wait(UringApiRing *self, PyObject *args, PyObject *kwargs);

#endif

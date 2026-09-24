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
PyObject *UringApiRing_wait_idle(UringApiRing *self, URING_API_PARSE_ARGS);
int UringApiRing_stop_delivery(UringApiRing *self);
PyObject *UringApiRing_stop_serving(UringApiRing *self, PyObject *ignored);
PyObject *UringApiRing_reset_serving(UringApiRing *self, PyObject *ignored);
PyObject *UringApiRing_wait_impl(UringApiRing *self, int timeout_kind, struct __kernel_timespec *timeout);
void cqe_fifo_clear(UringApiCqeFifo *fifo);
/* Drain returns None when this wait snapshotted a callback (already delivered
 * or empty); consume that, flush, and return None. A list is pull-mode. */
PyObject *UringApiRing_wait_finish_with_optional_delivery(UringApiRing *self, PyObject *ready);
PyObject *UringApiRing_serve_completions(UringApiRing *self, PyObject *ignored);
int UringApiRing_set_c_callback_impl(UringApiRing *self, UringApiCompletionCallback callback, void *user_data);
PyObject *UringApiRing_wait(UringApiRing *self, URING_API_PARSE_ARGS);
/* Same park as wait() without harvest (no cqe_seen). 0 success (*ready is 1 if
 * the CQ has an entry, 0 on timeout/empty), -1 exception. */
int UringApiRing_poll_impl(UringApiRing *self, int timeout_kind, struct __kernel_timespec *timeout, int *ready);
PyObject *UringApiRing_poll(UringApiRing *self, URING_API_PARSE_ARGS);
/* 1 skip wait()/callback (skip_all: report nowait_error_handler when res < 0;
 * skip_success: skip only when res >= 0), 0 deliver the handle. */
int skip_success_omit_delivery(UringApiRing *self, UringApiCompletion *completion, int res, unsigned int flags);

#endif

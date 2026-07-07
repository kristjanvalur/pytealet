/*
 * Completion dispatch and delivery service for the _uring_api extension.
 */

#include "uring_api_dispatch.h"
#include "uring_api_completion.h"
#include "uring_api_core.h"
#include "uring_api_staging.h"

#include <assert.h>

static bool delivery_should_stop(UringApiRing *self);

static int reap_one_cqe(UringApiRing *self, int timeout_kind, struct __kernel_timespec *timeout,
                        struct io_uring_cqe **cqe_out) {
    if (timeout_kind == URING_API_WAIT_BLOCKING) {
        return io_uring_wait_cqe(&self->ring, cqe_out);
    }
    if (timeout_kind == URING_API_WAIT_TIMEOUT) {
        return io_uring_wait_cqe_timeout(&self->ring, cqe_out, timeout);
    }
    if (timeout_kind == URING_API_WAIT_PEEK) {
        return io_uring_peek_cqe(&self->ring, cqe_out);
    }
    errno = EINVAL;
    return -EINVAL;
}

static PyObject *build_completion_result(UringApiCompletion *completion, int res, unsigned int flags,
                                         unsigned long long leg_index);

static int append_ready_completion(UringApiCompletion *completion, int res, unsigned int flags,
                                   unsigned long long leg_index, PyObject *ready) {
    PyObject *result = build_completion_result(completion, res, flags, leg_index);
    if (!result) {
        return -1;
    }
    if (result == Py_None) {
        Py_DECREF(result);
        return 0;
    }
    if (PyList_Append(ready, result) < 0) {
        Py_DECREF(result);
        return -1;
    }
    Py_DECREF(result);
    return 0;
}

static PyObject *staging_build_ready_list(UringApiStagingBuffer *staging) {
    PyObject *ready;
    size_t index;

    /* build failure is fatal for this drain: earlier rows may already have
     * cqe_seen set and complete() applied. no special rollback — when nothing
     * works, nothing works (same contract as callback invocation failure). */
    ready = PyList_New(0);
    if (!ready) {
        return NULL;
    }
    for (index = 0; index < staging->count; index++) {
        UringApiStagedCQE *staged = &staging->entries[index];
        if (append_ready_completion(staged->completion, staged->res, staged->flags, staged->leg_index, ready) <
            0) {
            Py_DECREF(ready);
            return NULL;
        }
    }
    return ready;
}

PyObject *UringApiRing_break_wait(UringApiRing *self, PyObject *Py_UNUSED(ignored)) {
    struct io_uring_sqe *sqe;
    PyObject *completion = NULL;
    int failed = 0;

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else {
        completion = UringApiCompletion_new_pending(URING_API_PENDING_WAKE, Py_None);
        if (completion) {
            sqe = get_sqe(self);
            if (!sqe) {
                failed = 1;
            } else {
                io_uring_prep_nop(sqe);
                sqe_set_completion(self, sqe, completion);
                if (submit_one(self) < 0) {
                    failed = 1;
                }
            }
        } else {
            failed = 1;
        }
    }
    Py_END_CRITICAL_SECTION();

    if (failed) {
        Py_XDECREF(completion);
        return NULL;
    }
    Py_RETURN_NONE;
}

int UringApiRing_stop_delivery(UringApiRing *self) {
    PyObject *wakeup = NULL;
    bool running;

    Py_BEGIN_CRITICAL_SECTION(self);
    running = delivery_is_running_locked(self);
    self->delivery_stop_requested = true;
    Py_END_CRITICAL_SECTION();

    if (!running) {
        return 0;
    }

    wakeup = UringApiRing_break_wait(self, NULL);
    if (!wakeup) {
        return -1;
    }
    Py_DECREF(wakeup);
    return 0;
}

PyObject *UringApiRing_stop_serving(UringApiRing *self, PyObject *Py_UNUSED(ignored)) {
    if (UringApiRing_stop_delivery(self) < 0) {
        return NULL;
    }
    Py_RETURN_NONE;
}

PyObject *UringApiRing_reset_serving(UringApiRing *self, PyObject *Py_UNUSED(ignored)) {
    int failed = 0;

    Py_BEGIN_CRITICAL_SECTION(self);
    if (delivery_is_running_locked(self)) {
        PyErr_SetString(PyExc_RuntimeError, "completion service is active");
        failed = 1;
    } else {
        self->delivery_stop_requested = false;
    }
    Py_END_CRITICAL_SECTION();

    if (failed) {
        return NULL;
    }
    Py_RETURN_NONE;
}

static int parse_timeout(PyObject *timeout_obj, struct __kernel_timespec *timeout) {
    double seconds;
    if (timeout_obj == NULL || timeout_obj == Py_None) {
        return URING_API_WAIT_BLOCKING;
    }
    if (PyLong_Check(timeout_obj)) {
        long value = PyLong_AsLong(timeout_obj);
        if (value == -1 && PyErr_Occurred()) {
            return -1;
        }
        seconds = (double)value;
    } else {
        seconds = PyFloat_AsDouble(timeout_obj);
        if (PyErr_Occurred()) {
            return -1;
        }
    }
    if (seconds < 0.0) {
        PyErr_SetString(PyExc_ValueError, "timeout must be non-negative or None");
        return -1;
    }
    if (seconds == 0.0) {
        return URING_API_WAIT_PEEK;
    }
    timeout->tv_sec = (long long)seconds;
    timeout->tv_nsec = (long long)((seconds - (double)timeout->tv_sec) * 1000000000.0);
    if (timeout->tv_nsec < 0) {
        timeout->tv_nsec = 0;
    }
    if (timeout->tv_nsec > 999999999) {
        timeout->tv_nsec = 999999999;
    }
    return URING_API_WAIT_TIMEOUT;
}

static PyObject *build_completion_result(UringApiCompletion *completion, int res, unsigned int flags,
                                         unsigned long long leg_index) {
    PyObject *delivered;
    int completion_result;

    /* the zc notification is not a user-visible result; it only releases resources retained for the send. */
    if (is_zero_copy_send_kind(completion->kind) && (flags & IORING_CQE_F_NOTIF)) {
        UringApiCompletion_clear_pending_state(completion);
        Py_DECREF(completion);
        Py_RETURN_NONE;
    }
    /* multishot CQEs with MORE are intermediate results: complete a fresh shell so the armed
     * pending handle is left untouched for the next kernel leg. */
    if (completion->multishot && (flags & IORING_CQE_F_MORE)) {
        delivered = UringApiCompletion_new_multishot_delivered_shell(completion, leg_index);
        if (!delivered) {
            return NULL;
        }
        completion_result = UringApiCompletion_complete((UringApiCompletion *)delivered, res, flags);
        if (completion_result < 0) {
            Py_DECREF(delivered);
            return NULL;
        }
        if (completion_result > 0) {
            Py_DECREF(delivered);
            Py_RETURN_NONE;
        }
        return delivered;
    }
    completion_result = UringApiCompletion_complete(completion, res, flags);
    /* negative means we failed while converting the CQE into Python-visible completion state. */
    if (completion_result < 0) {
        Py_DECREF(completion);
        return NULL;
    }
    /* positive means the CQE was handled internally, such as a wake completion for break_wait(). */
    if (completion_result > 0) {
        Py_DECREF(completion);
        Py_RETURN_NONE;
    }
    /* the zc operation CQE is the real result. Successful sends keep the internal ref until the NOTIF CQE. */
    if (is_zero_copy_send_kind(completion->kind)) {
        if (res >= 0) {
            return Py_NewRef(completion);
        }
        return (PyObject *)completion;
    }
    if (completion->multishot) {
        completion->sequence = leg_index;
    }
    return (PyObject *)completion;
}

static PyObject *drain_ready_completions(UringApiRing *self, UringApiStagingBuffer *staging, int timeout_kind,
                                          struct __kernel_timespec *timeout, bool from_delivery_thread) {
    struct io_uring_cqe *cqe = NULL;
    int reap_ret = 0;
    int peek_ret;
    int errnum;
    int record_failed = 0;
    bool stop_after_lock = from_delivery_thread && self->delivery_stop_requested;

    Py_BEGIN_ALLOW_THREADS;
    /* only one thread can drain at the time. */
    PyThread_acquire_lock(self->cqe_drain_lock, WAIT_LOCK);

    /* only one delivery worker can be in the kernel wait at once; any others are
     * queued on the drain lock and should bail out once stop is requested. */
    if (stop_after_lock) {
        PyThread_release_lock(self->cqe_drain_lock);
    } else {
        staging_buffer_reset(staging);
        reap_ret = reap_one_cqe(self, timeout_kind, timeout, &cqe);
        if (reap_ret == 0 && cqe) {
            if (staging_buffer_record_cqe(self, staging, cqe) < 0) {
                record_failed = 1;
            } else {
                for (;;) {
                    peek_ret = io_uring_peek_cqe(&self->ring, &cqe);
                    if (peek_ret != 0 || !cqe) {
                        break;
                    }
                    if (staging_buffer_record_cqe(self, staging, cqe) < 0) {
                        record_failed = 1;
                        break;
                    }
                }
            }
        }
        PyThread_release_lock(self->cqe_drain_lock);
    }
    Py_END_ALLOW_THREADS;

    if (stop_after_lock) {
        return PyList_New(0);
    }

    if (record_failed) {
        PyErr_NoMemory();
        return NULL;
    }
    if (reap_ret < 0) {
        errnum = normalize_ret_errno(reap_ret);
        if (errnum == EAGAIN || errnum == ETIME || errnum == ETIMEDOUT) {
            return PyList_New(0);
        }
        errno = errnum;
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }
    if (staging->count == 0) {
        return PyList_New(0);
    }
    return staging_build_ready_list(staging);
}

PyObject *UringApiRing_wait_impl(UringApiRing *self, int timeout_kind, struct __kernel_timespec *timeout,
                                 bool from_delivery_thread, UringApiStagingBuffer *staging) {
    PyObject *ready;

    if (!staging) {
        staging = &self->wait_staging;
    }
    if (ring_check_open(self) < 0) {
        return NULL;
    }
    if (ring_check_client_thread(self) < 0) {
        return NULL;
    }
    if (receive_wait_begin(self, from_delivery_thread) < 0) {
        return NULL;
    }
    if (from_delivery_thread && delivery_should_stop(self)) {
        receive_wait_end(self, from_delivery_thread);
        return PyList_New(0);
    }

    ready = drain_ready_completions(self, staging, timeout_kind, timeout, from_delivery_thread);
    if (!ready) {
        receive_wait_end(self, from_delivery_thread);
        return NULL;
    }

    receive_wait_end(self, from_delivery_thread);
    return ready;
}

static bool delivery_should_stop(UringApiRing *self) {
    bool stop;

    Py_BEGIN_CRITICAL_SECTION(self);
    stop = self->delivery_stop_requested || self->receive_state != URING_API_RECEIVE_DELIVERING || !self->initialized;
    Py_END_CRITICAL_SECTION();
    return stop;
}

static PyObject *delivery_get_callback(UringApiRing *self) {
    PyObject *callback;

    Py_BEGIN_CRITICAL_SECTION(self);
    callback = Py_XNewRef(self->delivery_callback);
    Py_END_CRITICAL_SECTION();
    if (!callback) {
        PyErr_SetString(PyExc_RuntimeError, "delivery callback is not set");
    }
    return callback;
}

static int delivery_get_c_callback(UringApiRing *self, UringApiCompletionCallback *callback, void **user_data) {
    int found;

    Py_BEGIN_CRITICAL_SECTION(self);
    *callback = self->c_delivery_callback;
    *user_data = self->c_delivery_callback_user_data;
    found = *callback != NULL;
    Py_END_CRITICAL_SECTION();
    return found;
}

static void delivery_request_stop(UringApiRing *self) {
    Py_BEGIN_CRITICAL_SECTION(self);
    self->delivery_stop_requested = true;
    Py_END_CRITICAL_SECTION();
}

static void delivery_request_stop_and_wake(UringApiRing *self) {
    PyObject *wakeup;

    delivery_request_stop(self);
    wakeup = UringApiRing_break_wait(self, NULL);
    if (!wakeup) {
        PyErr_WriteUnraisable((PyObject *)self);
        return;
    }
    Py_DECREF(wakeup);
}

static int delivery_invoke_batch(UringApiRing *self, PyObject *ready) {
    UringApiCompletionCallback c_callback;
    void *c_callback_user_data;

    if (PyList_GET_SIZE(ready) == 0) {
        return 0;
    }

    if (delivery_get_c_callback(self, &c_callback, &c_callback_user_data)) {
        int callback_ret = c_callback((PyObject *)self, ready, c_callback_user_data);
        if (callback_ret < 0) {
            PyErr_WriteUnraisable((PyObject *)self);
            delivery_request_stop_and_wake(self);
            return -1;
        }
        return 0;
    }

    PyObject *callback = delivery_get_callback(self);
    PyObject *call_result;
    if (!callback) {
        PyErr_WriteUnraisable((PyObject *)self);
        delivery_request_stop_and_wake(self);
        return -1;
    }
    call_result = PyObject_CallOneArg(callback, ready);
    Py_DECREF(callback);
    if (!call_result) {
        PyErr_WriteUnraisable((PyObject *)self);
        delivery_request_stop_and_wake(self);
        return -1;
    }
    Py_DECREF(call_result);
    return 0;
}

PyObject *UringApiRing_serve_completions(UringApiRing *self, PyObject *Py_UNUSED(ignored)) {
    UringApiStagingBuffer worker_staging = {NULL, 0, 0};
    bool failed = false;
    bool wait_failed = false;

    if (ring_check_open(self) < 0) {
        return NULL;
    }
    if (ring_check_client_thread(self) < 0) {
        return NULL;
    }

    Py_BEGIN_CRITICAL_SECTION(self);
    if (!self->initialized) {
        PyErr_SetString(PyExc_RuntimeError, "ring is closed");
        failed = true;
    } else if (!self->delivery_callback && !self->c_delivery_callback) {
        PyErr_SetString(PyExc_RuntimeError, "delivery callback is not set");
        failed = true;
    } else if (self->receive_state != URING_API_RECEIVE_IDLE && self->receive_state != URING_API_RECEIVE_DELIVERING) {
        PyErr_SetString(PyExc_RuntimeError, "another wait is already active");
        failed = true;
    } else {
        self->receive_state = URING_API_RECEIVE_DELIVERING;
        self->delivery_active_workers++;
    }
    Py_END_CRITICAL_SECTION();

    if (failed) {
        return NULL;
    }

    while (!delivery_should_stop(self)) {
        PyObject *ready = UringApiRing_wait_impl(self, URING_API_WAIT_BLOCKING, NULL, true, &worker_staging);

        if (!ready) {
            delivery_request_stop(self);
            wait_failed = true;
            break;
        }
        if (delivery_invoke_batch(self, ready) < 0) {
            Py_DECREF(ready);
            wait_failed = true;
            break;
        }
        Py_DECREF(ready);
    }

    staging_buffer_clear(&worker_staging);
    delivery_mark_exited(self);
    if (wait_failed) {
        return NULL;
    }
    Py_RETURN_NONE;
}

int UringApiRing_set_c_callback_impl(UringApiRing *self, UringApiCompletionCallback callback, void *user_data) {
    int ret = 0;

    Py_BEGIN_CRITICAL_SECTION(self);
    if (delivery_is_running_locked(self)) {
        PyErr_SetString(PyExc_RuntimeError, "cannot change callback while completion service is active");
        ret = -1;
    } else {
        self->c_delivery_callback = callback;
        self->c_delivery_callback_user_data = callback ? user_data : NULL;
    }
    Py_END_CRITICAL_SECTION();
    return ret;
}

PyObject *UringApiRing_wait(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"timeout", NULL};
    struct __kernel_timespec timeout;
    PyObject *timeout_obj = Py_None;
    int timeout_kind;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|O", keywords, &timeout_obj)) {
        return NULL;
    }
    timeout_kind = parse_timeout(timeout_obj, &timeout);
    if (timeout_kind < 0) {
        return NULL;
    }

    return UringApiRing_wait_impl(self, timeout_kind, &timeout, false, NULL);
}
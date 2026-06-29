/*
 * Completion dispatch and delivery service for the _uring_api extension.
 *
 * This file contains wait(), CQE conversion, break_wait(), and callback-driven
 * completion serving. It is included by _uring_api.c as part of the single
 * extension translation unit.
 */

static PyObject *UringApiRing_break_wait(UringApiRing *self, PyObject *Py_UNUSED(ignored)) {
    struct io_uring_sqe *sqe;
    PyObject *completion = NULL;
    int failed = 0;

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else {
        completion = UringApiCompletion_new_pending(URING_API_PENDING_WAKE, Py_None, NULL);
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

static int UringApiRing_stop_delivery(UringApiRing *self) {
    PyObject *wakeup = NULL;
    bool running;

    Py_BEGIN_CRITICAL_SECTION_MUTEX(&self->receive_mutex);
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

static PyObject *UringApiRing_stop_serving(UringApiRing *self, PyObject *Py_UNUSED(ignored)) {
    if (UringApiRing_stop_delivery(self) < 0) {
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *UringApiRing_reset_serving(UringApiRing *self, PyObject *Py_UNUSED(ignored)) {
    int failed = 0;

    Py_BEGIN_CRITICAL_SECTION_MUTEX(&self->receive_mutex);
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
        return 0;
    }
    seconds = PyFloat_AsDouble(timeout_obj);
    if (PyErr_Occurred()) {
        return -1;
    }
    if (seconds < 0.0) {
        PyErr_SetString(PyExc_ValueError, "timeout must be non-negative or None");
        return -1;
    }
    timeout->tv_sec = (long long)seconds;
    timeout->tv_nsec = (long long)((seconds - (double)timeout->tv_sec) * 1000000000.0);
    if (timeout->tv_nsec < 0) {
        timeout->tv_nsec = 0;
    }
    if (timeout->tv_nsec > 999999999) {
        timeout->tv_nsec = 999999999;
    }
    return 1;
}

static PyObject *build_cqe_result(UringApiRing *self, struct io_uring_cqe *cqe) {
    UringApiCompletion *completion = cqe_get_completion(self, cqe);
    PyObject *delivered;
    int res = cqe->res;
    unsigned int flags = cqe->flags;
    int completion_result;

    if (!completion) {
        PyErr_SetString(PyExc_SystemError, "io_uring CQE is missing its completion object");
        return NULL;
    }
    /* the zc notification is not a user-visible result; it only releases resources retained for the send. */
    if (is_zero_copy_send_kind(completion->kind) && (flags & IORING_CQE_F_NOTIF)) {
        UringApiCompletion_clear_pending_state(completion);
        Py_DECREF(completion);
        Py_RETURN_NONE;
    }
    completion_result = UringApiCompletion_complete(completion, res, flags);
    /* negative means we failed while converting the CQE into Python-visible completion state. */
    if (completion_result < 0) {
        if (!(flags & IORING_CQE_F_MORE)) {
            Py_DECREF(completion);
        }
        return NULL;
    }
    /* positive means the CQE was handled internally, such as a wake completion for break_wait(). */
    if (completion_result > 0) {
        if (!(flags & IORING_CQE_F_MORE)) {
            Py_DECREF(completion);
        }
        Py_RETURN_NONE;
    }
    /* the zc operation CQE is the real result. Successful sends keep the internal ref until the NOTIF CQE. */
    if (is_zero_copy_send_kind(completion->kind)) {
        if (res >= 0) {
            return Py_NewRef(completion);
        }
        return (PyObject *)completion;
    }
    /* multishot CQEs with MORE are intermediate results, so return copies while the original remains armed. */
    if (flags & IORING_CQE_F_MORE) {
        delivered = UringApiCompletion_new_delivered_copy(completion);
        if (!delivered) {
            return NULL;
        }
        return delivered;
    }
    return (PyObject *)completion;
}

static void receive_wait_lock(UringApiRing *self) {
    Py_BEGIN_ALLOW_THREADS
    PyThread_acquire_lock(self->delivery_wait_lock, WAIT_LOCK);
    Py_END_ALLOW_THREADS
}

static void receive_wait_unlock(UringApiRing *self) { PyThread_release_lock(self->delivery_wait_lock); }

static PyObject *UringApiRing_wait_impl(UringApiRing *self, int timeout_kind, struct __kernel_timespec *timeout,
                                        bool from_delivery_thread) {
    struct io_uring_cqe *cqe = NULL;
    PyObject *result;
    int ret;

    if (ring_check_open(self) < 0) {
        return NULL;
    }
    if (receive_wait_begin(self, from_delivery_thread) < 0) {
        return NULL;
    }
    if (from_delivery_thread) {
        receive_wait_lock(self);
        if (delivery_should_stop(self)) {
            receive_wait_unlock(self);
            Py_RETURN_NONE;
        }
    }

    errno = 0;
    if (timeout_kind == 0) {
        Py_BEGIN_ALLOW_THREADS
        ret = io_uring_wait_cqe(&self->ring, &cqe);
        Py_END_ALLOW_THREADS
    } else if (timeout->tv_sec == 0 && timeout->tv_nsec == 0) {
        ret = io_uring_peek_cqe(&self->ring, &cqe);
    } else {
        Py_BEGIN_ALLOW_THREADS
        ret = io_uring_wait_cqe_timeout(&self->ring, &cqe, timeout);
        Py_END_ALLOW_THREADS
    }

    if (ret < 0) {
        int errnum = normalize_ret_errno(ret);
        if (errnum == EAGAIN || errnum == ETIME || errnum == ETIMEDOUT) {
            if (from_delivery_thread) {
                receive_wait_unlock(self);
            }
            receive_wait_end(self, from_delivery_thread);
            Py_RETURN_NONE;
        }
        errno = errnum;
        PyErr_SetFromErrno(PyExc_OSError);
        if (from_delivery_thread) {
            receive_wait_unlock(self);
        }
        receive_wait_end(self, from_delivery_thread);
        return NULL;
    }
    if (!cqe) {
        if (from_delivery_thread) {
            receive_wait_unlock(self);
        }
        receive_wait_end(self, from_delivery_thread);
        Py_RETURN_NONE;
    }

    Py_BEGIN_CRITICAL_SECTION_MUTEX(&self->receive_mutex);
    result = build_cqe_result(self, cqe);
    io_uring_cqe_seen(&self->ring, cqe);
    if (!from_delivery_thread) {
        self->receive_state = URING_API_RECEIVE_IDLE;
    }
    Py_END_CRITICAL_SECTION();
    if (from_delivery_thread) {
        receive_wait_unlock(self);
    }
    return result;
}

static bool delivery_should_stop(UringApiRing *self) {
    bool stop;

    Py_BEGIN_CRITICAL_SECTION_MUTEX(&self->receive_mutex);
    stop = self->delivery_stop_requested || self->receive_state != URING_API_RECEIVE_DELIVERING ||
           !self->initialized;
    Py_END_CRITICAL_SECTION();
    return stop;
}

static PyObject *delivery_get_callback(UringApiRing *self) {
    PyObject *callback;

    Py_BEGIN_CRITICAL_SECTION_MUTEX(&self->receive_mutex);
    callback = Py_XNewRef(self->delivery_callback);
    Py_END_CRITICAL_SECTION();
    if (!callback) {
        PyErr_SetString(PyExc_RuntimeError, "delivery callback is not set");
    }
    return callback;
}

static int delivery_get_c_callback(UringApiRing *self, UringApi_CCompletionCallback *callback, void **user_data) {
    int found;

    Py_BEGIN_CRITICAL_SECTION_MUTEX(&self->receive_mutex);
    *callback = self->c_delivery_callback;
    *user_data = self->c_delivery_callback_user_data;
    found = *callback != NULL;
    Py_END_CRITICAL_SECTION();
    return found;
}

static void delivery_request_stop(UringApiRing *self) {
    Py_BEGIN_CRITICAL_SECTION_MUTEX(&self->receive_mutex);
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

static PyObject *UringApiRing_serve_completions(UringApiRing *self, PyObject *Py_UNUSED(ignored)) {
    bool failed = false;
    bool wait_failed = false;

    Py_BEGIN_CRITICAL_SECTION_MUTEX(&self->receive_mutex);
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
        if (!self->delivery_wait_lock) {
            self->delivery_wait_lock = PyThread_allocate_lock();
        }
        if (!self->delivery_wait_lock) {
            PyErr_NoMemory();
            failed = true;
        } else {
            self->receive_state = URING_API_RECEIVE_DELIVERING;
            self->delivery_active_workers++;
        }
    }
    Py_END_CRITICAL_SECTION();

    if (failed) {
        return NULL;
    }

    while (!delivery_should_stop(self)) {
        UringApi_CCompletionCallback c_callback;
        void *c_callback_user_data;
        PyObject *result = UringApiRing_wait_impl(self, 0, NULL, true);

        if (!result) {
            delivery_request_stop(self);
            wait_failed = true;
            break;
        }
        if (result == Py_None) {
            Py_DECREF(result);
            continue;
        }

        if (delivery_get_c_callback(self, &c_callback, &c_callback_user_data)) {
            int callback_ret = c_callback((PyObject *)self, result, c_callback_user_data);
            Py_DECREF(result);
            if (callback_ret < 0) {
                PyErr_WriteUnraisable((PyObject *)self);
                delivery_request_stop_and_wake(self);
                break;
            }
        } else {
            PyObject *callback = delivery_get_callback(self);
            PyObject *call_result;
            if (!callback) {
                Py_DECREF(result);
                PyErr_WriteUnraisable((PyObject *)self);
                delivery_request_stop_and_wake(self);
                break;
            }
            call_result = PyObject_CallOneArg(callback, result);
            Py_DECREF(callback);
            Py_DECREF(result);
            if (!call_result) {
                PyErr_WriteUnraisable((PyObject *)self);
                delivery_request_stop_and_wake(self);
                break;
            }
            Py_DECREF(call_result);
        }
    }

    delivery_mark_exited(self);
    if (wait_failed) {
        return NULL;
    }
    Py_RETURN_NONE;
}

static int UringApiRing_set_c_callback_impl(UringApiRing *self, UringApi_CCompletionCallback callback, void *user_data) {
    int ret = 0;

    Py_BEGIN_CRITICAL_SECTION_MUTEX(&self->receive_mutex);
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

static PyObject *UringApiRing_wait(UringApiRing *self, PyObject *args, PyObject *kwargs) {
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

    return UringApiRing_wait_impl(self, timeout_kind, &timeout, false);
}


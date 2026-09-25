/*
 * Completion dispatch and delivery service for the _uring_api extension.
 */

#include "uring_api_dispatch.h"
#include "uring_api_completion.h"
#include "uring_api_core.h"
#include "uring_api_park.h"
#include "uring_api_prepare.h"
#include "uring_api_send_all.h"

#include <assert.h>
#include <string.h>

static bool delivery_should_stop(UringApiRing *self);
static bool delivery_snapshot(UringApiRing *self, UringApiCompletionCallback *c_callback, void **c_callback_user_data,
                              PyObject **py_callback);
static void report_nowait_error(UringApiRing *self, int res, unsigned int flags, unsigned int kind, int has_fd, int fd);
static int fill_parked_without_enter(UringApiRing *self);
static int deliver_staged_one(UringApiRing *self, const UringApiStagedCQE *staged,
                              UringApiCompletionCallback c_callback, void *c_callback_user_data, PyObject *py_callback);
static int process_staged_cqe(UringApiRing *self, const UringApiStagedCQE *staged,
                              UringApiCompletionCallback c_callback, void *c_callback_user_data, PyObject *py_callback);
static int delivery_invoke_one(UringApiRing *self, PyObject *completion, UringApiCompletionCallback c_callback,
                               void *c_callback_user_data, PyObject *py_callback);

static PyObject *drain_empty_result(bool deliver) {
    if (deliver) {
        Py_RETURN_NONE;
    }
    return PyList_New(0);
}

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

int skip_success_omit_delivery(UringApiRing *self, UringApiCompletion *completion, int res, unsigned int flags) {
    int fd;

    if (completion_has_bit(completion, URING_API_C_SKIP_ALL)) {
        if (res < 0) {
            fd = nowait_advisory_fd(completion);
            report_nowait_error(self, res, flags, (unsigned int)completion->kind, fd >= 0, fd);
        }
        return 1;
    }
    if (!completion_has_bit(completion, URING_API_C_SKIP_SUCCESS)) {
        return 0;
    }
    /* skip_success: success stays silent; failure delivers this handle. */
    return res >= 0;
}

static PyObject *build_completion_result(UringApiRing *ring, UringApiCompletion *completion, int res,
                                         unsigned int flags, unsigned long long leg_index);

/* Package one staged CQE. *out is a new delivery ref, or NULL when the CQE was
 * internal (NOTIF) or on error. Always finishes the in-flight/aux bookkeeping
 * so a later callback failure cannot strand refs. */
static int package_ready_completion(UringApiRing *ring, UringApiCompletion *completion, int res, unsigned int flags,
                                    unsigned long long leg_index, PyObject **out) {
    PyObject *result;
    bool drop_in_flight_ref;

    /* build first so MORE shells copy live user_data while aux still counts
     * this CQE; finish then applies a pending user_data clear if aux hits 0. */
    result = build_completion_result(ring, completion, res, flags, leg_index);
    drop_in_flight_ref = completion_finish_in_flight_ref(ring, completion);
    /* result is always a delivery ref owned here, separate from the in-flight ref on completion. */
    if (!result) {
        if (drop_in_flight_ref) {
            ring_pending_dec(ring);
            Py_DECREF(completion);
        }
        *out = NULL;
        return -1;
    }
    if (drop_in_flight_ref) {
        ring_pending_dec(ring);
        Py_DECREF(completion);
    }
    /* zero-copy NOTIF (and similar): complete() returned internal; never delivered.
     * break_wait wake NOPs are discarded in consume_cqe (never packaged). */
    if (result == Py_None) {
        Py_DECREF(result);
        *out = NULL;
        return 0;
    }
    *out = result;
    return 0;
}

static int append_ready_completion(UringApiRing *ring, UringApiCompletion *completion, int res, unsigned int flags,
                                   unsigned long long leg_index, PyObject **ready) {
    PyObject *result;

    if (package_ready_completion(ring, completion, res, flags, leg_index, &result) < 0) {
        return -1;
    }
    if (!result) {
        return 0;
    }
    /* lazy list: allocate only when the first user-visible completion is ready. */
    if (*ready == NULL) {
        *ready = PyList_New(1);
        if (!*ready) {
            Py_DECREF(result);
            return -1;
        }
        PyList_SET_ITEM(*ready, 0, result);
    } else if (PyList_Append(*ready, result) < 0) {
        Py_DECREF(result);
        return -1;
    } else {
        Py_DECREF(result);
    }
    return 0;
}

enum {
    CQE_TAKE_READY = 1,
    CQE_TAKE_SKIP = 2,
};

static int nowait_cancel_cqe_is_lost_race(unsigned int kind, int res) {
    return kind == (unsigned int)URING_API_PENDING_CANCEL && (res == -ENOENT || res == -EALREADY);
}

static int cqe_fifo_grow(UringApiCqeFifo *fifo) {
    size_t new_cap = fifo->cap ? fifo->cap * 2 : 4;
    UringApiStagedCQE *items = PyMem_Malloc(new_cap * sizeof(*items));
    size_t i;

    if (!items) {
        return -1;
    }
    for (i = 0; i < fifo->count; i++) {
        items[i] = fifo->items[(fifo->head + i) % fifo->cap];
    }
    PyMem_Free(fifo->items);
    fifo->items = items;
    fifo->head = 0;
    fifo->cap = new_cap;
    return 0;
}

static int cqe_fifo_ensure(UringApiCqeFifo *fifo) {
    if (fifo->count < fifo->cap) {
        return 0;
    }
    return cqe_fifo_grow(fifo);
}

static int cqe_fifo_push(UringApiCqeFifo *fifo, const UringApiStagedCQE *item) {
    if (cqe_fifo_ensure(fifo) < 0) {
        return -1;
    }
    fifo->items[(fifo->head + fifo->count) % fifo->cap] = *item;
    fifo->count++;
    return 0;
}

static int cqe_fifo_pop(UringApiCqeFifo *fifo, UringApiStagedCQE *out) {
    if (fifo->count == 0) {
        return 0;
    }
    *out = fifo->items[fifo->head];
    fifo->head = (fifo->head + 1) % fifo->cap;
    fifo->count--;
    return 1;
}

void cqe_fifo_clear(UringApiCqeFifo *fifo) {
    PyMem_Free(fifo->items);
    fifo->items = NULL;
    fifo->head = 0;
    fifo->count = 0;
    fifo->cap = 0;
}

/* GIL held. cqe_seen. nowait failures invoke the handler here. */
static int consume_cqe(UringApiRing *self, struct io_uring_cqe *cqe, UringApiStagedCQE *out) {
    UringApiCompletion *completion;
    unsigned long long user_data;

    user_data = io_uring_cqe_get_data64(cqe);
    if (uring_api_ud_is_special(user_data)) {
        if (uring_api_ud_is_wake(user_data)) {
            ring_note_cqe(self);
            io_uring_cqe_seen(&self->ring, cqe);
            return CQE_TAKE_SKIP;
        }
        if (uring_api_ud_is_nowait(user_data)) {
            int res = cqe->res;
            unsigned int flags = cqe->flags;
            unsigned int kind = uring_api_nowait_kind(user_data);
            int fd = 0;
            int has_fd = uring_api_nowait_fd(user_data, &fd);

            if (res < 0 && !nowait_cancel_cqe_is_lost_race(kind, res)) {
                report_nowait_error(self, res, flags, kind, has_fd, fd);
            }
            ring_note_cqe(self);
            io_uring_cqe_seen(&self->ring, cqe);
            return CQE_TAKE_SKIP;
        }
        ring_note_cqe(self);
        io_uring_cqe_seen(&self->ring, cqe);
        return CQE_TAKE_SKIP;
    }

    completion = (UringApiCompletion *)(uintptr_t)user_data;
    assert(completion != NULL);
    out->res = cqe->res;
    out->flags = cqe->flags;
    out->completion = completion;
    out->leg_index = 0;
    if (completion_has_bit(completion, URING_API_C_MULTISHOT)) {
        out->leg_index = completion->sequence;
        completion->sequence++;
    }
    completion_prep_in_flight_ref(self, completion, cqe->res, cqe->flags);
    ring_note_cqe(self);
    io_uring_cqe_seen(&self->ring, cqe);
    return CQE_TAKE_READY;
}

/*
 * Wake entry point:
 * - open the host idle park for wait_idle() immediately
 * - best-effort internal NOP when a wait() reaper may be blocked on an empty CQ
 *
 * While completion service workers are active they own CQ reaping, so the NOP is
 * skipped (idle park only) unless force_nop is set — stop_serving needs a NOP to
 * interrupt workers blocked in the kernel wait. When the SQ is full, NOP failure
 * is ignored: a real CQE will arrive soon enough. The idle park never waits on
 * the NOP path.
 */
int UringApiRing_break_wait_impl(UringApiRing *self, int force_nop) {
    struct io_uring_sqe *sqe;
    int fatal = 0;
    int want_nop = force_nop;

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        fatal = 1;
    } else {
        if (!force_nop) {
            /* workers already reap; host only needs wait_idle */
            want_nop = !delivery_is_running_locked(self);
        }
        if (want_nop && ring_check_submit_thread(self, 1) < 0) {
            fatal = 1;
        }
    }
    Py_END_CRITICAL_SECTION();

    if (fatal) {
        return -1;
    }

    /* host park first: independent of SQ capacity and of the NOP */
    UringApiIdlePark_signal(&self->idle);

    if (!want_nop) {
        return 0;
    }

    /* best-effort NOP for wait() reapers; no Completion — tagged wake user_data (…01).
     * SQ full / submit errors ignored (a real CQE will arrive soon enough). */
    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        PyErr_Clear();
    } else {
        sqe = get_sqe(self);
        if (!sqe) {
            PyErr_Clear();
        } else {
            io_uring_prep_nop(sqe);
            io_uring_sqe_set_data64(sqe, URING_API_WAKE_USER_DATA);
            if (submit_one(self) < 0) {
                PyErr_Clear();
            }
        }
    }
    Py_END_CRITICAL_SECTION();

    return 0;
}

PyObject *UringApiRing_break_wait(UringApiRing *self, PyObject *Py_UNUSED(ignored)) {
    if (UringApiRing_break_wait_impl(self, 0) < 0) {
        return NULL;
    }
    Py_RETURN_NONE;
}

/*
 * Park until break_wait / close, or timeout. Host-side only: not CQ reaping.
 * timeout: None = forever, float/int seconds (0 = poll). Returns True if signalled.
 */
PyObject *UringApiRing_wait_idle(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"timeout", NULL};
    PyObject *timeout_obj = Py_None;
    double timeout_sec;
    const double *timeout_ptr;
    int signaled;

    if (!URING_API_PARSE_KEYWORDS("|O", keywords, &timeout_obj)) {
        return NULL;
    }
    if (timeout_obj == Py_None) {
        timeout_ptr = NULL;
    } else {
        if (PyLong_Check(timeout_obj)) {
            long value = PyLong_AsLong(timeout_obj);
            if (value == -1 && PyErr_Occurred()) {
                return NULL;
            }
            timeout_sec = (double)value;
        } else {
            timeout_sec = PyFloat_AsDouble(timeout_obj);
            if (PyErr_Occurred()) {
                return NULL;
            }
        }
        if (timeout_sec < 0.0) {
            PyErr_SetString(PyExc_ValueError, "timeout must be non-negative or None");
            return NULL;
        }
        timeout_ptr = &timeout_sec;
    }

    signaled = UringApiIdlePark_wait(&self->idle, timeout_ptr);
    if (signaled) {
        Py_RETURN_TRUE;
    }
    Py_RETURN_FALSE;
}

int UringApiRing_stop_delivery(UringApiRing *self) {
    bool running;

    Py_BEGIN_CRITICAL_SECTION(self);
    running = delivery_is_running_locked(self);
    self->delivery_stop_requested = true;
    Py_END_CRITICAL_SECTION();

    Py_BEGIN_ALLOW_THREADS;
    pthread_mutex_lock(&self->cqe_mu);
    pthread_cond_broadcast(&self->cqe_cv);
    pthread_mutex_unlock(&self->cqe_mu);
    Py_END_ALLOW_THREADS;

    if (!running) {
        return 0;
    }

    /* force NOP: the unique kernel waiter may be blocked in io_uring_wait_cqe */
    if (UringApiRing_break_wait_impl(self, 1) < 0) {
        return -1;
    }
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

static PyObject *build_completion_result(UringApiRing *ring, UringApiCompletion *completion, int res,
                                         unsigned int flags, unsigned long long leg_index) {
    PyObject *delivered;
    int completion_result;

    /*
     * Multishot delivery contract (public API):
     *   - MORE: fresh shell Completion that copies user_data; armed handle
     *     stays pending for later legs (shells do not re-arm reverse links).
     *     Client take_user_data() on the armed handle defers while aux > 0.
     *   - !MORE (terminal, including cancel / poll_remove): deliver the armed
     *     handle itself so taking user_data breaks reverse-linked waitables.
     */
    if (completion_has_bit(completion, URING_API_C_MULTISHOT) && (flags & IORING_CQE_F_MORE)) {
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
    /* terminal multishot: armed handle; sequence was bumped when this CQE
     * was consumed, so restore the leg index for Python. */
    if (completion_has_bit(completion, URING_API_C_MULTISHOT)) {
        completion->sequence = leg_index;
    }
    if (completion->kind == URING_API_PENDING_SEND_ALL) {
        completion_result = send_all_on_cqe(ring, completion, res, flags);
        if (completion_result < 0) {
            return NULL;
        }
        if (completion_result > 0) {
            Py_RETURN_NONE;
        }
        return Py_NewRef((PyObject *)completion);
    }
    completion_result = UringApiCompletion_complete(completion, res, flags);
    /* negative means we failed while converting the CQE into Python-visible completion state. */
    if (completion_result < 0) {
        return NULL;
    }
    /* positive means the CQE was handled internally (e.g. zero-copy NOTIF). */
    if (completion_result > 0) {
        Py_RETURN_NONE;
    }
    if (skip_success_omit_delivery(ring, completion, res, flags)) {
        Py_RETURN_NONE;
    }

    return Py_NewRef((PyObject *)completion);
}

static int handle_reap_ret(int reap_ret, int eintr_ok) {
    int errnum;

    if (reap_ret >= 0) {
        return 0;
    }
    errnum = normalize_ret_errno(reap_ret);
    if (errnum == EAGAIN || errnum == ETIME || errnum == ETIMEDOUT || (eintr_ok && errnum == EINTR)) {
        return 1;
    }
    errno = errnum;
    PyErr_SetFromErrno(PyExc_OSError);
    return -1;
}

static int apply_ready_cqe(UringApiRing *self, const UringApiStagedCQE *staged, bool deliver,
                           UringApiCompletionCallback c_callback, void *c_callback_user_data, PyObject *py_callback,
                           PyObject **ready_list, int *callback_failed, PyObject **exc_type, PyObject **exc_value,
                           PyObject **exc_tb) {
    if (deliver) {
        PyObject *result = NULL;

        if (package_ready_completion(self, staged->completion, staged->res, staged->flags, staged->leg_index, &result) <
            0) {
            return -1;
        }
        if (result) {
            if (delivery_invoke_one(self, result, c_callback, c_callback_user_data, py_callback) < 0) {
                if (!*callback_failed) {
                    *callback_failed = 1;
                    PyErr_Fetch(exc_type, exc_value, exc_tb);
                } else {
                    PyErr_WriteUnraisable(result);
                }
            }
            Py_DECREF(result);
        }
        if (fill_parked_without_enter(self) < 0) {
            if (*callback_failed) {
                PyErr_WriteUnraisable((PyObject *)self);
                return 0;
            }
            return -1;
        }
        return 0;
    }
    if (append_ready_completion(self, staged->completion, staged->res, staged->flags, staged->leg_index, ready_list) <
        0) {
        return -1;
    }
    return fill_parked_without_enter(self);
}

static PyObject *drain_ready_completions(UringApiRing *self, int timeout_kind, struct __kernel_timespec *timeout,
                                         bool deliver, UringApiCompletionCallback c_callback,
                                         void *c_callback_user_data, PyObject *py_callback) {
    struct io_uring_cqe *cqe = NULL;
    int reap_ret = 0;
    int first = 1;
    int callback_failed = 0;
    PyObject *ready_list = NULL;
    PyObject *exc_type = NULL;
    PyObject *exc_value = NULL;
    PyObject *exc_tb = NULL;

    Py_BEGIN_ALLOW_THREADS;
    reap_ret = reap_one_cqe(self, timeout_kind, timeout, &cqe);
    Py_END_ALLOW_THREADS;

    if (handle_reap_ret(reap_ret, 0) < 0) {
        return NULL;
    }
    if (reap_ret != 0 || !cqe) {
        return drain_empty_result(deliver);
    }

    for (;;) {
        UringApiStagedCQE staged;
        int take;

        if (!first) {
            int peek_ret = io_uring_peek_cqe(&self->ring, &cqe);
            if (peek_ret != 0 || !cqe) {
                break;
            }
        }
        take = consume_cqe(self, cqe, &staged);
        first = 0;
        if (take == CQE_TAKE_SKIP) {
            continue;
        }
        if (apply_ready_cqe(self, &staged, deliver, c_callback, c_callback_user_data, py_callback, &ready_list,
                            &callback_failed, &exc_type, &exc_value, &exc_tb) < 0) {
            Py_XDECREF(ready_list);
            Py_XDECREF(exc_type);
            Py_XDECREF(exc_value);
            Py_XDECREF(exc_tb);
            return NULL;
        }
    }

    if (callback_failed) {
        Py_XDECREF(ready_list);
        PyErr_Restore(exc_type, exc_value, exc_tb);
        return NULL;
    }
    if (deliver) {
        Py_RETURN_NONE;
    }
    if (ready_list) {
        return ready_list;
    }
    return PyList_New(0);
}

/*
 * Flush prepared SQEs so lazy-queued ops can complete.
 * Skipped unless ring_can_submit() (auto_submit and this thread may enter).
 * ring_flush_pending skips io_uring_enter when the SQ has nothing pending.
 */
static int wait_flush_pending_sqes(UringApiRing *self) {
    unsigned char saved_kind;
    int ret = 0;

    if (!ring_can_submit(self)) {
        /* auto_submit off, or non-issuer: leave pending SQEs for submit() */
        return 0;
    }

    Py_BEGIN_CRITICAL_SECTION(self);
    /* inline wait()/poll() and the serve_completions harvest flush. */
    saved_kind = ring_submit_kind_push(self, URING_API_SUBMIT_WAITER);
    if (ring_check_open(self) < 0) {
        ret = -1;
    } else if (drain_parked(self, 1, NULL) < 0) {
        ret = -1;
    } else if (ring_flush_pending(self, NULL) < 0) {
        ret = -1;
    }
    ring_submit_kind_pop(self, saved_kind);
    Py_END_CRITICAL_SECTION();
    return ret;
}

/*
 * Wait order (lazy submit):
 *  1. If auto_submit is on, flush prepared SQEs when this thread may submit
 *     (no-op if SQ empty / non-issuer). Callers need not ring.submit() first.
 *     If auto_submit is off, only already-submitted work is visible.
 *  2. Drain with the caller's timeout (blocking / timed / peek). liburing's
 *     wait_cqe peeks the CQ before entering the kernel when CQEs are ready.
 */
PyObject *UringApiRing_wait_impl(UringApiRing *self, int timeout_kind, struct __kernel_timespec *timeout) {
    UringApiCompletionCallback c_callback = NULL;
    void *c_callback_user_data = NULL;
    PyObject *py_callback = NULL;
    PyObject *ready;
    bool deliver;

    if (ring_check_open(self) < 0) {
        return NULL;
    }
    if (ring_check_client_thread(self) < 0) {
        return NULL;
    }
    if (receive_wait_begin(self) < 0) {
        return NULL;
    }

    /* one sample for this wait: kernel wait can drop the GIL, and Ring.callback
     * may change while receive_state is WAITING. */
    deliver = delivery_snapshot(self, &c_callback, &c_callback_user_data, &py_callback);

    if (wait_flush_pending_sqes(self) < 0) {
        receive_wait_end(self);
        Py_XDECREF(py_callback);
        return NULL;
    }

    ready =
        drain_ready_completions(self, timeout_kind, timeout, deliver, c_callback, c_callback_user_data, py_callback);
    Py_XDECREF(py_callback);
    if (!ready) {
        receive_wait_end(self);
        return NULL;
    }

    receive_wait_end(self);
    return ready;
}

static bool delivery_should_stop(UringApiRing *self) {
    bool stop;

    Py_BEGIN_CRITICAL_SECTION(self);
    stop = self->delivery_stop_requested || self->receive_state != URING_API_RECEIVE_DELIVERING || !self->initialized;
    Py_END_CRITICAL_SECTION();
    return stop;
}

static bool delivery_snapshot(UringApiRing *self, UringApiCompletionCallback *c_callback, void **c_callback_user_data,
                              PyObject **py_callback) {
    Py_BEGIN_CRITICAL_SECTION(self);
    *c_callback = self->c_delivery_callback;
    *c_callback_user_data = self->c_delivery_callback_user_data;
    if (*c_callback) {
        *py_callback = NULL;
    } else {
        *py_callback = Py_XNewRef(self->delivery_callback);
    }
    Py_END_CRITICAL_SECTION();
    return *c_callback != NULL || *py_callback != NULL;
}

static void delivery_request_stop(UringApiRing *self) {
    Py_BEGIN_CRITICAL_SECTION(self);
    self->delivery_stop_requested = true;
    Py_END_CRITICAL_SECTION();
}

static void delivery_request_stop_and_wake(UringApiRing *self) {
    delivery_request_stop(self);
    if (UringApiRing_break_wait_impl(self, 1) < 0) {
        PyErr_WriteUnraisable((PyObject *)self);
    }
}

static int delivery_report_callback_error(UringApiRing *self, PyObject *completion) {
    PyObject *handler = NULL;
    PyObject *context = NULL;
    PyObject *call_result = NULL;
    PyObject *exc_type = NULL;
    PyObject *exc_value = NULL;
    PyObject *exc_tb = NULL;

    PyErr_Fetch(&exc_type, &exc_value, &exc_tb);
    PyErr_NormalizeException(&exc_type, &exc_value, &exc_tb);

    Py_BEGIN_CRITICAL_SECTION(self);
    handler = self->delivery_exception_handler;
    if (handler) {
        Py_INCREF(handler);
    }
    Py_END_CRITICAL_SECTION();

    if (!handler) {
        PyErr_Restore(exc_type, exc_value, exc_tb);
        return -1;
    }

    context = PyDict_New();
    if (!context) {
        goto handler_failed;
    }
    {
        PyObject *message = PyUnicode_FromString("Exception in delivery callback");
        if (!message) {
            goto handler_failed;
        }
        if (PyDict_SetItemString(context, "message", message) < 0) {
            Py_DECREF(message);
            goto handler_failed;
        }
        Py_DECREF(message);
    }
    if (PyDict_SetItemString(context, "exception", exc_value ? exc_value : Py_None) < 0) {
        goto handler_failed;
    }
    if (PyDict_SetItemString(context, "ring", (PyObject *)self) < 0) {
        goto handler_failed;
    }
    if (PyDict_SetItemString(context, "completion", completion) < 0) {
        goto handler_failed;
    }

    call_result = PyObject_CallOneArg(handler, context);
    Py_DECREF(handler);
    handler = NULL;
    Py_DECREF(context);
    context = NULL;
    Py_XDECREF(exc_type);
    Py_XDECREF(exc_value);
    Py_XDECREF(exc_tb);
    if (!call_result) {
        return -1;
    }
    Py_DECREF(call_result);
    return 0;

handler_failed:
    Py_XDECREF(handler);
    Py_XDECREF(context);
    if (!PyErr_Occurred()) {
        PyErr_Restore(exc_type, exc_value, exc_tb);
    } else {
        Py_XDECREF(exc_type);
        Py_XDECREF(exc_value);
        Py_XDECREF(exc_tb);
    }
    return -1;
}

static int delivery_invoke_one(UringApiRing *self, PyObject *completion, UringApiCompletionCallback c_callback,
                               void *c_callback_user_data, PyObject *py_callback) {
    if (c_callback) {
        int callback_ret = c_callback((PyObject *)self, completion, c_callback_user_data);
        if (callback_ret < 0) {
            if (delivery_report_callback_error(self, completion) < 0) {
                return -1;
            }
            return 0;
        }
        return 0;
    }

    PyObject *call_result = PyObject_CallOneArg(py_callback, completion);
    if (!call_result) {
        if (delivery_report_callback_error(self, completion) < 0) {
            return -1;
        }
        return 0;
    }
    Py_DECREF(call_result);
    return 0;
}

static void report_via_exception_handler(UringApiRing *self, const char *message) {
    PyObject *handler = NULL;
    PyObject *context = NULL;
    PyObject *call_result = NULL;
    PyObject *exc_type = NULL;
    PyObject *exc_value = NULL;
    PyObject *exc_tb = NULL;
    PyObject *msg_obj = NULL;

    PyErr_Fetch(&exc_type, &exc_value, &exc_tb);
    PyErr_NormalizeException(&exc_type, &exc_value, &exc_tb);

    Py_BEGIN_CRITICAL_SECTION(self);
    handler = self->delivery_exception_handler;
    if (handler) {
        Py_INCREF(handler);
    }
    Py_END_CRITICAL_SECTION();

    if (!handler) {
        PyErr_Restore(exc_type, exc_value, exc_tb);
        PyErr_WriteUnraisable((PyObject *)self);
        return;
    }

    context = PyDict_New();
    if (!context) {
        goto fail;
    }
    msg_obj = PyUnicode_FromString(message);
    if (!msg_obj) {
        goto fail;
    }
    if (PyDict_SetItemString(context, "message", msg_obj) < 0) {
        goto fail;
    }
    Py_CLEAR(msg_obj);
    if (PyDict_SetItemString(context, "exception", exc_value ? exc_value : Py_None) < 0) {
        goto fail;
    }
    if (PyDict_SetItemString(context, "ring", (PyObject *)self) < 0) {
        goto fail;
    }
    if (PyDict_SetItemString(context, "completion", Py_None) < 0) {
        goto fail;
    }

    call_result = PyObject_CallOneArg(handler, context);
    Py_DECREF(handler);
    handler = NULL;
    Py_DECREF(context);
    context = NULL;
    Py_XDECREF(exc_type);
    Py_XDECREF(exc_value);
    Py_XDECREF(exc_tb);
    if (!call_result) {
        PyErr_WriteUnraisable((PyObject *)self);
        return;
    }
    Py_DECREF(call_result);
    return;

fail:
    Py_XDECREF(handler);
    Py_XDECREF(context);
    Py_XDECREF(msg_obj);
    if (!PyErr_Occurred()) {
        PyErr_Restore(exc_type, exc_value, exc_tb);
    } else {
        Py_XDECREF(exc_type);
        Py_XDECREF(exc_value);
        Py_XDECREF(exc_tb);
    }
    PyErr_WriteUnraisable((PyObject *)self);
}

static void report_nowait_error(UringApiRing *self, int res, unsigned int flags, unsigned int kind, int has_fd,
                                int fd) {
    PyObject *handler = NULL;
    PyObject *context = NULL;
    PyObject *call_result = NULL;
    PyObject *res_obj = NULL;
    PyObject *flags_obj = NULL;
    PyObject *kind_obj = NULL;
    PyObject *fd_obj = NULL;
    PyObject *msg_obj = NULL;

    Py_BEGIN_CRITICAL_SECTION(self);
    handler = self->nowait_error_handler;
    if (handler) {
        Py_INCREF(handler);
    }
    Py_END_CRITICAL_SECTION();

    if (!handler) {
        return;
    }

    context = PyDict_New();
    if (!context) {
        goto fail;
    }
    msg_obj = PyUnicode_FromString("Nowait operation failed");
    if (!msg_obj) {
        goto fail;
    }
    if (PyDict_SetItemString(context, "message", msg_obj) < 0) {
        goto fail;
    }
    Py_CLEAR(msg_obj);
    if (PyDict_SetItemString(context, "ring", (PyObject *)self) < 0) {
        goto fail;
    }
    res_obj = PyLong_FromLong(res);
    if (!res_obj) {
        goto fail;
    }
    if (PyDict_SetItemString(context, "res", res_obj) < 0) {
        goto fail;
    }
    Py_CLEAR(res_obj);
    flags_obj = PyLong_FromUnsignedLong(flags);
    if (!flags_obj) {
        goto fail;
    }
    if (PyDict_SetItemString(context, "flags", flags_obj) < 0) {
        goto fail;
    }
    Py_CLEAR(flags_obj);
    kind_obj = PyLong_FromUnsignedLong(kind);
    if (!kind_obj) {
        goto fail;
    }
    if (PyDict_SetItemString(context, "kind", kind_obj) < 0) {
        goto fail;
    }
    Py_CLEAR(kind_obj);
    if (has_fd) {
        fd_obj = PyLong_FromLong(fd);
        if (!fd_obj) {
            goto fail;
        }
    } else {
        fd_obj = Py_NewRef(Py_None);
    }
    if (PyDict_SetItemString(context, "fd", fd_obj) < 0) {
        goto fail;
    }
    Py_CLEAR(fd_obj);

    call_result = PyObject_CallOneArg(handler, context);
    Py_DECREF(handler);
    handler = NULL;
    Py_DECREF(context);
    context = NULL;
    if (!call_result) {
        report_via_exception_handler(self, "Exception in nowait_error_handler");
        return;
    }
    Py_DECREF(call_result);
    return;

fail:
    Py_XDECREF(handler);
    Py_XDECREF(context);
    Py_XDECREF(msg_obj);
    Py_XDECREF(res_obj);
    Py_XDECREF(flags_obj);
    Py_XDECREF(kind_obj);
    Py_XDECREF(fd_obj);
    if (PyErr_Occurred()) {
        report_via_exception_handler(self, "Exception building nowait_error_handler context");
    }
}

/* Drain returns None when this wait snapshotted a callback (already delivered,
 * or empty). A list is always pull-mode. Do not re-read Ring.callback here. */
PyObject *UringApiRing_wait_finish_with_optional_delivery(UringApiRing *self, PyObject *ready) {
    if (!ready) {
        return NULL;
    }
    if (ready != Py_None) {
        return ready;
    }
    Py_DECREF(ready);
    /* inline wait() callback path: flush prepares done during that drain. */
    if (wait_flush_pending_sqes(self) < 0) {
        return NULL;
    }
    Py_RETURN_NONE;
}

enum {
    CQE_CLAIM_TAKE = 1,
    CQE_CLAIM_WAIT = 2,
    CQE_CLAIM_STOP = 3,
};

static int deliver_staged_one(UringApiRing *self, const UringApiStagedCQE *staged,
                              UringApiCompletionCallback c_callback, void *c_callback_user_data,
                              PyObject *py_callback) {
    PyObject *result = NULL;

    if (package_ready_completion(self, staged->completion, staged->res, staged->flags, staged->leg_index, &result) <
        0) {
        return -1;
    }
    if (!result) {
        return 0;
    }
    if (delivery_invoke_one(self, result, c_callback, c_callback_user_data, py_callback) < 0) {
        Py_DECREF(result);
        return -1;
    }
    Py_DECREF(result);
    return 0;
}

/* Fill parked next-legs / conflict FIFO if the SQ has a slot. Do not enter. */
static int fill_parked_without_enter(UringApiRing *self) {
    int failed = 0;

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else if (drain_parked(self, 0, NULL) < 0) {
        failed = 1;
    }
    Py_END_CRITICAL_SECTION();
    return failed ? -1 : 0;
}

/* One copied CQE: package, callback, then fill any parked SQE that now fits. */
static int process_staged_cqe(UringApiRing *self, const UringApiStagedCQE *staged,
                              UringApiCompletionCallback c_callback, void *c_callback_user_data,
                              PyObject *py_callback) {
    if (deliver_staged_one(self, staged, c_callback, c_callback_user_data, py_callback) < 0) {
        return -1;
    }
    return fill_parked_without_enter(self);
}

/* GIL released. TAKE fills *item; WAIT means this thread is the unique kernel waiter. */
static int cqe_queue_claim(UringApiRing *self, UringApiStagedCQE *item) {
    int result;

    pthread_mutex_lock(&self->cqe_mu);
    for (;;) {
        if (cqe_fifo_pop(&self->cqe_queue, item)) {
            result = CQE_CLAIM_TAKE;
            break;
        }
        if (self->delivery_stop_requested) {
            result = CQE_CLAIM_STOP;
            break;
        }
        if (!self->cqe_waiting) {
            self->cqe_waiting = 1;
            result = CQE_CLAIM_WAIT;
            break;
        }
        pthread_cond_wait(&self->cqe_cv, &self->cqe_mu);
    }
    pthread_mutex_unlock(&self->cqe_mu);
    return result;
}

static int cqe_queue_reserve(UringApiRing *self) {
    int failed = 0;

    pthread_mutex_lock(&self->cqe_mu);
    if (cqe_fifo_ensure(&self->cqe_queue) < 0) {
        failed = 1;
    }
    pthread_mutex_unlock(&self->cqe_mu);
    return failed ? -1 : 0;
}

static int cqe_queue_push(UringApiRing *self, const UringApiStagedCQE *item) {
    int failed = 0;

    pthread_mutex_lock(&self->cqe_mu);
    if (cqe_fifo_push(&self->cqe_queue, item) < 0) {
        failed = 1;
    } else {
        pthread_cond_broadcast(&self->cqe_cv);
    }
    pthread_mutex_unlock(&self->cqe_mu);
    return failed ? -1 : 0;
}

static void cqe_waiter_release(UringApiRing *self) {
    pthread_mutex_lock(&self->cqe_mu);
    self->cqe_waiting = 0;
    pthread_cond_broadcast(&self->cqe_cv);
    pthread_mutex_unlock(&self->cqe_mu);
}

static void cqe_queue_wake(UringApiRing *self) {
    pthread_mutex_lock(&self->cqe_mu);
    pthread_cond_broadcast(&self->cqe_cv);
    pthread_mutex_unlock(&self->cqe_mu);
}

static void cqe_queue_take_all(UringApiRing *self, UringApiCqeFifo *dst) {
    pthread_mutex_lock(&self->cqe_mu);
    *dst = self->cqe_queue;
    memset(&self->cqe_queue, 0, sizeof(self->cqe_queue));
    pthread_cond_broadcast(&self->cqe_cv);
    pthread_mutex_unlock(&self->cqe_mu);
}

static unsigned delivery_worker_count(UringApiRing *self) {
    unsigned n;

    Py_BEGIN_CRITICAL_SECTION(self);
    n = self->delivery_active_workers;
    Py_END_CRITICAL_SECTION();
    return n;
}

static int finish_leftover_cqes(UringApiRing *self, UringApiCqeFifo *fifo, UringApiCompletionCallback c_callback,
                                void *c_callback_user_data, PyObject *py_callback, int already_failed) {
    PyObject *exc_type = NULL;
    PyObject *exc_value = NULL;
    PyObject *exc_tb = NULL;
    UringApiStagedCQE item;
    int first_fail = 0;

    if (already_failed) {
        PyErr_Fetch(&exc_type, &exc_value, &exc_tb);
    }
    while (cqe_fifo_pop(fifo, &item)) {
        if (process_staged_cqe(self, &item, c_callback, c_callback_user_data, py_callback) < 0) {
            if (already_failed || first_fail) {
                PyErr_WriteUnraisable(py_callback != NULL ? py_callback : (PyObject *)self);
            } else {
                first_fail = 1;
                PyErr_Fetch(&exc_type, &exc_value, &exc_tb);
            }
        }
    }
    cqe_fifo_clear(fifo);
    if (already_failed) {
        PyErr_Restore(exc_type, exc_value, exc_tb);
        return 0;
    }
    if (first_fail) {
        PyErr_Restore(exc_type, exc_value, exc_tb);
        return -1;
    }
    return 0;
}

static int waiter_consume_burst(UringApiRing *self, struct io_uring_cqe *first, UringApiCompletionCallback c_callback,
                                void *c_callback_user_data, PyObject *py_callback, int *join_take) {
    unsigned n = delivery_worker_count(self);
    int first_cqe = 1;
    int pushed = 0;
    int callback_failed = 0;
    struct io_uring_cqe *cqe = first;
    PyObject *exc_type = NULL;
    PyObject *exc_value = NULL;
    PyObject *exc_tb = NULL;

    *join_take = 0;
    for (;;) {
        UringApiStagedCQE staged;
        int take;

        if (n > 1 && cqe_queue_reserve(self) < 0) {
            PyErr_NoMemory();
            return -1;
        }
        if (!first_cqe) {
            int peek_ret = io_uring_peek_cqe(&self->ring, &cqe);
            if (peek_ret != 0 || !cqe) {
                break;
            }
        }
        take = consume_cqe(self, cqe, &staged);
        first_cqe = 0;
        if (take != CQE_TAKE_READY) {
            continue;
        }
        if (n <= 1) {
            if (apply_ready_cqe(self, &staged, 1, c_callback, c_callback_user_data, py_callback, NULL, &callback_failed,
                                &exc_type, &exc_value, &exc_tb) < 0) {
                Py_XDECREF(exc_type);
                Py_XDECREF(exc_value);
                Py_XDECREF(exc_tb);
                return -1;
            }
            if (delivery_should_stop(self) && !callback_failed) {
                break;
            }
        } else if (cqe_queue_push(self, &staged) < 0) {
            PyErr_NoMemory();
            return -1;
        } else {
            pushed = 1;
        }
    }
    if (callback_failed) {
        PyErr_Restore(exc_type, exc_value, exc_tb);
        return -1;
    }
    if (n > 1 && pushed) {
        *join_take = 1;
    }
    return 0;
}

PyObject *UringApiRing_serve_completions(UringApiRing *self, PyObject *Py_UNUSED(ignored)) {
    UringApiCompletionCallback c_callback = NULL;
    void *c_callback_user_data = NULL;
    PyObject *py_callback = NULL;
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

    (void)delivery_snapshot(self, &c_callback, &c_callback_user_data, &py_callback);

    while (!wait_failed) {
        UringApiStagedCQE item;
        int claim;

        Py_BEGIN_ALLOW_THREADS;
        claim = cqe_queue_claim(self, &item);
        Py_END_ALLOW_THREADS;

        if (claim == CQE_CLAIM_STOP) {
            break;
        }
        if (claim == CQE_CLAIM_TAKE) {
            if (process_staged_cqe(self, &item, c_callback, c_callback_user_data, py_callback) < 0) {
                wait_failed = true;
                break;
            }
            /* TAKE never io_uring_submit: that unbatches the SQ against the
             * driver. Prepares sit until unique-waiter harvest or host submit. */
            continue;
        }

        for (;;) {
            struct io_uring_cqe *cqe = NULL;
            int reap_ret = 0;
            int join_take = 0;
            int reap_idle;

            if (delivery_should_stop(self)) {
                break;
            }
            /* unique waiter: enter when worker_auto_submit and this thread may
             * submit; otherwise fill parked SQEs only. TAKE never submits. */
            if (self->worker_auto_submit && ring_can_submit(self)) {
                if (wait_flush_pending_sqes(self) < 0) {
                    wait_failed = true;
                    break;
                }
            } else if (fill_parked_without_enter(self) < 0) {
                wait_failed = true;
                break;
            }
            Py_BEGIN_ALLOW_THREADS;
            reap_ret = reap_one_cqe(self, URING_API_WAIT_BLOCKING, NULL, &cqe);
            Py_END_ALLOW_THREADS;
            reap_idle = handle_reap_ret(reap_ret, 1);
            if (reap_idle < 0) {
                wait_failed = true;
                break;
            }
            if (reap_idle || !cqe) {
                if (delivery_should_stop(self)) {
                    break;
                }
                continue;
            }
            if (waiter_consume_burst(self, cqe, c_callback, c_callback_user_data, py_callback, &join_take) < 0) {
                wait_failed = true;
                break;
            }
            if (join_take) {
                break;
            }
        }
        Py_BEGIN_ALLOW_THREADS;
        cqe_waiter_release(self);
        Py_END_ALLOW_THREADS;
        if (wait_failed) {
            break;
        }
    }

    Py_BEGIN_ALLOW_THREADS;
    cqe_queue_wake(self);
    Py_END_ALLOW_THREADS;
    if (delivery_worker_count(self) <= 1) {
        UringApiCqeFifo leftover = {NULL, 0, 0, 0};

        Py_BEGIN_ALLOW_THREADS;
        cqe_queue_take_all(self, &leftover);
        Py_END_ALLOW_THREADS;
        if (finish_leftover_cqes(self, &leftover, c_callback, c_callback_user_data, py_callback, wait_failed) < 0) {
            wait_failed = true;
        }
    }

    Py_XDECREF(py_callback);
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

PyObject *UringApiRing_wait(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"timeout", NULL};
    struct __kernel_timespec timeout;
    PyObject *timeout_obj = Py_None;
    int timeout_kind;
    PyObject *ready;

    if (!URING_API_PARSE_KEYWORDS("|O", keywords, &timeout_obj)) {
        return NULL;
    }
    timeout_kind = parse_timeout(timeout_obj, &timeout);
    if (timeout_kind < 0) {
        return NULL;
    }

    ready = UringApiRing_wait_impl(self, timeout_kind, &timeout);
    return UringApiRing_wait_finish_with_optional_delivery(self, ready);
}

static int cq_is_ready(UringApiRing *self) { return io_uring_cq_ready(&self->ring) != 0; }

/*
 * Same park as wait() (flush, thread rules, unique waiter) without harvest.
 * io_uring_wait_cqe / peek_cqe, no cqe_seen — a later wait() packages the CQE.
 */
int UringApiRing_poll_impl(UringApiRing *self, int timeout_kind, struct __kernel_timespec *timeout, int *ready) {
    struct io_uring_cqe *cqe = NULL;
    int ret;
    int errnum;

    if (ring_check_open(self) < 0) {
        return -1;
    }
    if (ring_check_client_thread(self) < 0) {
        return -1;
    }
    if (receive_wait_begin(self) < 0) {
        return -1;
    }
    if (wait_flush_pending_sqes(self) < 0) {
        receive_wait_end(self);
        return -1;
    }

    Py_BEGIN_ALLOW_THREADS;
    ret = reap_one_cqe(self, timeout_kind, timeout, &cqe);
    Py_END_ALLOW_THREADS;

    receive_wait_end(self);

    if (ret < 0) {
        errnum = normalize_ret_errno(ret);
        if (errnum == EAGAIN || errnum == ETIME || errnum == ETIMEDOUT) {
            *ready = cq_is_ready(self);
            return 0;
        }
        errno = errnum;
        PyErr_SetFromErrno(PyExc_OSError);
        return -1;
    }
    *ready = cqe != NULL || cq_is_ready(self);
    return 0;
}

PyObject *UringApiRing_poll(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"timeout", NULL};
    struct __kernel_timespec timeout;
    PyObject *timeout_obj = Py_None;
    int timeout_kind;
    int ready = 0;

    if (!URING_API_PARSE_KEYWORDS("|O", keywords, &timeout_obj)) {
        return NULL;
    }
    timeout_kind = parse_timeout(timeout_obj, &timeout);
    if (timeout_kind < 0) {
        return NULL;
    }
    if (UringApiRing_poll_impl(self, timeout_kind, timeout_kind == URING_API_WAIT_TIMEOUT ? &timeout : NULL, &ready) <
        0) {
        return NULL;
    }
    if (ready) {
        Py_RETURN_TRUE;
    }
    Py_RETURN_FALSE;
}

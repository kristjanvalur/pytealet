/*
 * Synthetic send-all: next-leg fill, CQE packaging, and fd occupancy.
 */

#include "uring_api_send_all.h"
#include "uring_api_completion.h"
#include "uring_api_core.h"
#include "uring_api_fd_table.h"
#include "uring_api_park.h"
#include "uring_api_probe.h"
#include "uring_api_staging.h"

#include <assert.h>

static Py_ssize_t send_all_remaining(const UringApiCompletionViewState *view_state) {
    if (view_state->offset >= (unsigned long long)view_state->view.len) {
        return 0;
    }
    return view_state->view.len - (Py_ssize_t)view_state->offset;
}

static int send_all_park_continuation(UringApiRing *self, UringApiCompletion *completion) {
    UringApiCompletionViewState *view_state;
    UringApiFdSlot *slot;

    if (completion_has_bit(completion, URING_API_C_SEND_ALL_CONT)) {
        return 0;
    }
    view_state = UringApiCompletion_get_view_state(completion);
    assert(view_state != NULL);
    slot = fd_table_get(self, view_state->fd);
    if (!slot) {
        return -1;
    }
    /* occupy only after a successful park: a failed push must not leave
     * SEND_ALL_CONT/active set with the handle off fill-wait. in-flight
     * send-all already holds active; try_free is then a no-op. */
    if (enqueue_fill_wait(self, completion, 1) < 0) {
        fd_table_try_free(self, slot);
        return -1;
    }
    slot->active = completion;
    completion_set_bit(completion, URING_API_C_SEND_ALL_CONT);
    return 0;
}

int send_all_fill_sqe(UringApiRing *self, UringApiCompletion *completion, struct io_uring_sqe *sqe, int later_leg) {
    UringApiCompletionViewState *view_state;
    unsigned int flags;
    Py_ssize_t remaining;

    view_state = UringApiCompletion_get_view_state(completion);
    assert(view_state != NULL && view_state->has_view);
    if (completion_has_bit(completion, URING_API_C_SEND_ALL_ABANDON)) {
        io_uring_prep_nop(sqe);
        sqe_set_completion(self, sqe, (PyObject *)completion);
        completion_clear_bit(completion, URING_API_C_SEND_ALL_CONT);
        return 0;
    }
    remaining = send_all_remaining(view_state);
    flags = view_state->flags;
    /* later legs set POLL_FIRST only when the 5.19 probe says the ioprio bit exists. */
    if (later_leg && uring_api_recvsend_poll_first_capable()) {
        flags |= IORING_RECVSEND_POLL_FIRST;
    }
    io_uring_prep_send(sqe, view_state->fd, (char *)view_state->view.buf + (Py_ssize_t)view_state->offset,
                       (size_t)remaining, (int)recvsend_msg_flags(flags));
    recvsend_apply_ioprio(sqe, flags);
    sqe_set_completion(self, sqe, (PyObject *)completion);
    completion_clear_bit(completion, URING_API_C_SEND_ALL_CONT);
    return 0;
}

static int send_all_release_active(UringApiRing *self, UringApiCompletion *completion) {
    UringApiCompletionViewState *view_state;
    UringApiFdSlot *slot;
    int failed = 0;

    view_state = UringApiCompletion_get_view_state(completion);
    assert(view_state != NULL);
    Py_BEGIN_CRITICAL_SECTION(self);
    slot = fd_table_lookup(self, view_state->fd);
    if (slot != NULL && slot->active == completion) {
        slot->active = NULL;
        completion_clear_bit(completion, URING_API_C_SEND_ALL_CONT);
        if (slot->fifo.count > 0) {
            fd_table_mark_drain(self, slot);
            if (drain_fd_slot(self, slot, 0, NULL) < 0) {
                /* SQ-full after terminal: deliver the CQE; issuer drain retries */
                if (PyErr_ExceptionMatches(UringApiSubmissionQueueFullError)) {
                    PyErr_Clear();
                } else {
                    failed = 1;
                }
            } else if (ring_can_submit(self) && ring_flush_pending(self, NULL) < 0) {
                failed = 1;
            }
        } else {
            fd_table_try_free(self, slot);
        }
    }
    Py_END_CRITICAL_SECTION();
    return failed ? -1 : 0;
}

static int send_all_try_next_leg(UringApiRing *self, UringApiCompletion *completion) {
    struct io_uring_sqe *sqe;
    int failed = 0;

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else {
        sqe = get_sqe_fill(self, 0, NULL);
        if (!sqe) {
            if (PyErr_ExceptionMatches(UringApiSubmissionQueueFullError)) {
                PyErr_Clear();
                if (send_all_park_continuation(self, completion) < 0) {
                    failed = 1;
                }
            } else {
                failed = 1;
            }
        } else if (send_all_fill_sqe(self, completion, sqe, 1) < 0) {
            failed = 1;
        } else if (self->experimental_send_all_submit_next && ring_can_submit(self) &&
                   ring_flush_pending(self, NULL) < 0) {
            failed = 1;
        }
    }
    Py_END_CRITICAL_SECTION();
    return failed ? -1 : 0;
}

int send_all_on_cqe(UringApiRing *self, UringApiCompletion *completion, int res, unsigned int flags) {
    UringApiCompletionViewState *view_state;
    Py_ssize_t remaining;
    int complete_res;
    int status;

    view_state = UringApiCompletion_get_view_state(completion);
    assert(view_state != NULL && view_state->has_view);
    remaining = send_all_remaining(view_state);
    if (res > 0) {
        assert((Py_ssize_t)res <= remaining);
        view_state->offset += (unsigned long long)res;
        remaining = send_all_remaining(view_state);
    }
    if (res < 0) {
        complete_res = res;
    } else if (remaining == 0) {
        /* Completion.res is CQE-shaped int; clamp. result holds the full offset. */
        if (view_state->offset > (unsigned long long)INT_MAX) {
            complete_res = INT_MAX;
        } else {
            complete_res = (int)view_state->offset;
        }
    } else if (completion_has_bit(completion, URING_API_C_SEND_ALL_ABANDON)) {
        complete_res = -ECANCELED;
        /* a partial success CQE was staged as non-terminal if abandon was set after drain. */
        if (res > 0) {
            uring_api_refcount_mutex_lock(&self->refcount_mutex);
            completion_set_bit(completion, URING_API_C_AUX_DECREF);
            uring_api_refcount_mutex_unlock(&self->refcount_mutex);
        }
    } else if (res == 0) {
        complete_res = -EAGAIN;
    } else {
        if (send_all_try_next_leg(self, completion) < 0) {
            return -1;
        }
        return 1;
    }
    status = UringApiCompletion_complete(completion, complete_res, flags);
    if (status < 0) {
        goto release_keep_err;
    }
    if (complete_res >= 0) {
        PyObject *payload = PyLong_FromUnsignedLongLong(view_state->offset);
        if (!payload) {
            goto release_keep_err;
        }
        Py_XSETREF(completion->result, payload);
    }
    if (send_all_release_active(self, completion) < 0) {
        return -1;
    }
    if (completion_has_bit(completion, URING_API_C_NOWAIT)) {
        if (complete_res < 0) {
            staging_report_nowait_error(self, complete_res, flags, (unsigned int)completion->kind, 1, view_state->fd);
        }
        return 1;
    }
    return 0;

release_keep_err:
    /* kernel CQE is already terminal; unstick the fd even if packaging failed. */
    {
        PyObject *type, *value, *tb;

        PyErr_Fetch(&type, &value, &tb);
        (void)send_all_release_active(self, completion);
        PyErr_Restore(type, value, tb);
    }
    return -1;
}

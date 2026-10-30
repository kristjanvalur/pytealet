/*
 * send_all drain: fill, next-leg re-arm, and CQE handling.
 */

#include "uring_api_send_all.h"
#include "uring_api_completion.h"
#include "uring_api_core.h"
#include "uring_api_dispatch.h"
#include "uring_api_fd_table.h"
#include "uring_api_park.h"
#include "uring_api_prepare.h"
#include "uring_api_probe.h"

#ifndef IORING_RECVSEND_POLL_FIRST
#define IORING_RECVSEND_POLL_FIRST (1U << 0)
#endif

#include <assert.h>

static int send_all_park_continuation(UringApiRing *self, UringApiCompletion *completion);

static unsigned int recvsend_msg_flags(unsigned int flags) { return flags & ~(unsigned int)IORING_RECVSEND_POLL_FIRST; }

static void recvsend_apply_ioprio(struct io_uring_sqe *sqe, unsigned int flags) {
    if (flags & IORING_RECVSEND_POLL_FIRST) {
        sqe->ioprio |= IORING_RECVSEND_POLL_FIRST;
    }
}

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
    ring_note_next_leg_park(self);
    return 0;
}

int send_all_fill_sqe(UringApiRing *self, UringApiCompletion *completion, struct io_uring_sqe *sqe, int later_leg) {
    UringApiCompletionViewState *view_state;
    unsigned int flags;
    Py_ssize_t remaining;

    view_state = UringApiCompletion_get_view_state(completion);
    assert(view_state != NULL && view_state->has_view);
    /* no leg has been submitted. the NOP CQE is what wait() and a
     * callback deliver for this send_all. abandon turns that CQE into
     * -ECANCELED and does not arm another leg. a parked continuation
     * stays a send, and the cancel follows this SQE. */
    if (!later_leg && completion_has_bit(completion, URING_API_C_SEND_ALL_ABANDON)) {
        io_uring_prep_nop(sqe);
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
    return 0;
}

void send_all_commit_leg(UringApiRing *self, UringApiCompletion *completion, int later_leg) {
    /* the caller did not prepare this SQE. note it as housekeeping so
     * the flush publishes when this thread's flag is set. a later leg
     * is a real send, including one filled after abandon. */
    if (later_leg) {
        ring_note_housekeeping_sqe();
        ring_note_next_leg(self);
    }
    completion_clear_bit(completion, URING_API_C_SEND_ALL_CONT);
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
        /* sticky across an empty FIFO. a close already queued is left
         * alone. the fill that copies it out sets the abortive bit and
         * poisons every other op taken off this pipeline. */
        if (completion->res < 0) {
            slot->abortive = 1;
        }
        if (slot->fifo.count > 0) {
            fd_table_mark_drain(self, slot);
            {
                int drain_ret = drain_fd_slot(self, slot, 0, NULL);

                if (drain_ret < 0) {
                    failed = 1;
                }
            }
        } else {
            fd_table_try_free(self, slot);
        }
    }
    Py_END_CRITICAL_SECTION();
    return failed ? -1 : 0;
}

/* partial CQE was staged non-terminal, and this leg is neither submitted
 * nor parked. finish the drain with the error already set so the in-flight
 * ref can drop. res records ENOMEM; the Python exception is the failure. */
static int send_all_abort_unparked(UringApiRing *self, UringApiCompletion *completion) {
    PyObject *type, *value, *tb;

    uring_api_refcount_mutex_lock(&self->refcount_mutex);
    completion_set_bit(completion, URING_API_C_AUX_DECREF);
    uring_api_refcount_mutex_unlock(&self->refcount_mutex);
    PyErr_Fetch(&type, &value, &tb);
    if (UringApiCompletion_complete(completion, -ENOMEM, 0) < 0) {
        Py_XDECREF(type);
        Py_XDECREF(value);
        Py_XDECREF(tb);
    } else {
        PyErr_Restore(type, value, tb);
    }
    PyErr_Fetch(&type, &value, &tb);
    (void)send_all_release_active(self, completion);
    PyErr_Restore(type, value, tb);
    if (!PyErr_Occurred()) {
        PyErr_NoMemory();
    }
    return -1;
}

/* 0: next leg filled or parked. 1: abandon is set, nothing armed.
 * -1: error, and a filled leg may still be in flight. -2: error,
 * nothing filled and nothing parked. get_sqe_try enters when this
 * thread's make_room flag is set. otherwise a full SQ parks. the
 * handler publish, the harvest flush, or submit() publishes a leg
 * that got a slot. */
static int send_all_try_next_leg(UringApiRing *self, UringApiCompletion *completion) {
    struct io_uring_sqe *sqe;
    int failed = 0;
    int unparked = 0;
    int waiting = 0;
    int abandoned = 0;

    Py_BEGIN_CRITICAL_SECTION(self);
    /* same section prepare_cancel holds while it sets abandon and
     * chooses the current leg. a check before this section can lose
     * that store and then arm a leg the cancel is not aimed at. */
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else if (completion_has_bit(completion, URING_API_C_SEND_ALL_ABANDON)) {
        abandoned = 1;
    } else if (self->fill_wait.count > 0) {
        /* copy a head only when it already fits. do not enter to force it:
         * a two-slot head with one free slot would otherwise raise
         * stuck-queue with this leg neither filled nor parked. submit()
         * is what flushes so the head can take both slots. */
        int made_way = fill_wait_make_way(self);

        if (self->fill_wait.count > 0) {
            if (send_all_park_continuation(self, completion) < 0) {
                failed = 1;
                unparked = 1;
            } else {
                waiting = 1;
                if (made_way < 0) {
                    failed = 1;
                }
            }
        } else if (made_way < 0) {
            failed = 1;
        }
    }
    if (!failed && !waiting && !abandoned) {
        /* on_sq_full still makes SQ room when this thread's flag is set.
         * park only if we cannot. fill-wait is empty, so nothing is ahead. */
        {
            /* no timeout: same single-slot get as before, including its
             * make-room flush. a timeout must not take one slot unless the
             * timer slot is reserved too. */
            int got;

            if (completion->has_link_timeout) {
                int reserved = reserve_link_timeout_sqes(self, 0, NULL);

                if (reserved < 0) {
                    got = -1;
                } else if (reserved != 1) {
                    got = 0;
                } else {
                    got = get_sqe_try(self, 0, NULL, &sqe);
                }
            } else {
                got = get_sqe_try(self, 0, NULL, &sqe);
            }
            if (got < 0) {
                failed = 1;
            } else if (got == 0) {
                if (send_all_park_continuation(self, completion) < 0) {
                    failed = 1;
                    unparked = 1;
                }
            } else if (send_all_fill_sqe(self, completion, sqe, 1) < 0) {
                failed = 1;
            } else if (completion->has_link_timeout && fill_link_timeout(self, completion, sqe) < 0) {
                /* op slot is already rolled back and the completion was not
                 * attached. a successful park is resumed later, so it must
                 * not also fail the wait. */
                PyObject *type, *value, *tb;

                PyErr_Fetch(&type, &value, &tb);
                if (send_all_park_continuation(self, completion) == 0) {
                    Py_XDECREF(type);
                    Py_XDECREF(value);
                    Py_XDECREF(tb);
                } else {
                    if (PyErr_Occurred()) {
                        Py_XDECREF(type);
                        Py_XDECREF(value);
                        Py_XDECREF(tb);
                    } else {
                        PyErr_Restore(type, value, tb);
                    }
                    failed = 1;
                    unparked = 1;
                }
            } else {
                sqe_set_completion(self, sqe, (PyObject *)completion);
                send_all_commit_leg(self, completion, 1);
            }
        }
    }
    Py_END_CRITICAL_SECTION();
    if (abandoned) {
        return 1;
    }
    if (unparked) {
        return -2;
    }
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
        /* visible already: do not arm. the arm path below repeats this
         * under the ring section, which is what meets prepare_cancel. */
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
        /* try_next_leg holds the ring section across the abandon check
         * and the arm. 1 means cancel won: finish, do not publish a leg. */
        int next = send_all_try_next_leg(self, completion);

        if (next == -2) {
            return send_all_abort_unparked(self, completion);
        }
        if (next < 0) {
            return -1;
        }
        if (next == 0) {
            return 1;
        }
        complete_res = -ECANCELED;
        /* a partial success CQE was staged as non-terminal if abandon was set after drain. */
        if (res > 0) {
            uring_api_refcount_mutex_lock(&self->refcount_mutex);
            completion_set_bit(completion, URING_API_C_AUX_DECREF);
            uring_api_refcount_mutex_unlock(&self->refcount_mutex);
        }
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
    if (skip_success_omit_delivery(self, completion, complete_res, flags)) {
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

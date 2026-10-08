/*
 * SQE fill for the _uring_api Ring type.
 */

#include "uring_api_prepare.h"
#include "uring_api_bufgroup.h"
#include "uring_api_completion.h"
#include "uring_api_core.h"
#include "uring_api_fd_table.h"
#include "uring_api_park.h"
#include "uring_api_probe.h"
#include "uring_api_send_all.h"
#include "uring_api_statx.h"

#include <time.h>

#ifndef IORING_RECVSEND_POLL_FIRST
#define IORING_RECVSEND_POLL_FIRST (1U << 0)
#endif

/* POLL_FIRST is sqe->ioprio, not MSG_* msg_flags.
 * Bit 0 is also MSG_OOB: that value is poll-first, not OOB. */
static unsigned int recvsend_msg_flags(unsigned int flags) { return flags & ~(unsigned int)IORING_RECVSEND_POLL_FIRST; }

static void recvsend_apply_ioprio(struct io_uring_sqe *sqe, unsigned int flags) {
    if (flags & IORING_RECVSEND_POLL_FIRST) {
        sqe->ioprio |= IORING_RECVSEND_POLL_FIRST;
    }
}

void take_in_flight_ref(UringApiRing *self, UringApiCompletion *completion) {
    Py_INCREF(completion);
    completion->aux_lock = &self->refcount_mutex;
    ring_pending_inc(self);
}

/* one successful waitable prepare() → one pending_count, wherever the handle
 * lands (SQ or conflict FIFO). skip_all (tagged SQE) is excluded: success
 * may skip the CQE, so there is nothing to decrement later. send_all always
 * counts (re-arm, in-flight ref). skip_success keeps the Completion* and
 * counts until the CQE retires that ref. */
int completion_counts_pending(const UringApiCompletion *completion) {
    if (completion->kind == URING_API_PENDING_SEND_ALL) {
        return 1;
    }
    return !completion_has_bit(completion, URING_API_C_SKIP_ALL);
}

static int constructed_kind_ready(UringApiCompletion *completion) {
    UringApiCompletionViewState *view_state;
    UringApiCompletionViewSockaddrState *view_sockaddr_state;
    UringApiCompletionMsgState *msg_state;
    UringApiCompletionSockaddrState *sockaddr_state;

    switch (completion->kind) {
    case URING_API_PENDING_SEND:
    case URING_API_PENDING_SEND_ZC:
    case URING_API_PENDING_SEND_ALL:
    case URING_API_PENDING_RECV:
    case URING_API_PENDING_READ:
    case URING_API_PENDING_WRITE:
        view_state = UringApiCompletion_get_view_state(completion);
        return view_state != NULL && view_state->fd >= 0;
    case URING_API_PENDING_SENDTO:
        view_sockaddr_state = UringApiCompletion_get_view_sockaddr_state(completion);
        return view_sockaddr_state != NULL && view_sockaddr_state->fd >= 0;
    case URING_API_PENDING_RECVMSG:
    case URING_API_PENDING_SENDMSG:
    case URING_API_PENDING_SENDMSG_ZC:
        msg_state = UringApiCompletion_get_msg_state(completion);
        return msg_state != NULL && msg_state->fd >= 0;
    case URING_API_PENDING_CONNECT:
        sockaddr_state = UringApiCompletion_get_sockaddr_state(completion);
        return sockaddr_state != NULL && sockaddr_state->fd >= 0;
    case URING_API_PENDING_RECV_BUF:
    case URING_API_PENDING_RECV_MULTISHOT: {
        UringApiCompletionBufGroupState *buf_group_state = UringApiCompletion_get_buf_group_state(completion);
        return buf_group_state != NULL && buf_group_state->fd >= 0;
    }
    case URING_API_PENDING_OPENAT: {
        UringApiCompletionPathState *path_state = UringApiCompletion_get_path_state(completion);
        return path_state != NULL && path_state->constructed;
    }
    case URING_API_PENDING_STATX: {
        UringApiCompletionStatxState *statx_state = UringApiCompletion_get_statx_state(completion);
        return statx_state != NULL && statx_state->constructed;
    }
    case URING_API_PENDING_STATX_FDSIZE: {
        UringApiCompletionStatxFdsizeState *statx_fdsize_state = UringApiCompletion_get_statx_fdsize_state(completion);
        return statx_fdsize_state != NULL && statx_fdsize_state->constructed;
    }
    case URING_API_PENDING_ACCEPT:
    case URING_API_PENDING_POLL:
    case URING_API_PENDING_POLL_MULTISHOT:
    case URING_API_PENDING_CLOSE:
    case URING_API_PENDING_SHUTDOWN:
    case URING_API_PENDING_SOCKET: {
        UringApiCompletionScalarState *scalar_state = UringApiCompletion_get_scalar_state(completion);
        return scalar_state != NULL && scalar_state->constructed;
    }
    case URING_API_PENDING_CANCEL:
    case URING_API_PENDING_POLL_REMOVE:
        return completion->cancel_target != NULL;
    default:
        return 0;
    }
}

/* Nowait SQE identity: tagged token, optional CQE_SKIP_SUCCESS. No Completion*. */
static int stamp_nowait_sqe(UringApiRing *self, struct io_uring_sqe *sqe, unsigned int kind, int fd) {
    io_uring_sqe_set_data64(sqe, uring_api_make_nowait_user_data(kind, fd));
    if (self->ring.features & IORING_FEAT_CQE_SKIP) {
        sqe->flags |= IOSQE_CQE_SKIP_SUCCESS;
    }
    return 0;
}

/* Caller holds the ring CS and has checked the ring is open.
 * 1 posted, 0 caller must park a Completion, -1 error. */
static int nowait_post_or_defer(UringApiRing *self, int must_park, unsigned int kind, int advisory_fd,
                                void (*prep)(struct io_uring_sqe *sqe, void *arg), void *arg) {
    struct io_uring_sqe *sqe;
    int got;

    if (must_park) {
        return 0;
    }
    if (drain_parked(self, 0, NULL) < 0) {
        return -1;
    }
    got = get_sqe_try(self, 0, NULL, &sqe);
    if (got < 0) {
        return -1;
    }
    if (got == 0) {
        return 0;
    }
    prep(sqe, arg);
    stamp_nowait_sqe(self, sqe, kind, advisory_fd);
    return 1;
}

/* nowait cancel of a silenced recv_multishot posts nothing a caller waits
 * on. every other cancel must be submitted by the next wait(), or a recv
 * (or the cancel itself) stays armed forever. */
static int nowait_cancel_is_sq_waitable(const UringApiCompletion *target) {
    if (target->kind == URING_API_PENDING_RECV_MULTISHOT && completion_has_bit(target, URING_API_C_NO_DELIVER_MULTI)) {
        return 0;
    }
    return 1;
}

static void prep_cancel_sqe(struct io_uring_sqe *sqe, void *arg) { io_uring_prep_cancel(sqe, arg, 0); }

static void prep_poll_remove_sqe(struct io_uring_sqe *sqe, void *arg) {
    io_uring_prep_poll_remove(sqe, (unsigned long long)(uintptr_t)arg);
}

static void prep_close_sqe(struct io_uring_sqe *sqe, void *arg) { io_uring_prep_close(sqe, *(int *)arg); }

struct nowait_shutdown_arg {
    int fd;
    int how;
};

static void prep_shutdown_sqe(struct io_uring_sqe *sqe, void *arg) {
    struct nowait_shutdown_arg *prep = arg;

    io_uring_prep_shutdown(sqe, prep->fd, prep->how);
}

int try_direct_cancel_nowait(UringApiRing *self, UringApiCompletion *target) {
    int result;

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        result = -1;
    } else {
        /* same order as prepare_one: abandon before drain, so a parked next leg is a NOP */
        if (target->kind == URING_API_PENDING_SEND_ALL) {
            completion_set_bit(target, URING_API_C_SEND_ALL_ABANDON);
        }
        result = nowait_post_or_defer(self, nowait_cancel_must_park(self, target),
                                      (unsigned int)URING_API_PENDING_CANCEL, -1, prep_cancel_sqe, target);
        if (result > 0 && nowait_cancel_is_sq_waitable(target)) {
            self->sq_waitable = true;
        }
    }
    Py_END_CRITICAL_SECTION();
    return result;
}

int try_direct_poll_remove_nowait(UringApiRing *self, UringApiCompletion *target) {
    int result;

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        result = -1;
    } else {
        /* poll has no conflict fd; only a full SQ on a non-submit thread defers */
        result = nowait_post_or_defer(self, 0, (unsigned int)URING_API_PENDING_POLL_REMOVE, -1, prep_poll_remove_sqe,
                                      target);
    }
    Py_END_CRITICAL_SECTION();
    return result;
}

int try_direct_close_nowait(UringApiRing *self, int fd) {
    int result;

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        result = -1;
    } else {
        result = nowait_post_or_defer(self, nowait_fd_op_must_park(self, fd), (unsigned int)URING_API_PENDING_CLOSE, fd,
                                      prep_close_sqe, &fd);
    }
    Py_END_CRITICAL_SECTION();
    return result;
}

int try_direct_shutdown_nowait(UringApiRing *self, int fd, int how) {
    struct nowait_shutdown_arg prep;
    int result;

    prep.fd = fd;
    prep.how = how;
    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        result = -1;
    } else {
        result = nowait_post_or_defer(self, nowait_fd_op_must_park(self, fd), (unsigned int)URING_API_PENDING_SHUTDOWN,
                                      fd, prep_shutdown_sqe, &prep);
    }
    Py_END_CRITICAL_SECTION();
    return result;
}

static int nowait_kind_ok(UringApiPendingKind kind) {
    return kind == URING_API_PENDING_CLOSE || kind == URING_API_PENDING_SHUTDOWN || kind == URING_API_PENDING_CANCEL ||
           kind == URING_API_PENDING_POLL_REMOVE || kind == URING_API_PENDING_SEND_ALL;
}

int nowait_advisory_fd(UringApiCompletion *completion) {
    UringApiCompletionScalarState *scalar_state;
    UringApiCompletionViewState *view_state;

    if (completion->kind == URING_API_PENDING_CLOSE || completion->kind == URING_API_PENDING_SHUTDOWN) {
        scalar_state = UringApiCompletion_get_scalar_state(completion);
        assert(scalar_state != NULL);
        return scalar_state->fd;
    }
    if (completion->kind == URING_API_PENDING_SEND_ALL) {
        view_state = UringApiCompletion_get_view_state(completion);
        assert(view_state != NULL);
        return view_state->fd;
    }
    return -1;
}

/* same bound as get_sqe_loop: a dead SQPOLL thread must not wait forever. */
#define URING_API_SQE_WAIT_TIMEOUT_SEC 5
#define URING_API_SQE_WAIT_EINVAL_BACKOFF_US 1000

static int64_t monotonic_ms(void) {
    struct timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return -1;
    }
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static void set_sqe_slot_stuck_error(void) {
    PyErr_SetString(PyExc_RuntimeError, "failed to obtain an io_uring SQE slot after flushing "
                                        "(submission queue stuck; with IORING_SETUP_SQPOLL the "
                                        "poller may be dead or hung)");
}

/* SQPOLL only. caller holds the ring CS and has not taken either slot.
 * 0: wait returned or was interrupted; caller checks space again.
 * -1: dead poller or wait error. */
static int sqpoll_wait_for_two_slots(UringApiRing *self, int64_t *deadline_ms) {
    int64_t now_ms;
    int wait_ret;
    int errnum;

    if (*deadline_ms < 0) {
        now_ms = monotonic_ms();
        if (now_ms < 0) {
            PyErr_SetFromErrno(PyExc_OSError);
            return -1;
        }
        *deadline_ms = now_ms + (int64_t)URING_API_SQE_WAIT_TIMEOUT_SEC * 1000;
    } else {
        now_ms = monotonic_ms();
        if (now_ms < 0) {
            PyErr_SetFromErrno(PyExc_OSError);
            return -1;
        }
        if (now_ms >= *deadline_ms) {
            set_sqe_slot_stuck_error();
            return -1;
        }
    }
    Py_BEGIN_ALLOW_THREADS;
    wait_ret = io_uring_sqring_wait(&self->ring);
    Py_END_ALLOW_THREADS;
    if (wait_ret < 0) {
        errnum = normalize_ret_errno(wait_ret);
        if (errnum == EINTR) {
            return 0;
        }
        if (errnum == EINVAL) {
            Py_BEGIN_ALLOW_THREADS;
            (void)usleep(URING_API_SQE_WAIT_EINVAL_BACKOFF_US);
            Py_END_ALLOW_THREADS;
            return 0;
        }
        errno = errnum;
        PyErr_SetFromErrno(PyExc_OSError);
        return -1;
    }
    return 0;
}

/* 1: two SQ slots are free. 0: caller parks (not the submit thread, or
 * auto_submit is off and this prepare must not enter). -1: error.
 * 2: from_parked leftover drain, quiet full.
 * does not take a slot. a shortfall must not leave a lone IOSQE_IO_LINK.
 * SQPOLL waits for the pair the way get_sqe_loop waits for one slot. */
int reserve_link_timeout_sqes(UringApiRing *self, int from_parked, int flush_if_full, int *submitted_out) {
    int noted_full = 0;
    int flushes = 0;
    int sqpoll = (self->setup_flags & IORING_SETUP_SQPOLL) != 0;
    int64_t deadline_ms = -1;

    for (;;) {
        unsigned int space = io_uring_sq_space_left(&self->ring);

        if (space >= 2) {
            return 1;
        }
        if (!noted_full) {
            ring_note_sq_full(self);
            noted_full = 1;
        }
        if (!from_parked && ring_check_submit_thread(self, 0) < 0) {
            return 0;
        }
        /* same gate as get_sqe_try: no enter, so the caller parks or raises.
         * a send_all continuation parks. prepare() on the submit thread raises. */
        if (!flush_if_full && !self->auto_submit) {
            return from_parked ? 2 : 0;
        }
        if (ring_check_submit_thread(self, 0) < 0) {
            return from_parked ? 2 : 0;
        }
        {
            unsigned char saved_kind = ring_submit_kind_push(self, URING_API_SUBMIT_SQ_FULL);
            int flush_ret = ring_flush_pending(self, submitted_out);

            ring_submit_kind_pop(self, saved_kind);
            if (flush_ret < 0) {
                return -1;
            }
        }
        flushes++;
        if (io_uring_sq_space_left(&self->ring) >= 2) {
            return 1;
        }
        /* leftover drain stays quiet-full. a user prepare on a non-SQPOLL
         * ring that is still full after enter raises SubmissionQueueFull.
         * SQPOLL waits, matching get_sqe_loop. */
        if (from_parked) {
            return 2;
        }
        if (!sqpoll) {
            PyErr_SetString(UringApiSubmissionQueueFullError, "no submission queue entries available");
            return -1;
        }
        if (flushes < 2) {
            continue;
        }
        if (sqpoll_wait_for_two_slots(self, &deadline_ms) < 0) {
            return -1;
        }
    }
}

/* 0 parked, 1 quiet full (from_parked), -1 error. the SQ cannot take this prepare. */
static int prepare_sq_short(UringApiRing *self, UringApiCompletion *completion, int from_parked,
                            UringApiFdSlot *send_all_slot) {
    if (!from_parked && ring_check_submit_thread(self, 0) < 0) {
        if (enqueue_fill_wait(self, completion, 0) < 0) {
            if (send_all_slot) {
                fd_table_try_free(self, send_all_slot);
            }
            return -1;
        }
        if (send_all_slot) {
            send_all_slot->active = completion;
        }
        return 0;
    }
    if (send_all_slot) {
        fd_table_try_free(self, send_all_slot);
    }
    if (!from_parked) {
        PyErr_SetString(UringApiSubmissionQueueFullError, "no submission queue entries available");
        return -1;
    }
    return 1;
}

/* op SQE already filled; its slot was reserved with the timeout slot.
 * flags 0 is relative CLOCK_MONOTONIC. the timeout SQE owns a copy of
 * link_ts: sqe->addr and user_data (tag 10 in the low bits) both refer to
 * that copy. the CQE frees it. the completion is not referenced. */
int fill_link_timeout(UringApiRing *self, UringApiCompletion *completion, struct io_uring_sqe *op_sqe) {
    struct io_uring_sqe *timeout_sqe;
    struct __kernel_timespec *ts;

    assert(completion->has_link_timeout);
    assert(op_sqe != NULL);

    ts = PyMem_Malloc(sizeof(*ts));
    if (ts == NULL) {
        /* drop the op so it is not submitted alone. */
        self->ring.sq.sqe_tail--;
        PyErr_NoMemory();
        return -1;
    }
    *ts = completion->link_ts;

    timeout_sqe = io_uring_get_sqe(&self->ring);
    if (timeout_sqe == NULL) {
        /* space_left promised this slot. drop the op so it is not submitted alone. */
        PyMem_Free(ts);
        self->ring.sq.sqe_tail--;
        assert(timeout_sqe != NULL);
        PyErr_SetString(UringApiSubmissionQueueFullError, "no submission queue entries available");
        return -1;
    }
    ring_note_sqe(self);
    /* prep cleared flags. the timeout SQE must not itself be IOSQE_IO_LINK. */
    op_sqe->flags |= IOSQE_IO_LINK;
    io_uring_prep_link_timeout(timeout_sqe, ts, 0);
    io_uring_sqe_set_data64(timeout_sqe, uring_api_link_timeout_user_data(ts));
    return 0;
}

/* Caller holds the ring critical section. On success the completion is in the
 * kernel SQ, on fill-wait, or on that fd's conflict FIFO. Waitable ops,
 * send_all, and skip_success take the in-flight ref at SQ fill or park
 * enqueue, and hold it until CQE delivery. skip_all (except send_all)
 * stamps a tagged SQE and drops the Completion. Kind is checked before
 * prepared so a non-constructed handle reports "not constructed", not
 * "already prepared". A link timeout reserves two SQ slots together so a
 * shortfall cannot publish a lone IO_LINK. The timeout SQE is not an
 * in-flight ref. Any kind is linked when timeout is set. */
int prepare_one_constructed_ex(UringApiRing *self, UringApiCompletion *completion, int from_parked, int flush_if_full,
                               int *submitted_out) {
    UringApiCompletionViewState *view_state;
    UringApiCompletionViewSockaddrState *view_sockaddr_state;
    UringApiCompletionMsgState *msg_state;
    UringApiCompletionSockaddrState *sockaddr_state;
    struct io_uring_sqe *sqe;
    UringApiFdSlot *send_all_slot = NULL;
    int send_all_later_leg = 0;

    if (!constructed_kind_ready(completion)) {
        PyErr_SetString(PyExc_ValueError, "prepare() only accepts constructed completions");
        return -1;
    }
    if (!from_parked && completion_is_accepted(completion)) {
        PyErr_SetString(PyExc_ValueError, "completion is already prepared");
        return -1;
    }
    if (completion_has_bit(completion, URING_API_C_SKIP_SUCCESS) && !nowait_kind_ok(completion->kind)) {
        PyErr_SetString(PyExc_ValueError,
                        "skip_success is only valid for close, shutdown, cancel, poll_remove, and send_all");
        return -1;
    }

    /* abandon before leftover drain so a parked next-leg is a NOP, not another send. */
    if (!from_parked && completion->kind == URING_API_PENDING_CANCEL && completion->cancel_target != NULL &&
        ((UringApiCompletion *)completion->cancel_target)->kind == URING_API_PENDING_SEND_ALL) {
        completion_set_bit((UringApiCompletion *)completion->cancel_target, URING_API_C_SEND_ALL_ABANDON);
    }

    /* conflicting ops park even if leftover drain would hit a full SQ. */
    if (!from_parked && should_enqueue_conflict(self, completion, NULL)) {
        return enqueue_conflict(self, completion);
    }
    /* leftover drain so parked next-legs take this SQE; from_parked must not recurse. */
    if (!from_parked && drain_parked(self, 0, NULL) < 0) {
        return -1;
    }

    if (completion->kind == URING_API_PENDING_SEND_ALL) {
        view_state = UringApiCompletion_get_view_state(completion);
        assert(view_state != NULL);
        send_all_slot = fd_table_get(self, view_state->fd);
        if (!send_all_slot) {
            return -1;
        }
    }

    /* two slots before either is taken, for every kind. a shortfall parks
     * or fails with the queue unchanged. */
    if (completion->has_link_timeout) {
        int reserved = reserve_link_timeout_sqes(self, from_parked, flush_if_full, submitted_out);

        if (reserved < 0) {
            if (send_all_slot) {
                fd_table_try_free(self, send_all_slot);
            }
            return -1;
        }
        if (reserved != 1) {
            return prepare_sq_short(self, completion, from_parked, send_all_slot);
        }
    }

    {
        int got = get_sqe_try(self, flush_if_full, submitted_out, &sqe);

        if (got < 0) {
            if (send_all_slot) {
                fd_table_try_free(self, send_all_slot);
            }
            return -1;
        }
        if (got == 0) {
            return prepare_sq_short(self, completion, from_parked, send_all_slot);
        }
    }
    switch (completion->kind) {
    case URING_API_PENDING_SEND:
        view_state = UringApiCompletion_get_view_state(completion);
        assert(view_state != NULL && view_state->has_view);
        io_uring_prep_send(sqe, view_state->fd, view_state->view.buf, (size_t)view_state->view.len,
                           (int)recvsend_msg_flags(view_state->flags));
        recvsend_apply_ioprio(sqe, view_state->flags);
        break;
    case URING_API_PENDING_SEND_ALL:
        send_all_later_leg = completion_has_bit(completion, URING_API_C_SEND_ALL_CONT);
        if (send_all_fill_sqe(self, completion, sqe, send_all_later_leg) < 0) {
            /* do not submit a half-filled slot, and do not leave a lone link. */
            self->ring.sq.sqe_tail--;
            if (send_all_slot) {
                fd_table_try_free(self, send_all_slot);
            }
            return -1;
        }
        break;
    case URING_API_PENDING_SEND_ZC:
        view_state = UringApiCompletion_get_view_state(completion);
        assert(view_state != NULL && view_state->has_view);
        io_uring_prep_send_zc(sqe, view_state->fd, view_state->view.buf, (size_t)view_state->view.len,
                              (int)recvsend_msg_flags(view_state->flags), view_state->zc_flags);
        recvsend_apply_ioprio(sqe, view_state->flags);
        break;
    case URING_API_PENDING_RECV:
        view_state = UringApiCompletion_get_view_state(completion);
        assert(view_state != NULL && view_state->has_view);
        io_uring_prep_recv(sqe, view_state->fd, view_state->view.buf, (size_t)view_state->view.len,
                           (int)recvsend_msg_flags(view_state->flags));
        recvsend_apply_ioprio(sqe, view_state->flags);
        break;
    case URING_API_PENDING_READ:
        view_state = UringApiCompletion_get_view_state(completion);
        assert(view_state != NULL && view_state->has_view);
        io_uring_prep_read(sqe, view_state->fd, view_state->view.buf, (unsigned)view_state->view.len,
                           (__u64)view_state->offset);
        break;
    case URING_API_PENDING_WRITE:
        view_state = UringApiCompletion_get_view_state(completion);
        assert(view_state != NULL && view_state->has_view);
        io_uring_prep_write(sqe, view_state->fd, view_state->view.buf, (unsigned)view_state->view.len,
                            (__u64)view_state->offset);
        break;
    case URING_API_PENDING_SENDTO:
        view_sockaddr_state = UringApiCompletion_get_view_sockaddr_state(completion);
        assert(view_sockaddr_state != NULL && view_sockaddr_state->has_view);
        io_uring_prep_sendto(sqe, view_sockaddr_state->fd, view_sockaddr_state->view.buf,
                             (size_t)view_sockaddr_state->view.len, (int)recvsend_msg_flags(view_sockaddr_state->flags),
                             (struct sockaddr *)&view_sockaddr_state->addr, view_sockaddr_state->addrlen);
        recvsend_apply_ioprio(sqe, view_sockaddr_state->flags);
        break;
    case URING_API_PENDING_RECVMSG:
        msg_state = UringApiCompletion_get_msg_state(completion);
        assert(msg_state != NULL && msg_state->has_view);
        io_uring_prep_recvmsg(sqe, msg_state->fd, &msg_state->msg, (int)recvsend_msg_flags(msg_state->flags));
        recvsend_apply_ioprio(sqe, msg_state->flags);
        break;
    case URING_API_PENDING_SENDMSG:
        msg_state = UringApiCompletion_get_msg_state(completion);
        assert(msg_state != NULL && msg_state->has_view);
        io_uring_prep_sendmsg(sqe, msg_state->fd, &msg_state->msg, recvsend_msg_flags(msg_state->flags));
        recvsend_apply_ioprio(sqe, msg_state->flags);
        break;
    case URING_API_PENDING_SENDMSG_ZC:
        msg_state = UringApiCompletion_get_msg_state(completion);
        assert(msg_state != NULL && msg_state->has_view);
        io_uring_prep_sendmsg_zc(sqe, msg_state->fd, &msg_state->msg, recvsend_msg_flags(msg_state->flags));
        recvsend_apply_ioprio(sqe, msg_state->flags);
        break;
    case URING_API_PENDING_CONNECT:
        sockaddr_state = UringApiCompletion_get_sockaddr_state(completion);
        assert(sockaddr_state != NULL);
        io_uring_prep_connect(sqe, sockaddr_state->fd, (struct sockaddr *)&sockaddr_state->addr,
                              sockaddr_state->addrlen);
        break;
    case URING_API_PENDING_RECV_BUF: {
        UringApiCompletionBufGroupState *buf_group_state = UringApiCompletion_get_buf_group_state(completion);
        UringApiBufGroup *buf_group;

        assert(buf_group_state != NULL && buf_group_state->buf_group != NULL);
        buf_group = (UringApiBufGroup *)buf_group_state->buf_group;
        io_uring_prep_recv(sqe, buf_group_state->fd, NULL, (size_t)buf_group->buffer_size,
                           (int)recvsend_msg_flags(buf_group_state->flags));
        recvsend_apply_ioprio(sqe, buf_group_state->flags);
        sqe->flags |= IOSQE_BUFFER_SELECT;
        sqe->buf_group = buf_group->group_id;
        break;
    }
    case URING_API_PENDING_RECV_MULTISHOT: {
        UringApiCompletionBufGroupState *buf_group_state = UringApiCompletion_get_buf_group_state(completion);
        UringApiBufGroup *buf_group;

        assert(buf_group_state != NULL && buf_group_state->buf_group != NULL);
        buf_group = (UringApiBufGroup *)buf_group_state->buf_group;
        io_uring_prep_recv_multishot(sqe, buf_group_state->fd, NULL, 0,
                                     (int)recvsend_msg_flags(buf_group_state->flags));
        /* kernel can strand MORE with no EOF CQE; do not set POLL_FIRST */
        sqe->flags |= IOSQE_BUFFER_SELECT;
        sqe->buf_group = buf_group->group_id;
        break;
    }
    case URING_API_PENDING_OPENAT: {
        UringApiCompletionPathState *path_state = UringApiCompletion_get_path_state(completion);

        assert(path_state != NULL && path_state->path != NULL);
        io_uring_prep_openat(sqe, path_state->dfd, path_state->path, path_state->flags, path_state->mode);
        break;
    }
    case URING_API_PENDING_STATX: {
        UringApiCompletionStatxState *statx_state = UringApiCompletion_get_statx_state(completion);

        assert(statx_state != NULL && statx_state->path != NULL && statx_state->has_view);
        io_uring_prep_statx(sqe, statx_state->dfd, statx_state->path, statx_state->flags, statx_state->mask,
                            (struct statx *)statx_state->view.buf);
        break;
    }
    case URING_API_PENDING_STATX_FDSIZE: {
        UringApiCompletionStatxFdsizeState *statx_fdsize_state = UringApiCompletion_get_statx_fdsize_state(completion);

        assert(statx_fdsize_state != NULL);
        io_uring_prep_statx(sqe, statx_fdsize_state->fd, "", URING_API_AT_EMPTY_PATH, URING_API_STATX_SIZE_MASK,
                            (struct statx *)statx_fdsize_state->buf);
        break;
    }
    case URING_API_PENDING_ACCEPT: {
        UringApiCompletionScalarState *scalar_state = UringApiCompletion_get_scalar_state(completion);

        assert(scalar_state != NULL);
        if (completion_has_bit(completion, URING_API_C_MULTISHOT)) {
            io_uring_prep_multishot_accept(sqe, scalar_state->fd, NULL, NULL, scalar_state->flags);
        } else {
            io_uring_prep_accept(sqe, scalar_state->fd, NULL, NULL, scalar_state->flags);
        }
        break;
    }
    case URING_API_PENDING_POLL: {
        UringApiCompletionScalarState *scalar_state = UringApiCompletion_get_scalar_state(completion);

        assert(scalar_state != NULL);
        io_uring_prep_poll_add(sqe, scalar_state->fd, scalar_state->poll_mask);
        break;
    }
    case URING_API_PENDING_POLL_MULTISHOT: {
        UringApiCompletionScalarState *scalar_state = UringApiCompletion_get_scalar_state(completion);

        assert(scalar_state != NULL);
        io_uring_prep_poll_multishot(sqe, scalar_state->fd, scalar_state->poll_mask);
        break;
    }
    case URING_API_PENDING_CLOSE: {
        UringApiCompletionScalarState *scalar_state = UringApiCompletion_get_scalar_state(completion);

        assert(scalar_state != NULL);
        io_uring_prep_close(sqe, scalar_state->fd);
        break;
    }
    case URING_API_PENDING_SHUTDOWN: {
        UringApiCompletionScalarState *scalar_state = UringApiCompletion_get_scalar_state(completion);

        assert(scalar_state != NULL);
        io_uring_prep_shutdown(sqe, scalar_state->fd, scalar_state->how);
        break;
    }
    case URING_API_PENDING_SOCKET: {
        UringApiCompletionScalarState *scalar_state = UringApiCompletion_get_scalar_state(completion);

        assert(scalar_state != NULL);
        io_uring_prep_socket(sqe, scalar_state->domain, scalar_state->type, scalar_state->protocol,
                             scalar_state->flags);
        break;
    }
    case URING_API_PENDING_CANCEL: {
        UringApiCompletion *cancel_target;

        assert(completion->cancel_target != NULL);
        io_uring_prep_cancel(sqe, completion->cancel_target, 0);
        cancel_target = (UringApiCompletion *)completion->cancel_target;
        if (cancel_target->kind == URING_API_PENDING_SEND_ALL) {
            completion_set_bit(cancel_target, URING_API_C_SEND_ALL_ABANDON);
        }
        break;
    }
    case URING_API_PENDING_POLL_REMOVE:
        assert(completion->cancel_target != NULL);
        io_uring_prep_poll_remove(sqe, (unsigned long long)(uintptr_t)completion->cancel_target);
        break;
    default:
        /* kind already validated */
        break;
    }
    /* timeout SQE before any completion state. a malloc or slot miss rolls
     * the op SQE back and leaves PREPARED, the in-flight ref, and the fd
     * slot untouched. */
    if (completion->has_link_timeout && fill_link_timeout(self, completion, sqe) < 0) {
        if (send_all_slot) {
            fd_table_try_free(self, send_all_slot);
        }
        return -1;
    }
    if (!completion_counts_pending(completion)) {
        if (stamp_nowait_sqe(self, sqe, (unsigned int)completion->kind, nowait_advisory_fd(completion)) < 0) {
            return -1;
        }
        completion_set_bit(completion, URING_API_C_PREPARED);
        /* direct nowait cancel sets this in try_direct_cancel_nowait. a
         * cancel that had to park is filled here, still without a Completion*
         * on the SQE, so sqe_set_completion does not see it.
         * a parked close/shutdown/poll_remove is the tail of something the
         * caller is already waiting on (send_all conflict FIFO, or fill-wait).
         * the wait that copies it onto the SQ must submit it. a direct nowait
         * of those ops does not: nothing is waiting on the ack. */
        if (completion->kind == URING_API_PENDING_CANCEL) {
            if (nowait_cancel_is_sq_waitable((UringApiCompletion *)completion->cancel_target)) {
                self->sq_waitable = true;
            }
        } else if (from_parked) {
            self->sq_waitable = true;
        }
        return 0;
    }
    sqe_set_completion(self, sqe, (PyObject *)completion);
    if (!from_parked) {
        /* drain copying a parked handle into an SQE is not a second prepare(). */
        take_in_flight_ref(self, completion);
    }
    if (send_all_slot) {
        send_all_slot->active = completion;
    }
    /* count only a filled SQE, not construct and not a fill-wait park. */
    if (completion->kind == URING_API_PENDING_RECV_BUF || completion->kind == URING_API_PENDING_RECV_MULTISHOT) {
        UringApiCompletionBufGroupState *buf_group_state = UringApiCompletion_get_buf_group_state(completion);

        assert(buf_group_state != NULL && buf_group_state->buf_group != NULL);
        UringApiBufGroup_note_request((UringApiBufGroup *)buf_group_state->buf_group);
    }
    if (completion->kind == URING_API_PENDING_SEND_ALL) {
        send_all_commit_leg(self, completion, send_all_later_leg);
    }
    return 0;
}

int prepare_one_constructed(UringApiRing *self, UringApiCompletion *completion) {
    return prepare_one_constructed_ex(self, completion, 0, 0, NULL);
}

static int prepare_constructed(UringApiRing *self, PyObject *const *items, Py_ssize_t count) {
    Py_ssize_t i;

    for (i = 0; i < count; i++) {
        PyObject *item = items[i];

        if (!PyObject_TypeCheck(item, &UringApiCompletion_Type)) {
            PyErr_SetString(PyExc_TypeError, "prepare() items must be Completion objects");
            return -1;
        }
        if (prepare_one_constructed(self, (UringApiCompletion *)item) < 0) {
            return -1;
        }
    }
    return 0;
}

int UringApiRing_prepare_impl(UringApiRing *self, PyObject *completions, int *prepared_out) {
    PyObject *seq = NULL;
    PyObject *single[1];
    PyObject *const *items;
    Py_ssize_t count;
    int failed = 0;
    int prepared = 0;

    if (PyObject_TypeCheck(completions, &UringApiCompletion_Type)) {
        single[0] = completions;
        items = single;
        count = 1;
    } else {
        seq = PySequence_Fast(completions, "completions must be a Completion or a sequence of Completions");
        if (!seq) {
            return -1;
        }
        items = PySequence_Fast_ITEMS(seq);
        count = PySequence_Fast_GET_SIZE(seq);
    }

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else if (prepare_constructed(self, items, count) < 0) {
        failed = 1;
        /* count how many in the prefix are now accepted (SQE, conflict FIFO, or fill-wait) */
        {
            Py_ssize_t i;
            for (i = 0; i < count; i++) {
                if (PyObject_TypeCheck(items[i], &UringApiCompletion_Type) &&
                    completion_is_accepted((UringApiCompletion *)items[i])) {
                    prepared++;
                }
            }
        }
    } else {
        prepared = (int)count;
    }
    Py_END_CRITICAL_SECTION();

    Py_XDECREF(seq);
    if (prepared_out) {
        *prepared_out = prepared;
    }
    return failed ? -1 : 0;
}

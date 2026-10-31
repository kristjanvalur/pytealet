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

void drop_parked_ring_ref(UringApiRing *self, UringApiCompletion *completion) {
    if (completion_counts_pending(completion)) {
        ring_pending_dec(self);
    }
    Py_DECREF(completion);
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
        view_state = UringApiCompletion_get_view_state(completion);
        /* fd -1 is the unconstructed sentinel. poison keeps the real fd. */
        return view_state != NULL && view_state->fd >= 0;
    case URING_API_PENDING_RECV:
    case URING_API_PENDING_READ:
    case URING_API_PENDING_WRITE:
        view_state = UringApiCompletion_get_view_state(completion);
        return view_state != NULL && view_state->fd >= 0;
    case URING_API_PENDING_SENDTO:
        view_sockaddr_state = UringApiCompletion_get_view_sockaddr_state(completion);
        return view_sockaddr_state != NULL && view_sockaddr_state->fd >= 0;
    case URING_API_PENDING_RECVMSG:
        msg_state = UringApiCompletion_get_msg_state(completion);
        return msg_state != NULL && msg_state->fd >= 0;
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

/* Nowait SQE identity: tagged token, optional CQE_SKIP_SUCCESS. No Completion*.
 * The caller notes the cared count once: waitable always, a direct nowait
 * only when the ring is SQPOLL. */
static int stamp_nowait_sqe(UringApiRing *self, struct io_uring_sqe *sqe, unsigned int kind, int fd) {
    io_uring_sqe_set_data64(sqe, uring_api_make_nowait_user_data(kind, fd));
    if (self->ring.features & IORING_FEAT_CQE_SKIP) {
        sqe->flags |= IOSQE_CQE_SKIP_SUCCESS;
    }
    return 0;
}

/* Caller holds the ring CS and has checked the ring is open.
 * 1 posted, 0 caller must build a skip_all Completion, -1 error.
 * fill-wait holds Completions. a naked nowait SQE cannot park on it,
 * and copying a head here can take the last slot before this op has
 * one. the caller allocates the Completion and prepare_one orders it
 * with every other prepare. this post runs only when that list is empty.
 * a parked fd-FIFO copy is housekeeping. auto_submit off publishes
 * that tail after the new SQE is queued, so the SQE can ride along. */
static int nowait_post_or_defer(UringApiRing *self, int must_park, unsigned int kind, int advisory_fd,
                                void (*prep)(struct io_uring_sqe *sqe, void *arg), void *arg) {
    struct io_uring_sqe *sqe;
    uint64_t internal_at;
    int got;

    if (must_park || self->fill_wait.count > 0) {
        return 0;
    }
    internal_at = ring_thread_housekeeping_sqes();
    /* fill-wait is empty. a ready conflict-FIFO head still copies first,
     * and may enter under make_room. */
    if (drain_parked(self, 0, NULL) < 0) {
        (void)ring_publish_housekeeping_since(self, internal_at);
        return -1;
    }
    got = get_sqe_try(self, 0, NULL, &sqe);
    if (got == 1) {
        prep(sqe, arg);
        stamp_nowait_sqe(self, sqe, kind, advisory_fd);
    }
    if (ring_publish_housekeeping_since(self, internal_at) < 0) {
        return -1;
    }
    return got;
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

/* best-effort. a non-socket rejects SO_LINGER. the close still runs.
 * call only after the close SQE will stay queued. the caller already
 * holds the ring critical section and that slot. do not drop the GIL
 * here: on a free-threaded build that suspends the critical section,
 * and another thread can take the same slot. */
static void close_apply_abortive(int fd) {
    struct linger linger;

    linger.l_onoff = 1;
    linger.l_linger = 0;
    (void)setsockopt(fd, SOL_SOCKET, SO_LINGER, &linger, sizeof(linger));
}

struct nowait_close_arg {
    int fd;
    int abortive;
};

static void prep_close_sqe(struct io_uring_sqe *sqe, void *arg) {
    struct nowait_close_arg *prep = arg;

    /* got == 1: this slot is taken. must_park and a missed slot never
     * call prep, and a drain error returns before it. publish runs
     * after prep, so the option is set before the kernel can see the
     * close. a publish failure still has this SQE filled. */
    if (prep->abortive) {
        close_apply_abortive(prep->fd);
    }
    io_uring_prep_close(sqe, prep->fd);
}

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
        /* abandon before this call returns the ring lock. a parked
         * continuation stays a send. if fill-wait is non-empty the
         * shared post defers, and prepare_one orders the skip_all
         * cancel with that leg. */
        if (target->kind == URING_API_PENDING_SEND_ALL) {
            completion_set_bit(target, URING_API_C_SEND_ALL_ABANDON);
        }
        result = nowait_post_or_defer(self, nowait_cancel_must_park(self, target),
                                      (unsigned int)URING_API_PENDING_CANCEL, -1, prep_cancel_sqe, target);
        /* a silenced cancel is not waitable. SQPOLL still counts it. */
        if (result > 0) {
            int waitable = nowait_cancel_is_sq_waitable(target);

            if (waitable) {
                self->sq_waitable = true;
            }
            ring_note_publish_sqe(self, waitable);
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
        if (result > 0) {
            ring_note_publish_sqe(self, 0);
        }
    }
    Py_END_CRITICAL_SECTION();
    return result;
}

int try_direct_close_nowait(UringApiRing *self, int fd, int abortive) {
    struct nowait_close_arg prep;
    int result;
    int discharge = 0;

    prep.fd = fd;
    prep.abortive = abortive;
    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        result = -1;
    } else {
        UringApiFdSlot *slot = fd_table_lookup(self, fd);

        /* nothing in progress: this close leaves the slot. a posted SQE
         * applies SO_LINGER and drops the mark below. a defer builds a
         * completion, and that prepare drops the mark as it leaves. a
         * busy fd still parks on the FIFO and does not drop it yet. */
        if (slot != NULL && slot->abortive && slot->active == NULL && slot->fifo.count == 0) {
            prep.abortive = 1;
            discharge = 1;
        }
        result = nowait_post_or_defer(self, nowait_fd_op_must_park(self, fd), (unsigned int)URING_API_PENDING_CLOSE, fd,
                                      prep_close_sqe, &prep);
        if (result > 0 && discharge) {
            slot = fd_table_lookup(self, fd);
            if (slot != NULL && slot->abortive) {
                slot->abortive = 0;
                fd_table_try_free(self, slot);
            }
        }
        if (result > 0) {
            ring_note_publish_sqe(self, 0);
        }
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
        UringApiFdSlot *slot = fd_table_lookup(self, fd);

        /* same as a waitable shutdown: an idle abortive slot submits -1.
         * the advisory fd on the SQE stays the caller's descriptor. */
        if (slot != NULL && slot->abortive && slot->active == NULL && slot->fifo.count == 0) {
            prep.fd = -1;
        }
        result = nowait_post_or_defer(self, nowait_fd_op_must_park(self, fd), (unsigned int)URING_API_PENDING_SHUTDOWN,
                                      fd, prep_shutdown_sqe, &prep);
        if (result > 0) {
            ring_note_publish_sqe(self, 0);
        }
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

/* 1: two slots are free. 0: they are not, and this call did not enter.
 * -1: error. a quiet full is 0 for a parked copy and for a new prepare.
 * prepare_sq_short is what parks or stops the drain. does not take a slot. */
int reserve_link_timeout_sqes(UringApiRing *self, int flush_if_full, int *submitted_out) {
    int enter = flush_if_full ? URING_API_SQ_ENTER_ALWAYS : URING_API_SQ_ENTER_POLICY;

    return sq_prepare_room(self, 2, enter, submitted_out);
}

/* A close is passing through an idle slot. Copy abortive onto it and
 * clear the slot. Called once, before fill-wait or the SQ. A close
 * still on the FIFO is drain_fd_slot. Caller holds the ring CS. */
static void close_inherit_idle_abortive(UringApiRing *self, UringApiCompletion *completion) {
    UringApiCompletionScalarState *scalar;
    UringApiFdSlot *slot;

    if (completion->kind != URING_API_PENDING_CLOSE) {
        return;
    }
    scalar = UringApiCompletion_get_scalar_state(completion);
    assert(scalar != NULL);
    if (scalar->fd < 0) {
        return;
    }
    slot = fd_table_lookup(self, scalar->fd);
    if (slot == NULL || !slot->abortive || slot->active != NULL || slot->fifo.count > 0) {
        return;
    }
    scalar->flags |= URING_API_CLOSE_ABORTIVE;
    slot->abortive = 0;
    fd_table_try_free(self, slot);
}

/* fill-wait still holds a completion this drain could not copy. park
 * behind it. a free slot is not a reason to pass that head: a link
 * pair needs two, and the next submit copies the head first. */
static int park_behind_fill_wait(UringApiRing *self, UringApiCompletion *completion) {
    UringApiFdSlot *send_all_slot = NULL;

    if (completion->kind == URING_API_PENDING_SEND_ALL) {
        UringApiCompletionViewState *view_state;

        view_state = UringApiCompletion_get_view_state(completion);
        assert(view_state != NULL);
        send_all_slot = fd_table_get(self, view_state->fd);
        if (!send_all_slot) {
            return -1;
        }
    }
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

/* the cancel's send_all next leg is on fill-wait, or this prepare's drain
 * just copied that leg into the last slot. park the cancel behind it.
 * the next drain publishes the send, then the cancel. issuer
 * make_room=False still raises for every other full-SQ prepare. */
static int park_cancel_behind_send_all(UringApiRing *self, UringApiCompletion *completion) {
    if (enqueue_fill_wait(self, completion, 0) < 0) {
        return -1;
    }
    return 0;
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
    /* not a Completion. stays raised until consume_cqe frees ts, so a drain
     * that stops at pending_count 0 cannot drop the copy. */
    ring_pending_inc(self);
    /* the op notes the thread count when it is waitable. the timer is not. */
    return 0;
}

/* Caller holds the ring critical section. On success the completion is in the
 * kernel SQ, on fill-wait, or on that fd's conflict FIFO. The ring holds one
 * reference: taken at SQ fill, or adopted by the list at park. Moving it
 * between those places does not take another. skip_all (except send_all)
 * stamps a tagged SQE and drops that reference. Kind is checked before
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
    int behind_parked_cont = 0;

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

    /* abandon before drain so this leg's CQE does not arm another send.
     * sample the parked continuation first: drain may copy it into the
     * last free slot, and the cancel then has to park behind it. */
    if (!from_parked && completion->kind == URING_API_PENDING_CANCEL && completion->cancel_target != NULL &&
        ((UringApiCompletion *)completion->cancel_target)->kind == URING_API_PENDING_SEND_ALL) {
        UringApiCompletion *target = (UringApiCompletion *)completion->cancel_target;

        behind_parked_cont =
            completion_has_bit(target, URING_API_C_SEND_ALL_CONT) && completion_has_bit(target, URING_API_C_FILL_WAIT);
        completion_set_bit(target, URING_API_C_SEND_ALL_ABANDON);
    }

    /* conflicting ops park even if leftover drain would hit a full SQ.
     * the slot stage ends here. a pass-through copies abortive onto the
     * op, and a close clears the slot. fill-wait and the SQ do not
     * read it again. */
    if (!from_parked && should_enqueue_conflict(self, completion, NULL)) {
        return enqueue_conflict(self, completion);
    }
    if (!from_parked) {
        poison_direct_on_idle_abortive(self, completion);
        close_inherit_idle_abortive(self, completion);
    }
    /* a new op must not pass a fill-wait head, and must not enter to force
     * that head in. make_room still enters for the op once the list is
     * empty. from_parked is the head's own copy and must not recurse.
     * conflict FIFOs drain only after the list is empty: a head that does
     * not fit stays ahead of them too. the caller's prepare() publishes
     * afterwards when auto_submit is off, so this SQE can ride along. */
    if (!from_parked) {
        int made_way = fill_wait_make_way(self);

        if (made_way < 0) {
            return -1;
        }
        if (made_way == 0) {
            return park_behind_fill_wait(self, completion);
        }
        if (drain_parked(self, 0, NULL) < 0) {
            return -1;
        }
    }

    if (completion->kind == URING_API_PENDING_SEND_ALL) {
        view_state = UringApiCompletion_get_view_state(completion);
        assert(view_state != NULL && view_state->fd >= 0);
        send_all_slot = fd_table_get(self, view_state->fd);
        if (!send_all_slot) {
            return -1;
        }
    }

    /* two slots before either is taken, for every kind. a shortfall parks
     * or fails with the queue unchanged. */
    if (completion->has_link_timeout) {
        int reserved = reserve_link_timeout_sqes(self, flush_if_full, submitted_out);

        if (reserved < 0) {
            if (send_all_slot) {
                fd_table_try_free(self, send_all_slot);
            }
            return -1;
        }
        if (reserved != 1) {
            if (behind_parked_cont) {
                return park_cancel_behind_send_all(self, completion);
            }
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
            if (behind_parked_cont) {
                return park_cancel_behind_send_all(self, completion);
            }
            return prepare_sq_short(self, completion, from_parked, send_all_slot);
        }
    }
    switch (completion->kind) {
    case URING_API_PENDING_SEND:
        view_state = UringApiCompletion_get_view_state(completion);
        assert(view_state != NULL && view_state->has_view);
        io_uring_prep_send(sqe, completion_sqe_fd(completion, view_state->fd), view_state->view.buf,
                           (size_t)view_state->view.len, (int)recvsend_msg_flags(view_state->flags));
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
        /* fd -1 may produce one -EBADF and no NOTIF. a copying send is
         * one CQE, so the in-flight ref can drop on it. */
        if (completion_has_bit(completion, URING_API_C_FD_POISON)) {
            io_uring_prep_send(sqe, completion_sqe_fd(completion, view_state->fd), view_state->view.buf,
                               (size_t)view_state->view.len, (int)recvsend_msg_flags(view_state->flags));
        } else {
            io_uring_prep_send_zc(sqe, view_state->fd, view_state->view.buf, (size_t)view_state->view.len,
                                  (int)recvsend_msg_flags(view_state->flags), view_state->zc_flags);
        }
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
        io_uring_prep_sendmsg(sqe, completion_sqe_fd(completion, msg_state->fd), &msg_state->msg,
                              recvsend_msg_flags(msg_state->flags));
        recvsend_apply_ioprio(sqe, msg_state->flags);
        break;
    case URING_API_PENDING_SENDMSG_ZC:
        msg_state = UringApiCompletion_get_msg_state(completion);
        assert(msg_state != NULL && msg_state->has_view);
        if (completion_has_bit(completion, URING_API_C_FD_POISON)) {
            io_uring_prep_sendmsg(sqe, completion_sqe_fd(completion, msg_state->fd), &msg_state->msg,
                                  recvsend_msg_flags(msg_state->flags));
        } else {
            io_uring_prep_sendmsg_zc(sqe, msg_state->fd, &msg_state->msg, recvsend_msg_flags(msg_state->flags));
        }
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
        unsigned int accept_flags;

        assert(scalar_state != NULL);
        /* ACCEPT_TCP_NODELAY is not an accept4 flag. The kernel rejects it. */
        accept_flags = scalar_state->flags & ~(unsigned int)URING_API_ACCEPT_TCP_NODELAY;
        if (completion_has_bit(completion, URING_API_C_MULTISHOT)) {
            io_uring_prep_multishot_accept(sqe, scalar_state->fd, NULL, NULL, accept_flags);
        } else {
            io_uring_prep_accept(sqe, scalar_state->fd, NULL, NULL, accept_flags);
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
        io_uring_prep_shutdown(sqe, completion_sqe_fd(completion, scalar_state->fd), scalar_state->how);
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
     * slot untouched. a resume that is already queued stays queued on
     * malloc: failing wait() or submit() would be a false failure. */
    if (completion->has_link_timeout && fill_link_timeout(self, completion, sqe) < 0) {
        if (from_parked && PyErr_ExceptionMatches(PyExc_MemoryError)) {
            PyErr_Clear();
            if (send_all_slot) {
                fd_table_try_free(self, send_all_slot);
            }
            return 2;
        }
        if (send_all_slot) {
            fd_table_try_free(self, send_all_slot);
        }
        return -1;
    }
    /* the close SQE stays queued: no timer, or the timeout SQE was filled.
     * a rolled-back timeout drops the op above and leaves the option
     * unset. the abortive bit remains, so a later fill still applies it.
     * do not apply inside the switch: fill_link_timeout can still unwind
     * this slot. */
    if (completion->kind == URING_API_PENDING_CLOSE) {
        UringApiCompletionScalarState *close_state = UringApiCompletion_get_scalar_state(completion);

        assert(close_state != NULL);
        /* the close took the bit when it left the slot. this only
         * applies the option. a rolled-back timeout never reaches here,
         * so the socket stays unchanged until a fill that sticks. */
        if (close_state->flags & URING_API_CLOSE_ABORTIVE) {
            close_apply_abortive(close_state->fd);
        }
    }
    if (!completion_counts_pending(completion)) {
        if (stamp_nowait_sqe(self, sqe, (unsigned int)completion->kind, nowait_advisory_fd(completion)) < 0) {
            return -1;
        }
        completion_set_bit(completion, URING_API_C_PREPARED);
        /* direct nowait cancel sets this in try_direct_cancel_nowait. a
         * cancel that had to park is filled here, still without a Completion*
         * on the SQE, so sqe_set_completion does not see it.
         * a parked close, shutdown, or poll_remove is housekeeping. the
         * copy notes the internal count, and auto_submit off publishes
         * that tail. sq_waitable is what makes auto_submit on publish it
         * from wait(). a direct nowait of those ops does not set
         * sq_waitable: nothing is waiting on the ack. the cared count
         * still moves for that direct nowait when the ring is SQPOLL. */
        {
            int waitable = 0;

            if (completion->kind == URING_API_PENDING_CANCEL) {
                if (nowait_cancel_is_sq_waitable((UringApiCompletion *)completion->cancel_target)) {
                    self->sq_waitable = true;
                    waitable = 1;
                }
            } else if (from_parked) {
                self->sq_waitable = true;
                waitable = 1;
            }
            ring_note_publish_sqe(self, waitable);
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
    uint64_t internal_at = 0;

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
    } else {
        /* after the new SQEs are queued, so a parked copy drained at the
         * start of this prepare can take them with it. */
        internal_at = ring_thread_housekeeping_sqes();
        if (prepare_constructed(self, items, count) < 0) {
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
        if (ring_publish_housekeeping_since(self, internal_at) < 0) {
            failed = 1;
        }
    }
    Py_END_CRITICAL_SECTION();

    Py_XDECREF(seq);
    if (prepared_out) {
        *prepared_out = prepared;
    }
    return failed ? -1 : 0;
}

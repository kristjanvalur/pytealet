/*
 * Fill-wait and per-fd conflict parks for the _uring_api Ring type.
 */

#include "uring_api_park.h"
#include "uring_api_completion.h"
#include "uring_api_core.h"
#include "uring_api_fd_table.h"
#include "uring_api_prepare.h"

#include <assert.h>

static int drain_fill_wait(UringApiRing *self, int flush_if_full, int *submitted_out);

static int completion_kind_conflicts(UringApiPendingKind kind) {
    return kind == URING_API_PENDING_SEND || kind == URING_API_PENDING_SEND_ALL || kind == URING_API_PENDING_SEND_ZC ||
           kind == URING_API_PENDING_SENDMSG || kind == URING_API_PENDING_SENDMSG_ZC ||
           kind == URING_API_PENDING_CLOSE || kind == URING_API_PENDING_SHUTDOWN;
}

static int completion_conflict_fd(UringApiCompletion *completion, int *fd_out) {
    UringApiCompletionViewState *view_state;
    UringApiCompletionViewSockaddrState *view_sockaddr_state;
    UringApiCompletionMsgState *msg_state;
    UringApiCompletionScalarState *scalar_state;

    switch (completion->kind) {
    case URING_API_PENDING_SEND:
    case URING_API_PENDING_SEND_ALL:
    case URING_API_PENDING_SEND_ZC:
    case URING_API_PENDING_RECV:
        view_state = UringApiCompletion_get_view_state(completion);
        assert(view_state != NULL);
        *fd_out = view_state->fd;
        return 0;
    case URING_API_PENDING_SENDTO:
        view_sockaddr_state = UringApiCompletion_get_view_sockaddr_state(completion);
        assert(view_sockaddr_state != NULL);
        *fd_out = view_sockaddr_state->fd;
        return 0;
    case URING_API_PENDING_SENDMSG:
    case URING_API_PENDING_SENDMSG_ZC:
    case URING_API_PENDING_RECVMSG:
        msg_state = UringApiCompletion_get_msg_state(completion);
        assert(msg_state != NULL);
        *fd_out = msg_state->fd;
        return 0;
    case URING_API_PENDING_CLOSE:
    case URING_API_PENDING_SHUTDOWN:
        scalar_state = UringApiCompletion_get_scalar_state(completion);
        assert(scalar_state != NULL);
        *fd_out = scalar_state->fd;
        return 0;
    case URING_API_PENDING_CANCEL:
    case URING_API_PENDING_POLL_REMOVE:
        if (completion->cancel_target == NULL ||
            !PyObject_TypeCheck(completion->cancel_target, &UringApiCompletion_Type)) {
            return -1;
        }
        return completion_conflict_fd((UringApiCompletion *)completion->cancel_target, fd_out);
    default:
        return -1;
    }
}

static int is_cancel_of_active(UringApiCompletion *completion, UringApiFdSlot *slot) {
    return completion->kind == URING_API_PENDING_CANCEL && slot->active != NULL &&
           (UringApiCompletion *)completion->cancel_target == slot->active;
}

int should_enqueue_conflict(UringApiRing *self, UringApiCompletion *completion, UringApiFdSlot **slot_out) {
    int fd;
    UringApiFdSlot *slot;
    UringApiCompletion *target;

    if (completion_conflict_fd(completion, &fd) < 0) {
        return 0;
    }
    slot = fd_table_lookup(self, fd);
    if (slot_out) {
        *slot_out = slot;
    }
    if (slot == NULL) {
        return 0;
    }
    if (slot->active == NULL && slot->fifo.count == 0) {
        return 0;
    }
    if (is_cancel_of_active(completion, slot)) {
        return 0;
    }
    if (completion_kind_conflicts(completion->kind)) {
        return 1;
    }
    if (completion->kind != URING_API_PENDING_CANCEL || completion->cancel_target == NULL) {
        return 0;
    }
    target = (UringApiCompletion *)completion->cancel_target;
    return completion_has_bit(target, URING_API_C_CONFLICT_QUEUED);
}

int nowait_cancel_must_park(UringApiRing *self, UringApiCompletion *target) {
    int fd;
    UringApiFdSlot *slot;

    if (completion_conflict_fd(target, &fd) < 0) {
        return 0;
    }
    slot = fd_table_lookup(self, fd);
    if (slot == NULL || (slot->active == NULL && slot->fifo.count == 0)) {
        return 0;
    }
    /* cancel of the in-kernel op posts now; a queued target must wait behind it */
    if (slot->active == target) {
        return 0;
    }
    return completion_has_bit(target, URING_API_C_CONFLICT_QUEUED);
}

int nowait_fd_op_must_park(UringApiRing *self, int fd) {
    UringApiFdSlot *slot = fd_table_lookup(self, fd);

    if (slot == NULL) {
        return 0;
    }
    return slot->active != NULL || slot->fifo.count != 0;
}

/* The ring holds one reference. A new park creates it and the list adopts
 * it. A send-all continuation already has it, so the list takes that one
 * instead of a second. Push failure did not adopt it: undo only a ref this
 * call created. */
static int take_park_ref(UringApiRing *self, UringApiCompletion *completion, int already_in_flight) {
    if (already_in_flight) {
        assert(completion_counts_pending(completion));
        return 0;
    }
    if (completion_counts_pending(completion)) {
        take_in_flight_ref(self, completion);
    } else {
        Py_INCREF(completion);
    }
    return 1;
}

int enqueue_conflict(UringApiRing *self, UringApiCompletion *completion) {
    int fd;
    UringApiFdSlot *slot;
    int took_ref;

    if (completion_conflict_fd(completion, &fd) < 0) {
        assert(0 && "conflict enqueue is missing an fd");
        return -1;
    }
    slot = fd_table_get(self, fd);
    if (!slot) {
        return -1;
    }
    took_ref = take_park_ref(self, completion, 0);
    completion_set_bit(completion, URING_API_C_CONFLICT_QUEUED);
    if (fd_table_fifo_push(slot, completion) < 0) {
        completion_clear_bit(completion, URING_API_C_CONFLICT_QUEUED);
        if (took_ref) {
            drop_parked_ring_ref(self, completion);
        }
        return -1;
    }
    fd_table_mark_drain(self, slot);
    return 0;
}

int enqueue_fill_wait(UringApiRing *self, UringApiCompletion *completion, int already_in_flight) {
    int took_ref;

    if (completion_has_bit(completion, URING_API_C_FILL_WAIT)) {
        return 0;
    }
    took_ref = take_park_ref(self, completion, already_in_flight);
    completion_set_bit(completion, URING_API_C_FILL_WAIT);
    if (completion_fifo_push(&self->fill_wait, completion) < 0) {
        completion_clear_bit(completion, URING_API_C_FILL_WAIT);
        if (took_ref) {
            drop_parked_ring_ref(self, completion);
        }
        return -1;
    }
    return 0;
}

/* a copy off fill-wait or a per-fd FIFO is housekeeping. a continuation
 * notes itself in send_all_commit_leg, so this does not count it twice. */
static void note_parked_copy(int continuation) {
    if (!continuation) {
        ring_note_housekeeping_sqe();
    }
}

int drain_parked(UringApiRing *self, int flush_if_full, int *submitted_out) {
    int ret;

    /* fill-wait first so a send-all next-leg precedes that fd's conflict FIFO. */
    ret = drain_fill_wait(self, flush_if_full, submitted_out);
    if (ret != 0) {
        return ret < 0 ? -1 : 0;
    }
    while (self->fd_drain_head) {
        UringApiFdSlot *slot = self->fd_drain_head;

        ret = drain_fd_slot(self, slot, flush_if_full, submitted_out);
        if (ret != 0) {
            /* leftover SQ-full re-queued the slot; do not resume the loop. */
            return ret < 0 ? -1 : 0;
        }
    }
    return 0;
}

void clear_parked(UringApiRing *self) {
    completion_fifo_clear(self, &self->fill_wait);
    fd_table_clear(self);
}

static int fill_queued_completion(UringApiRing *self, UringApiCompletion *completion, int flush_if_full,
                                  int *submitted_out) {
    return prepare_one_constructed_ex(self, completion, 1, flush_if_full, submitted_out);
}

/* leftover drain (flush_if_full==0) is best-effort: quiet SQ-full (prepare
 * return 1) means stop, leave the head queued. flush_if_full must have entered,
 * so a quiet full becomes SubmissionQueueFull. return 2 is not that: the
 * parked op could not allocate its link timeout, so stop without an error
 * even when this drain was allowed to enter. */
static int leftover_fill_result(int fill_ret, int flush_if_full) {
    if (fill_ret == 0) {
        return 0;
    }
    if (fill_ret == 2) {
        return 1;
    }
    if (fill_ret < 0) {
        return -1;
    }
    if (flush_if_full) {
        PyErr_SetString(UringApiSubmissionQueueFullError, "no submission queue entries available");
        return -1;
    }
    return 1;
}

static int drain_fill_wait(UringApiRing *self, int flush_if_full, int *submitted_out) {
    while (self->fill_wait.count) {
        UringApiCompletion *completion = completion_fifo_peek(&self->fill_wait);

        /* terminal CQE already stored. the list still holds the only ring
         * ref: this leg was never copied onto an SQE. */
        if (completion->result != NULL) {
            completion_fifo_pop(&self->fill_wait);
            completion_clear_bit(completion, URING_API_C_FILL_WAIT);
            drop_parked_ring_ref(self, completion);
            continue;
        }
        {
            /* commit_leg clears SEND_ALL_CONT, so sample it before the fill.
             * a continuation notes itself there. any other copy is
             * housekeeping too. */
            int continuation = completion->kind == URING_API_PENDING_SEND_ALL &&
                               completion_has_bit(completion, URING_API_C_SEND_ALL_CONT);
            int fill_ret = leftover_fill_result(fill_queued_completion(self, completion, flush_if_full, submitted_out),
                                                flush_if_full);

            if (fill_ret != 0) {
                return fill_ret;
            }
            note_parked_copy(continuation);
        }
        completion_fifo_pop(&self->fill_wait);
        completion_clear_bit(completion, URING_API_C_FILL_WAIT);
        /* flush_if_full==0 does not enter. a copied SQE bumps this
         * thread's count, and the publish after the callback is the one
         * decision. a caller that passed flush_if_full flushes itself.
         * pop moved the list's one ref. a waitable SQE keeps it
         * (from_parked does not take another). a tagged nowait does not
         * store the completion. */
        if (!completion_counts_pending(completion)) {
            Py_DECREF(completion);
        }
    }
    return 0;
}

int drain_fd_slot(UringApiRing *self, UringApiFdSlot *slot, int flush_if_full, int *submitted_out) {
    fd_table_unlink_drain(self, slot);
    while (slot->fifo.count) {
        UringApiCompletion *completion = fd_table_fifo_peek(slot);

        if (slot->active != NULL && !is_cancel_of_active(completion, slot)) {
            break;
        }
        {
            int continuation = completion->kind == URING_API_PENDING_SEND_ALL &&
                               completion_has_bit(completion, URING_API_C_SEND_ALL_CONT);
            int fill_ret = leftover_fill_result(fill_queued_completion(self, completion, flush_if_full, submitted_out),
                                                flush_if_full);

            if (fill_ret != 0) {
                fd_table_mark_drain(self, slot);
                return fill_ret;
            }
            note_parked_copy(continuation);
        }
        completion = fd_table_fifo_pop(slot);
        completion_clear_bit(completion, URING_API_C_CONFLICT_QUEUED);
        /* same move as fill-wait: the popped ref is the in-flight ref
         * unless this was a tagged nowait. */
        if (!completion_counts_pending(completion)) {
            Py_DECREF(completion);
        }
    }
    if (slot->fifo.count > 0 && slot->active == NULL) {
        fd_table_mark_drain(self, slot);
    } else if (slot->fifo.count > 0 && is_cancel_of_active(fd_table_fifo_peek(slot), slot)) {
        fd_table_mark_drain(self, slot);
    } else {
        fd_table_try_free(self, slot);
    }
    return 0;
}

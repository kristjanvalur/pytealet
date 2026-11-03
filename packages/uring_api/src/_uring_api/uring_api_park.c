/*
 * Per-fd conflict parks for the _uring_api Ring type.
 */

#include "uring_api_park.h"
#include "uring_api_completion.h"
#include "uring_api_core.h"
#include "uring_api_fd_table.h"
#include "uring_api_prepare.h"

#include <assert.h>

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
    /* the push is the commit: a failure above did not adopt the completion.
     * the send_all completion copies this FIFO. nothing else walks it. */
    ring_note_parked_op();
    return 0;
}

void clear_parked(UringApiRing *self) { fd_table_clear(self); }

static int fill_queued_completion(UringApiRing *self, UringApiCompletion *completion) {
    /* always enter on a full SQ. the completion that freed the fd is the
     * only caller, and it cannot leave the head for a later retry. */
    return prepare_one_constructed_ex(self, completion, 1, 1, NULL);
}

int drain_fd_slot(UringApiRing *self, UringApiFdSlot *slot) {
    while (slot->fifo.count) {
        UringApiCompletion *completion = fd_table_fifo_peek(slot);

        if (slot->active != NULL && !is_cancel_of_active(completion, slot)) {
            break;
        }
        {
            int fill_ret = fill_queued_completion(self, completion);

            if (fill_ret != 0) {
                if (fill_ret > 0 && !PyErr_Occurred()) {
                    PyErr_SetString(UringApiSubmissionQueueFullError, "no submission queue entries available");
                }
                return -1;
            }
            /* a copy off this FIFO is housekeeping. a next leg notes
             * itself in send_all_commit_leg and is not queued here. */
            ring_note_housekeeping_sqe();
        }
        completion = fd_table_fifo_pop(slot);
        completion_clear_bit(completion, URING_API_C_CONFLICT_QUEUED);
        /* the popped ref is the in-flight ref unless this was a tagged nowait. */
        if (!completion_counts_pending(completion)) {
            Py_DECREF(completion);
        }
    }
    fd_table_try_free(self, slot);
    return 0;
}

#ifndef URING_API_PREPARE_H
#define URING_API_PREPARE_H

/* private: SQE fill and send-all re-arm. Parks are uring_api_park.h; construct is uring_api_construct.h. */

#include "uring_api_common.h"

int nowait_advisory_fd(UringApiCompletion *completion);
/* Prepare constructed completions (get_sqe + fill). On error the prefix
 * of *completions* is already prepared (and may have been flushed). */
int UringApiRing_prepare_impl(UringApiRing *self, PyObject *completions, int *prepared_out);
/* Fill one constructed handle (caller holds the ring CS). */
int prepare_one_constructed(UringApiRing *self, UringApiCompletion *completion);
/* 0 filled or parked. 1 leftover SQ-full (from_parked, no exception). -1 error. */
int prepare_one_constructed_ex(UringApiRing *self, UringApiCompletion *completion, int from_parked, int flush_if_full,
                               int *submitted_out);
void take_in_flight_ref(UringApiRing *self, UringApiCompletion *completion);
/* op SQE is already filled and the timeout slot was reserved. ORs
 * IOSQE_IO_LINK and fills the timeout SQE. That SQE owns a timespec copy
 * freed when its CQE is consumed. 0, or -1 after rolling the op slot back.
 * Caller holds the ring CS. */
int fill_link_timeout(UringApiRing *self, UringApiCompletion *completion, struct io_uring_sqe *op_sqe);
/* 1: two SQ slots are free. 0: caller parks (not the submit thread, or
 * auto_submit is off and this call must not enter). -1: error. A ring with
 * fewer than two entries raises RuntimeError before any park or enter.
 * Once this call may enter, a stuck queue is the same RuntimeError as one
 * slot. 2: from_parked and this call must not enter, quiet full.
 * Does not take a slot. Room is sq_ensure_space(need=2). sqring_wait runs
 * only when the SQ is completely full. */
int reserve_link_timeout_sqes(UringApiRing *self, int from_parked, int flush_if_full, int *submitted_out);
int completion_counts_pending(const UringApiCompletion *completion);
/* 1 = nowait SQE posted, no Completion. 0 = caller allocates one and prepares
 * it (conflict FIFO or fill-wait). -1 = error. */
int try_direct_cancel_nowait(UringApiRing *self, UringApiCompletion *target);
int try_direct_poll_remove_nowait(UringApiRing *self, UringApiCompletion *target);
int try_direct_close_nowait(UringApiRing *self, int fd);
int try_direct_shutdown_nowait(UringApiRing *self, int fd, int how);

#endif

#ifndef URING_API_SEND_ALL_H
#define URING_API_SEND_ALL_H

/* private: synthetic send-all next-leg fill, CQE, and fd occupancy. */

#include "uring_api_common.h"

#ifndef IORING_RECVSEND_POLL_FIRST
#define IORING_RECVSEND_POLL_FIRST (1U << 0)
#endif

/* POLL_FIRST is sqe->ioprio, not MSG_* msg_flags.
 * Bit 0 is also MSG_OOB: that value is poll-first, not OOB. */
static inline unsigned int recvsend_msg_flags(unsigned int flags) {
    return flags & ~(unsigned int)IORING_RECVSEND_POLL_FIRST;
}

static inline void recvsend_apply_ioprio(struct io_uring_sqe *sqe, unsigned int flags) {
    if (flags & IORING_RECVSEND_POLL_FIRST) {
        sqe->ioprio |= IORING_RECVSEND_POLL_FIRST;
    }
}

int send_all_fill_sqe(UringApiRing *self, UringApiCompletion *completion, struct io_uring_sqe *sqe, int later_leg);
int send_all_on_cqe(UringApiRing *self, UringApiCompletion *completion, int res, unsigned int flags);

#endif

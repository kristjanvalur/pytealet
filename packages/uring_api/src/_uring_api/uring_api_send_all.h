#ifndef URING_API_SEND_ALL_H
#define URING_API_SEND_ALL_H

/* private: send_all drain fill, next-leg re-arm, and CQE handling. */

#include "uring_api_common.h"

struct io_uring_sqe;

int send_all_on_cqe(UringApiRing *self, UringApiCompletion *completion, int res, unsigned int flags);
/* Fill the send or abandon NOP only. Does not attach the completion.
 * later_leg selects POLL_FIRST; it does not count the leg. */
int send_all_fill_sqe(UringApiRing *self, UringApiCompletion *completion, struct io_uring_sqe *sqe, int later_leg);
/* After the SQE pair is filled: count a later leg and leave the park. */
void send_all_commit_leg(UringApiRing *self, UringApiCompletion *completion, int later_leg);

#endif

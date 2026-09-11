#ifndef URING_API_SEND_ALL_H
#define URING_API_SEND_ALL_H

/* private: send_all drain fill, next-leg re-arm, and CQE handling. */

#include "uring_api_common.h"

struct io_uring_sqe;

int send_all_on_cqe(UringApiRing *self, UringApiCompletion *completion, int res, unsigned int flags);
int send_all_fill_sqe(UringApiRing *self, UringApiCompletion *completion, struct io_uring_sqe *sqe, int later_leg);

#endif

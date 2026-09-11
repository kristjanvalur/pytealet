#ifndef URING_API_PREPARE_H
#define URING_API_PREPARE_H

/* private: SQE fill and send-all re-arm. Parks are uring_api_park.h; construct is uring_api_construct.h. */

#include "uring_api_common.h"

int send_all_on_cqe(UringApiRing *self, UringApiCompletion *completion, int res, unsigned int flags);
/* 1 skip wait()/callback (skip_all: report nowait_error_handler when res < 0;
 * skip_success: skip only when res >= 0), 0 deliver the handle. */
int skip_success_omit_delivery(UringApiRing *self, UringApiCompletion *completion, int res, unsigned int flags);
/* Prepare constructed completions (get_sqe + fill). On error the prefix
 * of *completions* is already prepared (and may have been flushed). */
int UringApiRing_prepare_impl(UringApiRing *self, PyObject *completions, int *prepared_out);
/* Fill one constructed handle (caller holds the ring CS). */
int prepare_one_constructed(UringApiRing *self, UringApiCompletion *completion);
int prepare_one_constructed_ex(UringApiRing *self, UringApiCompletion *completion, int from_parked, int flush_if_full,
                               int *submitted_out);
void take_in_flight_ref(UringApiRing *self, UringApiCompletion *completion);
int completion_counts_pending(const UringApiCompletion *completion);

#endif

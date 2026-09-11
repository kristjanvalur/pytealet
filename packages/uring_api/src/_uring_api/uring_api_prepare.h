#ifndef URING_API_PREPARE_H
#define URING_API_PREPARE_H

/* private: SQE fill, park drain, send-all re-arm. construct factories live in uring_api_construct.h. */

#include "uring_api_common.h"

int send_all_on_cqe(UringApiRing *self, UringApiCompletion *completion, int res, unsigned int flags);
/* 1 skip wait()/callback (skip_all: report nowait_error_handler when res < 0;
 * skip_success: skip only when res >= 0), 0 deliver the handle. */
int skip_success_omit_delivery(UringApiRing *self, UringApiCompletion *completion, int res, unsigned int flags);
/* Drain fill-wait then conflict FIFOs into the SQ. flush_if_full: submit a
 * full SQ (submit() path). prepare passes 0 so auto_submit still gates room.
 * submitted_out, if non-NULL, accumulates SQEs flushed to make room. */
int drain_parked(UringApiRing *self, int flush_if_full, int *submitted_out);
void clear_parked(UringApiRing *self);
/* Prepare constructed completions (get_sqe + fill). On error the prefix
 * of *completions* is already prepared (and may have been flushed). */
int UringApiRing_prepare_impl(UringApiRing *self, PyObject *completions, int *prepared_out);
/* Fill one constructed handle (caller holds the ring CS). */
int prepare_one_constructed(UringApiRing *self, UringApiCompletion *completion);

#endif

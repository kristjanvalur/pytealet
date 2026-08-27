#ifndef URING_API_PARK_H
#define URING_API_PARK_H

/* private: fill-wait and per-fd conflict FIFO. callers hold the ring CS. */

#include "uring_api_common.h"

void take_in_flight_ref(UringApiRing *self, UringApiCompletion *completion);
int completion_counts_pending(const UringApiCompletion *completion);
int should_enqueue_conflict(UringApiRing *self, UringApiCompletion *completion, UringApiFdSlot **slot_out);
int enqueue_conflict(UringApiRing *self, UringApiCompletion *completion);
int enqueue_fill_wait(UringApiRing *self, UringApiCompletion *completion, int already_in_flight);
int drain_fd_slot(UringApiRing *self, UringApiFdSlot *slot, int flush_if_full, int *submitted_out);
/* Drain fill-wait then conflict FIFOs into the SQ. flush_if_full: submit a
 * full SQ (submit() path). prepare passes 0 so auto_submit still gates room.
 * submitted_out, if non-NULL, accumulates SQEs flushed to make room. */
int drain_parked(UringApiRing *self, int flush_if_full, int *submitted_out);
void clear_parked(UringApiRing *self);

#endif

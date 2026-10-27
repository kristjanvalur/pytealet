#ifndef URING_API_PARK_H
#define URING_API_PARK_H

/* private: fill-wait and per-fd conflict parks. */

#include "uring_api_common.h"

int drain_parked(UringApiRing *self, int flush_if_full, int *submitted_out);
/* Copy fill-wait heads whose submission already fits. Do not enter, and do
 * not drain per-fd FIFOs. 0: stopped (list empty, or the head needs more
 * free slots). -1: error. Caller holds the ring critical section. */
int drain_fill_wait_if_fitted(UringApiRing *self);
void clear_parked(UringApiRing *self);
int should_enqueue_conflict(UringApiRing *self, UringApiCompletion *completion, UringApiFdSlot **slot_out);
/* Caller holds the ring CS. 1 when a nowait op must sit on the conflict FIFO
 * instead of the SQ. Cancel parks only when its target is itself queued.
 * Close and shutdown park whenever that fd is busy. */
int nowait_cancel_must_park(UringApiRing *self, UringApiCompletion *target);
int nowait_fd_op_must_park(UringApiRing *self, int fd);
int enqueue_conflict(UringApiRing *self, UringApiCompletion *completion);
int enqueue_fill_wait(UringApiRing *self, UringApiCompletion *completion, int already_in_flight);
int drain_fd_slot(UringApiRing *self, UringApiFdSlot *slot, int flush_if_full, int *submitted_out);

#endif

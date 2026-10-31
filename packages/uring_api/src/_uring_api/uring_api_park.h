#ifndef URING_API_PARK_H
#define URING_API_PARK_H

/* private: fill-wait and per-fd conflict parks. */

#include "uring_api_common.h"

int drain_parked(UringApiRing *self, int flush_if_full, int *submitted_out);
/* Copy fill-wait heads whose whole submission already fits. Stop at the
 * first that does not. Do not enter, and do not touch per-fd FIFOs.
 * 1: the list is empty, so the caller may take slots. 0: a head remains,
 * so the caller parks. -1: error (the list may still hold that head).
 * Caller holds the ring critical section. */
int fill_wait_make_way(UringApiRing *self);
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
/* Nothing is in progress, so this op is about to be filled rather than
 * parked. An abortive slot poisons send, send_all, sendmsg, and shutdown.
 * Close is left alone: its fill drops the mark. Caller holds the ring CS. */
void poison_direct_on_idle_abortive(UringApiRing *self, UringApiCompletion *completion);

#endif

#ifndef URING_API_PARK_H
#define URING_API_PARK_H

/* private: per-fd conflict parks. */

#include "uring_api_common.h"

void clear_parked(UringApiRing *self);
int should_enqueue_conflict(UringApiRing *self, UringApiCompletion *completion, UringApiFdSlot **slot_out);
/* Caller holds the ring CS. 1 when a nowait op must sit on the conflict FIFO
 * instead of the SQ. Cancel parks only when its target is itself queued.
 * Close and shutdown park whenever that fd is busy. */
int nowait_cancel_must_park(UringApiRing *self, UringApiCompletion *target);
int nowait_fd_op_must_park(UringApiRing *self, int fd);
int enqueue_conflict(UringApiRing *self, UringApiCompletion *completion);
/* Poison a conflicting op that is leaving an abortive slot. Close and
 * cancel are left alone: the slot mark is not stored on the close.
 * Idle and busy slots share this. Caller holds the ring CS. */
void stage_a_copy_abortive(UringApiRing *self, UringApiCompletion *completion, int *poisoned_here);
/* Clear the slot mark after that close's SQE was taken. The mark comes
 * from a failed nowait send_all or abort_queue. Does not clear SO_LINGER.
 * An idle slot is freed. Caller holds the ring CS. */
void stage_a_discharge_close(UringApiRing *self, UringApiCompletion *completion);
/* Copy until the next send_all is active or the FIFO is empty. A full SQ
 * enters. make_room is not consulted. 0 drained, -1 making room failed. */
int drain_fd_slot(UringApiRing *self, UringApiFdSlot *slot);

#endif

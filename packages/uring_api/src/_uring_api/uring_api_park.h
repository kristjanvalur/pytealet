#ifndef URING_API_PARK_H
#define URING_API_PARK_H

/* private: fill-wait and per-fd conflict parks. */

#include "uring_api_common.h"

int drain_parked(UringApiRing *self, int flush_if_full, int *submitted_out);
void clear_parked(UringApiRing *self);
int should_enqueue_conflict(UringApiRing *self, UringApiCompletion *completion, UringApiFdSlot **slot_out);
int enqueue_conflict(UringApiRing *self, UringApiCompletion *completion);
int enqueue_fill_wait(UringApiRing *self, UringApiCompletion *completion, int already_in_flight);
int drain_fd_slot(UringApiRing *self, UringApiFdSlot *slot, int flush_if_full, int *submitted_out);

#endif

#ifndef URING_API_FD_TABLE_H
#define URING_API_FD_TABLE_H

/* private: per-fd send-all busy slot + circular Completion FIFOs. callers hold the ring CS. */

#include "uring_api_common.h"

UringApiFdSlot *fd_table_lookup(UringApiRing *self, int fd);
UringApiFdSlot *fd_table_get(UringApiRing *self, int fd);
void fd_table_mark_drain(UringApiRing *self, UringApiFdSlot *slot);
void fd_table_unlink_drain(UringApiRing *self, UringApiFdSlot *slot);
void fd_table_try_free(UringApiRing *self, UringApiFdSlot *slot);
/* Push adopts one reference. It does not incref. Pop returns that reference.
 * A failed push leaves the reference with the caller. */
int completion_fifo_push(UringApiCompletionFifo *fifo, UringApiCompletion *completion);
UringApiCompletion *completion_fifo_peek(UringApiCompletionFifo *fifo);
UringApiCompletion *completion_fifo_pop(UringApiCompletionFifo *fifo);
int completion_fifo_traverse(UringApiCompletionFifo *fifo, visitproc visit, void *arg);
void completion_fifo_clear(UringApiRing *ring, UringApiCompletionFifo *fifo);
int fd_table_fifo_push(UringApiFdSlot *slot, UringApiCompletion *completion);
UringApiCompletion *fd_table_fifo_peek(UringApiFdSlot *slot);
UringApiCompletion *fd_table_fifo_pop(UringApiFdSlot *slot);
int fd_table_traverse(UringApiRing *self, visitproc visit, void *arg);
void fd_table_clear(UringApiRing *self);
/* Caller holds the ring CS. 1 when completion is on this slot's FIFO or is
 * its active send_all. Sets abortive. *active_out is a new reference to the
 * in-progress send_all, or NULL when nothing is in progress. 0 when the
 * completion is not on a live pipeline. */
int fd_table_abort_pipeline(UringApiRing *self, UringApiCompletion *completion, UringApiCompletion **active_out);

#endif

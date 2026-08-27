#ifndef URING_API_FD_TABLE_H
#define URING_API_FD_TABLE_H

/* private: per-fd send-all busy slot + conflict FIFO. callers hold the ring CS. */

#include "uring_api_common.h"

UringApiFdSlot *fd_table_lookup(UringApiRing *self, int fd);
UringApiFdSlot *fd_table_get(UringApiRing *self, int fd);
void fd_table_mark_drain(UringApiRing *self, UringApiFdSlot *slot);
void fd_table_unlink_drain(UringApiRing *self, UringApiFdSlot *slot);
void fd_table_try_free(UringApiRing *self, UringApiFdSlot *slot);
int fd_table_fifo_push(UringApiFdSlot *slot, UringApiCompletion *completion);
UringApiCompletion *fd_table_fifo_peek(UringApiFdSlot *slot);
UringApiCompletion *fd_table_fifo_pop(UringApiFdSlot *slot);
int fd_table_traverse(UringApiRing *self, visitproc visit, void *arg);
void fd_table_clear(UringApiRing *self);

#endif

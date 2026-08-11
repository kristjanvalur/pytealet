#ifndef URING_API_STAGING_H
#define URING_API_STAGING_H

/* private implementation header; not part of the public C API. */

#include "uring_api_common.h"

struct io_uring_cqe;

void staging_buffer_clear(UringApiStagingBuffer *buf);
void staging_buffer_reset(UringApiStagingBuffer *buf);
int staging_buffer_record_cqe(UringApiRing *self, UringApiStagingBuffer *buf, struct io_uring_cqe *cqe);
/* invoke nowait_error_handler for staged failures; requires the GIL; never fails the drain */
void staging_flush_nowait_errors(UringApiRing *self, UringApiStagingBuffer *buf);

#endif

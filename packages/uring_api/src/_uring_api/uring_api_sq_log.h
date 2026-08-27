#ifndef URING_API_SQ_LOG_H
#define URING_API_SQ_LOG_H

/*
 * Opt-in SQ tracer: URING_API_SQ_LOG=1 (or true/yes) logs to stderr.
 * Prepare = SQE filled. Park = fill-wait or conflict FIFO. Submit = io_uring_enter.
 */

#include "uring_api_completion.h"
#include "uring_api_core.h"

void uring_api_sq_log_init(void);
int uring_api_sq_log_enabled(void);
void uring_api_sq_log_prepare(UringApiCompletion *completion, const char *how);
void uring_api_sq_log_prepare_nowait(unsigned int kind, int fd);
void uring_api_sq_log_prepare_nop(void);
void uring_api_sq_log_park(UringApiCompletion *completion, const char *queue);
void uring_api_sq_log_submit(unsigned int sq_ready, int submitted);

#endif

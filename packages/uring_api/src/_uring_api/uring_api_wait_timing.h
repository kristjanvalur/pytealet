#ifndef URING_API_WAIT_TIMING_H
#define URING_API_WAIT_TIMING_H

/*
 * Opt-in Ring.wait split: URING_API_WAIT_TIMING=1 (or true/yes) dumps on close.
 * Splits SQ flush, first CQE reap (CQ already ready vs empty enter), extra
 * peeks, CQE packaging, and delivery callback.
 */

#include <stddef.h>

void uring_api_wait_timing_init(void);
int uring_api_wait_timing_enabled(void);
unsigned long long uring_api_wait_timing_now_ns(void);

void uring_api_wait_timing_note_wait(int timeout_kind);
void uring_api_wait_timing_add_flush(unsigned long long ns);
void uring_api_wait_timing_add_first_reap(unsigned cq_ready, int timeout_kind, int reap_ret, unsigned long long ns);
void uring_api_wait_timing_add_extra_peek(unsigned n, unsigned long long ns);
void uring_api_wait_timing_add_build(unsigned long long ns, size_t staged);
void uring_api_wait_timing_add_delivery(unsigned long long ns);
void uring_api_wait_timing_dump(void);

#endif

/*
 * Opt-in perf-counter breakdown for hot submit paths.
 *
 * Enable with URING_API_SUBMIT_TRACE=1 and read aggregated averages via
 * Ring.drain_submit_trace().
 */

#ifndef URING_API_SUBMIT_TRACE_H
#define URING_API_SUBMIT_TRACE_H

#include <Python.h>
#include <stdbool.h>

typedef enum UringApiSubmitTracePhase {
    URING_API_SUBMIT_TRACE_RECV_MS_TOTAL = 0,
    URING_API_SUBMIT_TRACE_PARSE,
    URING_API_SUBMIT_TRACE_VALIDATE,
    URING_API_SUBMIT_TRACE_COMPLETION_NEW,
    URING_API_SUBMIT_TRACE_RING_LOCK,
    URING_API_SUBMIT_TRACE_CHECK_OPEN,
    URING_API_SUBMIT_TRACE_GET_SQE,
    URING_API_SUBMIT_TRACE_GET_SQE_FLUSH,
    URING_API_SUBMIT_TRACE_PREP,
    URING_API_SUBMIT_TRACE_SET_DATA,
    URING_API_SUBMIT_TRACE_SUBMIT_ONE,
    URING_API_SUBMIT_TRACE_RETURN_REF,
    URING_API_SUBMIT_TRACE_PHASE_COUNT,
} UringApiSubmitTracePhase;

bool uring_api_submit_trace_enabled(void);
void uring_api_submit_trace_begin_recv_multishot(void);
void uring_api_submit_trace_end_recv_multishot(void);
void uring_api_submit_trace_mark(UringApiSubmitTracePhase phase);
PyObject *uring_api_submit_trace_drain(void);
void uring_api_submit_trace_reset(void);

#endif
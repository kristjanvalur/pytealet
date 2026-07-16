/*
 * Opt-in submit-path timing accumulators.
 */

#include "uring_api_submit_trace.h"

#include <Python.h>
#include <stdlib.h>
#include <time.h>

typedef struct {
    unsigned long long total_ns;
    unsigned long long count;
} UringApiSubmitTraceBucket;

static UringApiSubmitTraceBucket submit_trace_buckets[URING_API_SUBMIT_TRACE_PHASE_COUNT];
static PyThread_type_lock submit_trace_lock = NULL;
static int submit_trace_enabled_cached = -1;

static _Thread_local unsigned long long submit_trace_last_ns;
static _Thread_local int submit_trace_recv_multishot_active;

static const char *submit_trace_phase_names[URING_API_SUBMIT_TRACE_PHASE_COUNT] = {
    "recv_ms_total",
    "parse",
    "validate",
    "completion_new",
    "ring_lock",
    "check_open",
    "get_sqe",
    "get_sqe_flush",
    "prep",
    "set_data",
    "submit_one",
    "return_ref",
};

static unsigned long long submit_trace_now_ns(void) {
    struct timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0;
    }
    return (unsigned long long)ts.tv_sec * 1000000000ULL + (unsigned long long)ts.tv_nsec;
}

static void submit_trace_ensure_lock(void) {
    if (submit_trace_lock == NULL) {
        submit_trace_lock = PyThread_allocate_lock();
    }
}

bool uring_api_submit_trace_enabled(void) {
    const char *value;

    if (submit_trace_enabled_cached >= 0) {
        return submit_trace_enabled_cached != 0;
    }
    value = getenv("URING_API_SUBMIT_TRACE");
    submit_trace_enabled_cached =
        (value != NULL && (value[0] == '1' || value[0] == 'y' || value[0] == 'Y' || value[0] == 't' || value[0] == 'T'))
            ? 1
            : 0;
    return submit_trace_enabled_cached != 0;
}

void uring_api_submit_trace_reset(void) {
    size_t index;

    submit_trace_ensure_lock();
    PyThread_acquire_lock(submit_trace_lock, WAIT_LOCK);
    for (index = 0; index < URING_API_SUBMIT_TRACE_PHASE_COUNT; index++) {
        submit_trace_buckets[index].total_ns = 0;
        submit_trace_buckets[index].count = 0;
    }
    PyThread_release_lock(submit_trace_lock);
}

void uring_api_submit_trace_mark(UringApiSubmitTracePhase phase) {
    unsigned long long now;
    unsigned long long delta;

    if (!uring_api_submit_trace_enabled() || !submit_trace_recv_multishot_active) {
        return;
    }
    now = submit_trace_now_ns();
    delta = now - submit_trace_last_ns;
    submit_trace_last_ns = now;

    submit_trace_ensure_lock();
    PyThread_acquire_lock(submit_trace_lock, WAIT_LOCK);
    submit_trace_buckets[phase].total_ns += delta;
    submit_trace_buckets[phase].count++;
    PyThread_release_lock(submit_trace_lock);
}

void uring_api_submit_trace_begin_recv_multishot(void) {
    if (!uring_api_submit_trace_enabled()) {
        return;
    }
    submit_trace_recv_multishot_active = 1;
    submit_trace_last_ns = submit_trace_now_ns();
}

void uring_api_submit_trace_end_recv_multishot(void) {
    if (!submit_trace_recv_multishot_active) {
        return;
    }
    uring_api_submit_trace_mark(URING_API_SUBMIT_TRACE_RETURN_REF);
    submit_trace_recv_multishot_active = 0;

    submit_trace_ensure_lock();
    PyThread_acquire_lock(submit_trace_lock, WAIT_LOCK);
    submit_trace_buckets[URING_API_SUBMIT_TRACE_RECV_MS_TOTAL].count++;
    PyThread_release_lock(submit_trace_lock);
}

PyObject *uring_api_submit_trace_drain(void) {
    PyObject *rows;
    size_t index;

    rows = PyList_New(0);
    if (!rows) {
        return NULL;
    }

    submit_trace_ensure_lock();
    PyThread_acquire_lock(submit_trace_lock, WAIT_LOCK);
    for (index = 0; index < URING_API_SUBMIT_TRACE_PHASE_COUNT; index++) {
        PyObject *row;
        double avg_us = 0.0;

        if (submit_trace_buckets[index].count > 0) {
            avg_us = (double)submit_trace_buckets[index].total_ns / (double)submit_trace_buckets[index].count / 1000.0;
        }
        row = Py_BuildValue(
            "(skd)",
            submit_trace_phase_names[index],
            (unsigned long long)submit_trace_buckets[index].count,
            avg_us);
        if (!row) {
            Py_DECREF(rows);
            PyThread_release_lock(submit_trace_lock);
            return NULL;
        }
        if (PyList_Append(rows, row) < 0) {
            Py_DECREF(row);
            Py_DECREF(rows);
            PyThread_release_lock(submit_trace_lock);
            return NULL;
        }
        Py_DECREF(row);
    }
    PyThread_release_lock(submit_trace_lock);
    return rows;
}
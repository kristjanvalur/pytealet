#ifndef URING_API_IDLE_H
#define URING_API_IDLE_H

/* Host-side idle park for the _uring_api extension.
 *
 * Separate from CQ wait/serve: completion workers may own the reaper while the
 * scheduler driver parks here. Uses PyThread_type_lock as a binary gate
 * (semaphore-style) plus UringApiMutex (PyMutex when available) for the latch.
 *
 * Contract: multiple signallers, one waiter. Concurrent waiters are not
 * supported; a second wait() can miss a coalesced signal meant for the first.
 * break_wait / close may signal from many threads; only one host parks.
 *
 * Include after UringApiMutex is defined (via uring_api_common.h).
 */

typedef struct UringApiIdlePark {
    /* Binary gate: held means "no permit"; release wakes the single waiter. */
    PyThread_type_lock wait_lock;
    /* Protects ``signaled`` only (short critical section). */
    UringApiMutex state_mutex;
    int signaled;
} UringApiIdlePark;

/* 0 on success, -1 on failure (sets MemoryError). Gate starts closed. */
int UringApiIdlePark_init(UringApiIdlePark *park);

/* Wake the waiter (if any), then free locks. Not safe while wait() is in progress. */
void UringApiIdlePark_fini(UringApiIdlePark *park);

/* Latch a wake: safe from many signallers; coalesces if already signaled. */
void UringApiIdlePark_signal(UringApiIdlePark *park);

/*
 * Park until signal or timeout. At most one concurrent waiter (see file header).
 * timeout_sec == NULL: block indefinitely.
 * timeout_sec points to 0.0: non-blocking poll.
 * Returns 1 if a signal was consumed, 0 on timeout.
 * GIL is released while blocked.
 */
int UringApiIdlePark_wait(UringApiIdlePark *park, const double *timeout_sec);

#endif

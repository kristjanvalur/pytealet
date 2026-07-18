#ifndef URING_API_IDLE_H
#define URING_API_IDLE_H

/* Host-side idle park for the _uring_api extension.
 *
 * Separate from CQ wait/serve: completion workers may own the reaper while the
 * scheduler driver parks here. Uses PyThread_type_lock as a binary gate
 * (semaphore-style) plus UringApiMutex (PyMutex when available) for the latch.
 *
 * Include after UringApiMutex is defined (via uring_api_common.h).
 */

typedef struct UringApiIdlePark {
    /* Binary gate: held means "no permit"; release wakes one waiter. */
    PyThread_type_lock wait_lock;
    /* Protects ``signaled`` only (short critical section). */
    UringApiMutex state_mutex;
    int signaled;
} UringApiIdlePark;

/* 0 on success, -1 on failure (sets MemoryError). Gate starts closed. */
int UringApiIdlePark_init(UringApiIdlePark *park);

/* Wake any waiter, then free locks. Not safe while another thread is in wait(). */
void UringApiIdlePark_fini(UringApiIdlePark *park);

/* Latch a wake: safe if already signaled (no double-release). */
void UringApiIdlePark_signal(UringApiIdlePark *park);

/*
 * Park until signal or timeout.
 * timeout_sec == NULL: block indefinitely.
 * timeout_sec points to 0.0: non-blocking poll.
 * Returns 1 if a signal was consumed, 0 on timeout.
 * GIL is released while blocked.
 */
int UringApiIdlePark_wait(UringApiIdlePark *park, const double *timeout_sec);

#endif

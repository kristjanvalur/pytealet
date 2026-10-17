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
    /* Nested count. Non-zero while the host is inside wait_idle, or across
     * the submit-then-park span note_driver_waiting covers. Not the
     * signaled latch: that stays clear after the host has consumed a wake
     * and is running again. */
    atomic_int waiting_depth;
    /* Sticky. 1 after a blocking wait (timeout other than zero) or
     * note_driver_waiting(True). A poll does not set it. */
    atomic_int blocks;
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

/* +1 on enter, -1 on leave. -1 and sets no exception when a leave underflows;
 * the caller raises. */
int UringApiIdlePark_waiting_add(UringApiIdlePark *park, int delta);
/* 1 when waiting_depth is non-zero. */
int UringApiIdlePark_is_waiting(UringApiIdlePark *park);
/* Remember that this host blocks in its loop. Sticky. */
void UringApiIdlePark_latch_blocks(UringApiIdlePark *park);
/* 1 once latch_blocks has run. */
int UringApiIdlePark_has_blocked(UringApiIdlePark *park);

#endif

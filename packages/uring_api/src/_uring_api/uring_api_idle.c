/*
 * Host idle park: PyThread gate + mutex-protected latch.
 */

#include "uring_api_common.h"

#include <assert.h>
#include <limits.h>
#include <time.h>

int UringApiIdlePark_init(UringApiIdlePark *park) {
    memset(park, 0, sizeof(*park));

    park->wait_lock = PyThread_allocate_lock();
    if (!park->wait_lock) {
        PyErr_NoMemory();
        return -1;
    }
    /* close the gate: next wait blocks until signal releases the lock. */
    PyThread_acquire_lock(park->wait_lock, WAIT_LOCK);

#ifdef URING_API_USE_PYTHREAD_MUTEX
    park->state_mutex = PyThread_allocate_lock();
    if (!park->state_mutex) {
        PyThread_release_lock(park->wait_lock);
        PyThread_free_lock(park->wait_lock);
        park->wait_lock = NULL;
        PyErr_NoMemory();
        return -1;
    }
#endif
    /* PyMutex path: zero-init from memset is the default unlocked state. */
    park->signaled = 0;
    return 0;
}

void UringApiIdlePark_fini(UringApiIdlePark *park) {
    if (!park->wait_lock) {
        return;
    }

    /* leave wait_lock free before free_lock: if the gate is closed we hold it. */
    uring_api_refcount_mutex_lock(&park->state_mutex);
    if (!park->signaled) {
        PyThread_release_lock(park->wait_lock);
        park->signaled = 1;
    }
    uring_api_refcount_mutex_unlock(&park->state_mutex);

    PyThread_free_lock(park->wait_lock);
    park->wait_lock = NULL;

#ifdef URING_API_USE_PYTHREAD_MUTEX
    if (park->state_mutex) {
        PyThread_free_lock(park->state_mutex);
        park->state_mutex = NULL;
    }
#endif
}

void UringApiIdlePark_signal(UringApiIdlePark *park) {
    if (!park->wait_lock) {
        return;
    }

    uring_api_refcount_mutex_lock(&park->state_mutex);
    if (!park->signaled) {
        park->signaled = 1;
        /* release the gate permit (multi-signaller latch, single waiter). */
        PyThread_release_lock(park->wait_lock);
    }
    uring_api_refcount_mutex_unlock(&park->state_mutex);
}

static int64_t idle_monotonic_us(void) {
    struct timespec now;

    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) {
        return -1;
    }
    return (int64_t)now.tv_sec * 1000000LL + (int64_t)now.tv_nsec / 1000LL;
}

static void idle_leave_waiting(UringApiIdlePark *park) {
    /* paired with the enter in wait. underflow means our own bookkeeping broke. */
    if (UringApiIdlePark_waiting_add(park, -1) < 0) {
        assert(0);
    }
}

int UringApiIdlePark_wait(UringApiIdlePark *park, const double *timeout_sec) {
    PY_TIMEOUT_T microseconds;
    PyLockStatus status;
    int64_t deadline_us = 0;

    if (!park->wait_lock) {
        return 0;
    }

    if (timeout_sec == NULL) {
        microseconds = -1;
    } else if (*timeout_sec <= 0.0) {
        microseconds = 0;
    } else {
        double us = *timeout_sec * 1000000.0;
        int64_t now_us;

        if (us >= (double)PY_TIMEOUT_MAX) {
            microseconds = PY_TIMEOUT_MAX;
        } else {
            microseconds = (PY_TIMEOUT_T)us;
        }
        now_us = idle_monotonic_us();
        if (now_us < 0) {
            PyErr_SetFromErrno(PyExc_OSError);
            return -1;
        }
        deadline_us = now_us + (int64_t)microseconds;
    }

    /* cover the blocked acquire itself. a host that also submits before
     * parking nests note_driver_waiting around this. a zero timeout is a
     * poll and must not arm the blocking-wait latch. */
    UringApiIdlePark_waiting_add(park, 1);
    if (timeout_sec == NULL || *timeout_sec > 0.0) {
        UringApiIdlePark_latch_blocks(park);
    }
    for (;;) {
        Py_BEGIN_ALLOW_THREADS;
        /* intr_flag: EINTR returns here instead of being swallowed. */
        status = PyThread_acquire_lock_timed(park->wait_lock, microseconds, 1);
        Py_END_ALLOW_THREADS;
        if (status != PY_LOCK_INTR) {
            break;
        }
        /* default SIGINT raises KeyboardInterrupt. a handler that returns
         * leaves this park running, with the time still left. */
        if (PyErr_CheckSignals() < 0) {
            idle_leave_waiting(park);
            return -1;
        }
        if (microseconds < 0) {
            continue;
        }
        if (microseconds == 0) {
            status = PY_LOCK_FAILURE;
            break;
        }
        {
            int64_t now_us = idle_monotonic_us();
            int64_t left;

            if (now_us < 0) {
                PyErr_SetFromErrno(PyExc_OSError);
                idle_leave_waiting(park);
                return -1;
            }
            left = deadline_us - now_us;
            if (left <= 0) {
                status = PY_LOCK_FAILURE;
                break;
            }
            if (left > (int64_t)PY_TIMEOUT_MAX) {
                microseconds = PY_TIMEOUT_MAX;
            } else {
                microseconds = (PY_TIMEOUT_T)left;
            }
        }
    }
    idle_leave_waiting(park);

    if (status != PY_LOCK_ACQUIRED) {
        return 0;
    }

    /* consume the latch; leave wait_lock held so the gate is closed again. */
    uring_api_refcount_mutex_lock(&park->state_mutex);
    park->signaled = 0;
    uring_api_refcount_mutex_unlock(&park->state_mutex);
    return 1;
}

int UringApiIdlePark_waiting_add(UringApiIdlePark *park, int delta) {
    int previous;

    previous = atomic_fetch_add_explicit(&park->waiting_depth, delta, memory_order_acq_rel);
    if (delta < 0 && previous <= 0) {
        atomic_fetch_add_explicit(&park->waiting_depth, 1, memory_order_acq_rel);
        return -1;
    }
    return 0;
}

int UringApiIdlePark_is_waiting(UringApiIdlePark *park) {
    return atomic_load_explicit(&park->waiting_depth, memory_order_acquire) > 0;
}

void UringApiIdlePark_latch_blocks(UringApiIdlePark *park) {
    atomic_store_explicit(&park->blocks, 1, memory_order_release);
}

int UringApiIdlePark_has_blocked(UringApiIdlePark *park) {
    return atomic_load_explicit(&park->blocks, memory_order_acquire) != 0;
}

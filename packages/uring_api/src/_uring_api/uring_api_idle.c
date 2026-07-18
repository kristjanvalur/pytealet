/*
 * Host idle park: PyThread gate + mutex-protected latch.
 */

#include "uring_api_common.h"

#include <limits.h>

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

int UringApiIdlePark_wait(UringApiIdlePark *park, const double *timeout_sec) {
    PY_TIMEOUT_T microseconds;
    PyLockStatus status;

    if (!park->wait_lock) {
        return 0;
    }

    if (timeout_sec == NULL) {
        microseconds = -1;
    } else if (*timeout_sec <= 0.0) {
        microseconds = 0;
    } else {
        double us = *timeout_sec * 1000000.0;
        if (us >= (double)PY_TIMEOUT_MAX) {
            microseconds = PY_TIMEOUT_MAX;
        } else {
            microseconds = (PY_TIMEOUT_T)us;
        }
    }

    Py_BEGIN_ALLOW_THREADS;
    status = PyThread_acquire_lock_timed(park->wait_lock, microseconds, 0);
    Py_END_ALLOW_THREADS;

    if (status != PY_LOCK_ACQUIRED) {
        return 0;
    }

    /* consume the latch; leave wait_lock held so the gate is closed again. */
    uring_api_refcount_mutex_lock(&park->state_mutex);
    park->signaled = 0;
    uring_api_refcount_mutex_unlock(&park->state_mutex);
    return 1;
}

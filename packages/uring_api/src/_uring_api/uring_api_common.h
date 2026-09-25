#ifndef URING_API_COMMON_H
#define URING_API_COMMON_H

/* private implementation header; not part of the public C API. */

#define PY_SSIZE_T_CLEAN

#include <Python.h>
#include <arpa/inet.h>
#include <errno.h>
#include <liburing.h>
#include <limits.h>
#include <netinet/in.h>
#include <pthread.h>
#include <pythread.h>
#include <stdatomic.h>
#include <stdbool.h>

#include "uring_api_completion_kinds.h"
#include <assert.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#if !defined(IO_URING_VERSION_MAJOR) || !defined(IO_URING_VERSION_MINOR)
#error "uring-api requires liburing >= 2.4 development headers"
#elif IO_URING_VERSION_MAJOR < 2 || (IO_URING_VERSION_MAJOR == 2 && IO_URING_VERSION_MINOR < 4)
#error "uring-api requires liburing >= 2.4 development headers"
#endif

typedef struct UringApiRing UringApiRing;
typedef struct UringApiFdSlot UringApiFdSlot;
typedef int (*UringApiCompletionCallback)(PyObject *ring, PyObject *completion, void *user_data);

#ifndef Py_BEGIN_CRITICAL_SECTION
#define URING_API_USE_PYTHREAD_RING_LOCK 1
#define Py_BEGIN_CRITICAL_SECTION(op)                                                                                  \
    {                                                                                                                  \
        PyThread_type_lock _uring_api_critical_section_lock = ((UringApiRing *)(op))->ring_lock;                       \
        PyThread_acquire_lock(_uring_api_critical_section_lock, WAIT_LOCK);
#define Py_END_CRITICAL_SECTION()                                                                                      \
    PyThread_release_lock(_uring_api_critical_section_lock);                                                           \
    }
#endif

#ifndef Py_BEGIN_CRITICAL_SECTION_MUTEX
#define URING_API_USE_PYTHREAD_MUTEX 1
typedef PyThread_type_lock UringApiMutex;
#define Py_BEGIN_CRITICAL_SECTION_MUTEX(mutex)                                                                         \
    {                                                                                                                  \
        PyThread_type_lock _uring_api_mutex = *(mutex);                                                                \
        PyThread_acquire_lock(_uring_api_mutex, WAIT_LOCK);
#else
typedef PyMutex UringApiMutex;
#endif

#ifdef URING_API_USE_PYTHREAD_MUTEX
#define Py_END_CRITICAL_SECTION_MUTEX()                                                                                \
    PyThread_release_lock(_uring_api_mutex);                                                                           \
    }
#elif !defined(Py_END_CRITICAL_SECTION_MUTEX)
#define Py_END_CRITICAL_SECTION_MUTEX() Py_END_CRITICAL_SECTION()
#endif

/* refcount_mutex may be touched from Py_BEGIN_ALLOW_THREADS drain paths where the
 * thread is detached and PyCriticalSection_* cannot run (free-threaded builds). */
#if defined(URING_API_USE_PYTHREAD_MUTEX)
static inline void uring_api_refcount_mutex_lock(UringApiMutex *mutex) { PyThread_acquire_lock(*mutex, WAIT_LOCK); }

static inline void uring_api_refcount_mutex_unlock(UringApiMutex *mutex) { PyThread_release_lock(*mutex); }
#else
static inline void uring_api_refcount_mutex_lock(UringApiMutex *mutex) { PyMutex_Lock(mutex); }

static inline void uring_api_refcount_mutex_unlock(UringApiMutex *mutex) { PyMutex_Unlock(mutex); }
#endif

#include "uring_api_idle.h"

#ifndef _PyCFunction_CAST
#define _PyCFunction_CAST(func) ((PyCFunction)(void (*)(void))(func))
#endif

/* 3.15 added PyArg_ParseArrayAndKeywords for METH_FASTCALL | METH_KEYWORDS.
 * tp_new / tp_init still take a tuple+dict and keep PyArg_ParseTupleAndKeywords.
 */
#if PY_VERSION_HEX >= 0x030F0000
#define URING_API_HAS_PARSEARRAY 1
#define URING_API_METH_KEYWORDS (METH_FASTCALL | METH_KEYWORDS)
#define URING_API_PARSE_ARGS PyObject *const *args, Py_ssize_t nargs, PyObject *kwnames
#define URING_API_PARSE_PASS args, nargs, kwnames
#define URING_API_PARSE_KEYWORDS(fmt, kwlist, ...)                                                                     \
    PyArg_ParseArrayAndKeywords(args, nargs, kwnames, (fmt), (const char *const *)(kwlist), __VA_ARGS__)
#else
#define URING_API_HAS_PARSEARRAY 0
#define URING_API_METH_KEYWORDS (METH_VARARGS | METH_KEYWORDS)
#define URING_API_PARSE_ARGS PyObject *args, PyObject *kwargs
#define URING_API_PARSE_PASS args, kwargs
#define URING_API_PARSE_KEYWORDS(fmt, kwlist, ...)                                                                     \
    PyArg_ParseTupleAndKeywords(args, kwargs, (fmt), (kwlist), __VA_ARGS__)
#endif

typedef enum {
    URING_API_RECEIVE_IDLE = 0,
    URING_API_RECEIVE_WAITING = 1,
    URING_API_RECEIVE_DELIVERING = 2,
} UringApiReceiveState;

typedef enum {
    URING_API_PENDING_RECV = URING_API_COMPLETION_KIND_RECV,
    URING_API_PENDING_SEND = URING_API_COMPLETION_KIND_SEND,
    URING_API_PENDING_WAKE = URING_API_COMPLETION_KIND_WAKE,
    URING_API_PENDING_SENDTO = URING_API_COMPLETION_KIND_SENDTO,
    URING_API_PENDING_RECVMSG = URING_API_COMPLETION_KIND_RECVMSG,
    URING_API_PENDING_ACCEPT = URING_API_COMPLETION_KIND_ACCEPT,
    URING_API_PENDING_CONNECT = URING_API_COMPLETION_KIND_CONNECT,
    URING_API_PENDING_CANCEL = URING_API_COMPLETION_KIND_CANCEL,
    URING_API_PENDING_SHUTDOWN = URING_API_COMPLETION_KIND_SHUTDOWN,
    URING_API_PENDING_CLOSE = URING_API_COMPLETION_KIND_CLOSE,
    URING_API_PENDING_SENDMSG = URING_API_COMPLETION_KIND_SENDMSG,
    URING_API_PENDING_SOCKET = URING_API_COMPLETION_KIND_SOCKET,
    URING_API_PENDING_RECV_MULTISHOT = URING_API_COMPLETION_KIND_RECV_MULTISHOT,
    URING_API_PENDING_SEND_ZC = URING_API_COMPLETION_KIND_SEND_ZC,
    URING_API_PENDING_SENDMSG_ZC = URING_API_COMPLETION_KIND_SENDMSG_ZC,
    URING_API_PENDING_RECV_BUF = URING_API_COMPLETION_KIND_RECV_BUF,
    URING_API_PENDING_POLL = URING_API_COMPLETION_KIND_POLL,
    URING_API_PENDING_POLL_MULTISHOT = URING_API_COMPLETION_KIND_POLL_MULTISHOT,
    URING_API_PENDING_POLL_REMOVE = URING_API_COMPLETION_KIND_POLL_REMOVE,
    URING_API_PENDING_READ = URING_API_COMPLETION_KIND_READ,
    URING_API_PENDING_WRITE = URING_API_COMPLETION_KIND_WRITE,
    URING_API_PENDING_OPENAT = URING_API_COMPLETION_KIND_OPENAT,
    URING_API_PENDING_STATX = URING_API_COMPLETION_KIND_STATX,
    URING_API_PENDING_STATX_FDSIZE = URING_API_COMPLETION_KIND_STATX_FDSIZE,
    URING_API_PENDING_SEND_ALL = URING_API_COMPLETION_KIND_SEND_ALL,
} UringApiPendingKind;

typedef enum {
    URING_API_COMPLETION_STATE_NONE = 0,
    URING_API_COMPLETION_STATE_VIEW,
    URING_API_COMPLETION_STATE_BUF_GROUP,
    URING_API_COMPLETION_STATE_SOCKADDR,
    URING_API_COMPLETION_STATE_VIEW_SOCKADDR,
    URING_API_COMPLETION_STATE_MSG,
    URING_API_COMPLETION_STATE_PATH,
    URING_API_COMPLETION_STATE_STATX,
    URING_API_COMPLETION_STATE_STATX_FDSIZE,
    URING_API_COMPLETION_STATE_SCALAR,
} UringApiCompletionStateKind;

typedef struct UringApiCompletion {
    PyObject_HEAD UringApiPendingKind kind;
    PyObject *user_data;
    PyObject *cancel_target;
    int res;
    unsigned int flags;
    PyObject *result;
    unsigned long long sequence;
    int aux_refcount;
    /* borrowed ring->refcount_mutex; set at prepare. NULL on shells / unprepared. */
    UringApiMutex *aux_lock;
    /* packed: MULTISHOT | AUX_DECREF | PREPARED | SKIP_SUCCESS | USER_DATA_CLEAR |
     * SEND_ALL_CONT | SEND_ALL_ABANDON | CONFLICT_QUEUED | FILL_WAIT | SKIP_ALL.
     * atomic: cancel sets ABANDON under the ring CS while CQE drain may set
     * AUX_DECREF under refcount_mutex. */
    atomic_uint_least16_t bits;
    void *state;
} UringApiCompletion;

typedef struct UringApiStagedCQE {
    int res;
    unsigned int flags;
    UringApiCompletion *completion;
    unsigned long long leg_index;
} UringApiStagedCQE;

/* extra serve workers: copied CQEs, not the kernel CQ. */
typedef struct UringApiCqeFifo {
    UringApiStagedCQE *items;
    size_t head;
    size_t count;
    size_t cap;
} UringApiCqeFifo;

typedef struct UringApiCompletionFifo {
    UringApiCompletion **items;
    size_t head;
    size_t count;
    size_t cap;
} UringApiCompletionFifo;

struct UringApiFdSlot {
    int fd;
    /* send-all whose SQE is filled, in-kernel, or next-leg on fill_wait. borrowed. */
    UringApiCompletion *active;
    UringApiCompletionFifo fifo;
    struct UringApiFdSlot *hash_next;
    struct UringApiFdSlot *drain_next;
    int on_drain_list;
};

/* ring_flush_pending bucket. 0 is front so a zeroed ring needs no init.
 * The outermost ring critical section pushes waiter or next and pops
 * before unlock. Nested flushes keep that bucket. */
enum {
    URING_API_SUBMIT_FRONT = 0,
    URING_API_SUBMIT_WAITER = 1,
    URING_API_SUBMIT_NEXT = 2,
    URING_API_SUBMIT_KIND_COUNT = 3
};

struct UringApiRing {
    PyObject_HEAD struct io_uring ring;
    PyObject *delivery_callback;
    PyObject *delivery_exception_handler;
    /* optional: hook(context) when a nowait CQE fails (res < 0) */
    PyObject *nowait_error_handler;
    UringApiCompletionCallback c_delivery_callback;
    void *c_delivery_callback_user_data;
#ifdef URING_API_USE_PYTHREAD_RING_LOCK
    PyThread_type_lock ring_lock;
#endif
    /* completion workers: one kernel waiter dumps CQEs; the rest pack one each. */
    pthread_mutex_t cqe_mu;
    pthread_cond_t cqe_cv;
    UringApiCqeFifo cqe_queue;
    int cqe_waiting;
    UringApiMutex refcount_mutex;
    UringApiIdlePark idle;
    unsigned int delivery_active_workers;
    unsigned int receive_state;
    unsigned short next_buf_group;
    unsigned short *free_buf_group_ids;
    unsigned int free_buf_group_id_count;
    unsigned int free_buf_group_id_capacity;
    unsigned int setup_flags;
    /* 0 = unset (closed). SINGLE_ISSUER / DEFER_TASKRUN: creating thread. */
    unsigned long long owner_thread_id;
    bool delivery_stop_requested;
    bool initialized;
    /* when true (default), get_sqe flushes if the SQ is full, and wait()
     * flushes before parking. when false, a full SQ raises SubmissionQueueFull
     * and wait() does not submit. */
    bool auto_submit;
    /* when true (default), the unique CQ waiter io_uring_submit before harvest.
     * TAKE workers never submit. false: only host wait()/submit()/wait_idle. */
    bool worker_auto_submit;
    /* waitable Completions with an in-flight prepare ref (not construct-only;
     * ordinary nowait is excluded, nowait send_all is counted until terminal).
     * ++ at that INCREF, -- when the ref is dropped. */
    unsigned int pending_count;
    /* per-fd send-all busy slots; drain_head is slots with conflict-FIFO work. */
    UringApiFdSlot **fd_slots;
    size_t fd_slot_cap;
    size_t fd_slot_count;
    UringApiFdSlot *fd_drain_head;
    /* Completions waiting for an SQE without enter (SQ full, this thread must
     * not io_uring_enter). Includes send-all next-legs (the active handle). */
    UringApiCompletionFifo fill_wait;
    /* monotonic counters. no reset. sqe / sq_full / next_leg / next_leg_park /
     * submit_* are incremented under the ring critical section. cqe and the
     * wait_* harvest counters are written only by the unique waiter; relaxed
     * atomics let stats() load them without that waiter holding the ring lock. */
    uint64_t stat_sqe;
    uint64_t stat_sq_full;
    uint64_t stat_next_leg;
    /* send-all continuation parked on fill-wait (no slot, this thread will not enter). */
    uint64_t stat_next_leg_park;
    uint64_t stat_submit_events[URING_API_SUBMIT_KIND_COUNT];
    uint64_t stat_submit_sqes[URING_API_SUBMIT_KIND_COUNT];
    _Atomic uint64_t stat_cqe;
    /* one reap that returned a CQE, plus how many CQEs that drain consumed.
     * empty timeout / peek is not an event. unique waiter only. */
    _Atomic uint64_t stat_wait_front_events;
    _Atomic uint64_t stat_wait_front_cqes;
    _Atomic uint64_t stat_wait_back_events;
    _Atomic uint64_t stat_wait_back_cqes;
    unsigned char submit_kind;
};

/* Caller holds the ring critical section. */
static inline unsigned char ring_submit_kind_push(UringApiRing *self, unsigned char kind) {
    unsigned char saved = self->submit_kind;

    self->submit_kind = kind;
    return saved;
}

static inline void ring_submit_kind_pop(UringApiRing *self, unsigned char saved) { self->submit_kind = saved; }

static inline void ring_note_sqe(UringApiRing *self) { self->stat_sqe++; }

static inline void ring_note_sq_full(UringApiRing *self) { self->stat_sq_full++; }

static inline void ring_note_next_leg(UringApiRing *self) { self->stat_next_leg++; }

static inline void ring_note_next_leg_park(UringApiRing *self) { self->stat_next_leg_park++; }

static inline void ring_note_submit(UringApiRing *self, int submitted) {
    unsigned int kind;

    if (submitted <= 0) {
        return;
    }
    kind = self->submit_kind;
    assert(kind < URING_API_SUBMIT_KIND_COUNT);
    self->stat_submit_events[kind]++;
    self->stat_submit_sqes[kind] += (uint64_t)submitted;
}

/* Single writer: the unique CQ waiter. Relaxed load/store is defined for a
 * concurrent stats() read and cheaper than a locked fetch-add. */
static inline void ring_note_relaxed(_Atomic uint64_t *slot, uint64_t n) {
    uint64_t cur = atomic_load_explicit(slot, memory_order_relaxed);

    atomic_store_explicit(slot, cur + n, memory_order_relaxed);
}

static inline void ring_note_cqe(UringApiRing *self) { ring_note_relaxed(&self->stat_cqe, 1); }

/* n == 0 is an empty reap: not an event, so it does not dilute cqes/events. */
static inline void ring_note_wait_burst(_Atomic uint64_t *events, _Atomic uint64_t *cqes, uint64_t n) {
    if (n == 0) {
        return;
    }
    ring_note_relaxed(events, 1);
    ring_note_relaxed(cqes, n);
}

extern PyTypeObject UringApiRing_Type;
extern PyTypeObject UringApiCompletion_Type;

#define URING_API_C_MULTISHOT ((uint16_t)(1u << 0))
#define URING_API_C_AUX_DECREF ((uint16_t)(1u << 1))
#define URING_API_C_PREPARED ((uint16_t)(1u << 2))
#define URING_API_C_SKIP_SUCCESS ((uint16_t)(1u << 3))
#define URING_API_C_USER_DATA_CLEAR ((uint16_t)(1u << 4))
#define URING_API_C_SEND_ALL_CONT ((uint16_t)(1u << 5))
#define URING_API_C_SEND_ALL_ABANDON ((uint16_t)(1u << 6))
#define URING_API_C_CONFLICT_QUEUED ((uint16_t)(1u << 7))
#define URING_API_C_FILL_WAIT ((uint16_t)(1u << 8))
#define URING_API_C_SKIP_ALL ((uint16_t)(1u << 9))

static inline int completion_has_bit(const UringApiCompletion *c, uint16_t bit) {
    return (atomic_load_explicit(&c->bits, memory_order_acquire) & bit) != 0;
}

static inline void completion_set_bit(UringApiCompletion *c, uint16_t bit) {
    atomic_fetch_or_explicit(&c->bits, bit, memory_order_acq_rel);
}

static inline void completion_clear_bit(UringApiCompletion *c, uint16_t bit) {
    atomic_fetch_and_explicit(&c->bits, (uint_least16_t)~bit, memory_order_acq_rel);
}

static inline int completion_is_accepted(const UringApiCompletion *c) {
    return completion_has_bit(c, URING_API_C_PREPARED) || completion_has_bit(c, URING_API_C_CONFLICT_QUEUED) ||
           completion_has_bit(c, URING_API_C_FILL_WAIT);
}

#define URING_API_CAPI_FEATURES (URING_API_CAPI_FEATURE_CORE)

extern PyObject *UringApiSubmissionQueueFullError;

#endif

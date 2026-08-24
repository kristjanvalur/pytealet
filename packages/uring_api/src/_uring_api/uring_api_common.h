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
#include <pythread.h>
#include <stdatomic.h>
#include <stdbool.h>

#include "uring_api_completion_kinds.h"
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
typedef int (*UringApiCompletionCallback)(PyObject *ring, PyObject *completions, void *user_data);

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
    /* packed: MULTISHOT | AUX_DECREF | PREPARED | NOWAIT | USER_DATA_CLEAR |
     * SEND_ALL_CONT | SEND_ALL_ABANDON | CONFLICT_QUEUED. atomic: cancel sets
     * ABANDON under the ring CS while CQE drain may set AUX_DECREF under
     * refcount_mutex. */
    atomic_uint_least8_t bits;
    void *state;
} UringApiCompletion;

typedef struct UringApiStagedCQE {
    int res;
    unsigned int flags;
    UringApiCompletion *completion;
    unsigned long long leg_index;
} UringApiStagedCQE;

/* nowait failure recorded under the drain lock; Python handler runs after unlock */
typedef struct UringApiStagedNowaitError {
    int res;
    unsigned int flags;
    unsigned int kind;
    int has_fd;
    int fd;
} UringApiStagedNowaitError;

typedef struct UringApiStagingBuffer {
    UringApiStagedCQE *entries;
    size_t capacity;
    size_t count;
    UringApiStagedNowaitError *nowait_errors;
    size_t nowait_capacity;
    size_t nowait_count;
} UringApiStagingBuffer;

typedef struct UringApiConflictFifo {
    UringApiCompletion **items;
    size_t head;
    size_t count;
    size_t cap;
} UringApiConflictFifo;

struct UringApiFdSlot {
    int fd;
    /* send-all whose SQE is filled, in-kernel, or continuation_pending. borrowed. */
    UringApiCompletion *active;
    int continuation_pending;
    UringApiConflictFifo fifo;
    struct UringApiFdSlot *hash_next;
    struct UringApiFdSlot *drain_next;
    int on_drain_list;
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
    PyThread_type_lock cqe_drain_lock;
    UringApiMutex refcount_mutex;
    UringApiIdlePark idle;
    unsigned int delivery_active_workers;
    unsigned int receive_state;
    unsigned short next_buf_group;
    unsigned short *free_buf_group_ids;
    unsigned int free_buf_group_id_count;
    unsigned int free_buf_group_id_capacity;
    unsigned int setup_flags;
    unsigned long long owner_thread_id;
    bool delivery_stop_requested;
    bool initialized;
    /* when true (default), get_sqe flushes if the SQ is full, and wait/serve
     * flush before parking. when false, a full SQ raises SubmissionQueueFull
     * and wait/serve do not submit. */
    bool auto_submit;
    /* experimental: after filling a send_all next-leg SQE, io_uring_submit
     * immediately (when this thread may submit). default false: leave the SQE
     * in the SQ until wait/submit or SQ-full, like ordinary prepare. */
    bool experimental_send_all_submit_next;
    /* waitable Completions with an in-flight prepare ref (not construct-only;
     * ordinary nowait is excluded, nowait send_all is counted until terminal).
     * ++ at that INCREF, -- when the ref is dropped. */
    unsigned int pending_count;
    UringApiStagingBuffer wait_staging;
    /* per-fd send-all busy slots; drain_head is slots with continuation or FIFO work. */
    UringApiFdSlot **fd_slots;
    size_t fd_slot_cap;
    size_t fd_slot_count;
    UringApiFdSlot *fd_drain_head;
};

extern PyTypeObject UringApiRing_Type;
extern PyTypeObject UringApiCompletion_Type;

#define URING_API_C_MULTISHOT ((uint8_t)(1u << 0))
#define URING_API_C_AUX_DECREF ((uint8_t)(1u << 1))
#define URING_API_C_PREPARED ((uint8_t)(1u << 2))
#define URING_API_C_NOWAIT ((uint8_t)(1u << 3))
#define URING_API_C_USER_DATA_CLEAR ((uint8_t)(1u << 4))
#define URING_API_C_SEND_ALL_CONT ((uint8_t)(1u << 5))
#define URING_API_C_SEND_ALL_ABANDON ((uint8_t)(1u << 6))
#define URING_API_C_CONFLICT_QUEUED ((uint8_t)(1u << 7))

static inline int completion_has_bit(const UringApiCompletion *c, uint8_t bit) {
    return (atomic_load_explicit(&c->bits, memory_order_acquire) & bit) != 0;
}

static inline void completion_set_bit(UringApiCompletion *c, uint8_t bit) {
    atomic_fetch_or_explicit(&c->bits, bit, memory_order_acq_rel);
}

static inline void completion_clear_bit(UringApiCompletion *c, uint8_t bit) {
    atomic_fetch_and_explicit(&c->bits, (uint_least8_t)~bit, memory_order_acq_rel);
}

#define URING_API_CAPI_FEATURES (URING_API_CAPI_FEATURE_CORE)

extern PyObject *UringApiSubmissionQueueFullError;

#endif

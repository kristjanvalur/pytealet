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

#ifndef _PyCFunction_CAST
#define _PyCFunction_CAST(func) ((PyCFunction)(void (*)(void))(func))
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
} UringApiCompletionStateKind;

typedef struct UringApiCompletion {
    PyObject_HEAD UringApiPendingKind kind;
    PyObject *user_data;
    int res;
    unsigned int flags;
    PyObject *result;
    unsigned long long sequence;
    bool multishot;
    void *state;
} UringApiCompletion;

typedef struct UringApiStagedCQE {
    int res;
    unsigned int flags;
    UringApiCompletion *completion;
    unsigned long long leg_index;
} UringApiStagedCQE;

typedef struct UringApiStagingBuffer {
    UringApiStagedCQE *entries;
    size_t capacity;
    size_t count;
} UringApiStagingBuffer;

struct UringApiRing {
    PyObject_HEAD struct io_uring ring;
    PyObject *delivery_callback;
    UringApiCompletionCallback c_delivery_callback;
    void *c_delivery_callback_user_data;
#ifdef URING_API_USE_PYTHREAD_RING_LOCK
    PyThread_type_lock ring_lock;
#endif
    PyThread_type_lock cqe_drain_lock;
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
    UringApiStagingBuffer wait_staging;
};

extern PyTypeObject UringApiRing_Type;
extern PyTypeObject UringApiCompletion_Type;
extern PyObject *UringApiSubmissionQueueFullError;

#define URING_API_CAPI_FEATURES (URING_API_CAPI_FEATURE_CORE)

#endif
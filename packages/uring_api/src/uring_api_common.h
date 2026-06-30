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

#ifndef _PyCFunction_CAST
#define _PyCFunction_CAST(func) ((PyCFunction)(void (*)(void))(func))
#endif

struct UringApiRing {
    PyObject_HEAD struct io_uring ring;
    PyObject *delivery_callback;
    UringApiCompletionCallback c_delivery_callback;
    void *c_delivery_callback_user_data;
#ifdef URING_API_USE_PYTHREAD_RING_LOCK
    PyThread_type_lock ring_lock;
#endif
    UringApiMutex receive_mutex;
    PyThread_type_lock delivery_wait_lock;
    unsigned int delivery_active_workers;
    unsigned int receive_state;
    unsigned short next_buf_group;
    bool delivery_stop_requested;
    bool initialized;
};

typedef struct {
    UringApiRing *ring;
    struct io_uring_buf_ring *ring_buffer;
    unsigned char *storage;
    unsigned int buffer_size;
    unsigned int buffer_count;
    unsigned short group_id;
    int mask;
} UringApiRecvBufferPool;

typedef enum {
    URING_API_RECEIVE_IDLE = 0,
    URING_API_RECEIVE_WAITING = 1,
    URING_API_RECEIVE_DELIVERING = 2,
} UringApiReceiveState;

typedef enum {
    URING_API_PENDING_RECV = 1,
    URING_API_PENDING_SEND = 2,
    URING_API_PENDING_WAKE = 3,
    URING_API_PENDING_SENDTO = 4,
    URING_API_PENDING_RECVMSG = 5,
    URING_API_PENDING_ACCEPT = 6,
    URING_API_PENDING_CONNECT = 7,
    URING_API_PENDING_CANCEL = 8,
    URING_API_PENDING_SHUTDOWN = 9,
    URING_API_PENDING_CLOSE = 10,
    URING_API_PENDING_SENDMSG = 11,
    URING_API_PENDING_SOCKET = 12,
    URING_API_PENDING_RECV_MULTISHOT = 13,
    URING_API_PENDING_SEND_ZC = 14,
    URING_API_PENDING_SENDMSG_ZC = 15,
} UringApiPendingKind;

typedef struct {
    PyObject_HEAD UringApiPendingKind kind;
    PyObject *user_data;
    int res;
    unsigned int flags;
    PyObject *result;
    PyObject *buffer;
    UringApiRecvBufferPool *recv_pool;
    unsigned long long sequence;
    Py_buffer view;
    struct iovec iov;
    struct msghdr msg;
    struct sockaddr_storage addr;
    socklen_t addrlen;
    bool has_view;
    bool has_msghdr;
} UringApiCompletion;

extern PyTypeObject UringApiRing_Type;
extern PyTypeObject UringApiCompletion_Type;
extern PyObject *UringApiSubmissionQueueFullError;

#define URING_API_CAPI_FEATURES (URING_API_CAPI_FEATURE_CORE)

#endif
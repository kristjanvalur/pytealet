#define PY_SSIZE_T_CLEAN

/*
 * Module assembly for the _uring_api extension.
 *
 * The extension is intentionally kept as one translation unit so private
 * helpers can remain file-local, while the implementation itself is split into
 * focused included files for navigation: core helpers, probes, ring lifecycle,
 * submissions, dispatch, properties, and the public C API capsule.
 */

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

#include "uring_api_capi.h"

#if !defined(IO_URING_VERSION_MAJOR) || !defined(IO_URING_VERSION_MINOR)
#error "uring-api requires liburing >= 2.4 development headers"
#elif IO_URING_VERSION_MAJOR < 2 || (IO_URING_VERSION_MAJOR == 2 && IO_URING_VERSION_MINOR < 4)
#error "uring-api requires liburing >= 2.4 development headers"
#endif

#ifndef Py_BEGIN_CRITICAL_SECTION
#define Py_BEGIN_CRITICAL_SECTION(op) {
#define Py_END_CRITICAL_SECTION() }
#endif

#ifndef Py_BEGIN_CRITICAL_SECTION_MUTEX
typedef char UringApiMutex;
#define Py_BEGIN_CRITICAL_SECTION_MUTEX(mutex) {
#else
typedef PyMutex UringApiMutex;
#endif

#ifndef Py_END_CRITICAL_SECTION_MUTEX
#define Py_END_CRITICAL_SECTION_MUTEX() Py_END_CRITICAL_SECTION()
#endif

#ifndef _PyCFunction_CAST
#define _PyCFunction_CAST(func) ((PyCFunction)(void (*)(void))(func))
#endif

typedef struct {
    PyObject_HEAD struct io_uring ring;
    PyObject *delivery_callback;
    UringApi_CCompletionCallback c_delivery_callback;
    void *c_delivery_callback_user_data;
    UringApiMutex receive_mutex;
    PyThread_type_lock delivery_wait_lock;
    unsigned int delivery_active_workers;
    unsigned int receive_state;
    unsigned short next_buf_group;
    bool delivery_stop_requested;
    bool initialized;
} UringApiRing;

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

static PyTypeObject UringApiRing_Type;
static PyTypeObject UringApiCompletion_Type;
static PyObject *UringApiSubmissionQueueFullError;

static PyObject *UringApiRing_break_wait(UringApiRing *self, PyObject *ignored);
static int UringApiRing_stop_delivery(UringApiRing *self);
static bool delivery_should_stop(UringApiRing *self);
static PyObject *build_capability_dict(void);

static PyObject *UringApiCapi_RingNew(unsigned int entries, unsigned int flags);
static int UringApiCapi_RingCheck(PyObject *ring);
static int UringApiCapi_RingClose(PyObject *ring);
static int UringApiCapi_RingFd(PyObject *ring);
static unsigned int UringApiCapi_RingFeatures(PyObject *ring);
static unsigned int UringApiCapi_RingSqEntries(PyObject *ring);
static unsigned int UringApiCapi_RingCqEntries(PyObject *ring);
static int UringApiCapi_RingClosed(PyObject *ring);
static int UringApiCapi_RingRunning(PyObject *ring);
static int UringApiCapi_RingSubmitRecv(PyObject *ring, int fd, PyObject *buf, PyObject *user_data);
static int UringApiCapi_RingSubmitRecvMultishot(PyObject *ring, int fd, unsigned int buffer_size,
                                                unsigned int buffer_count, unsigned int flags, PyObject *user_data);
static int UringApiCapi_RingSubmitSend(PyObject *ring, int fd, PyObject *data, unsigned int flags, PyObject *user_data);
static int UringApiCapi_RingSubmitSendZc(PyObject *ring, int fd, PyObject *data, unsigned int flags,
                                         unsigned int zc_flags, PyObject *user_data);
static int UringApiCapi_RingSubmitRecvmsg(PyObject *ring, int fd, PyObject *buf, PyObject *user_data);
static int UringApiCapi_RingSubmitSendto(PyObject *ring, int fd, PyObject *data, PyObject *address, unsigned int flags,
                                         PyObject *user_data);
static int UringApiCapi_RingSubmitSendmsg(PyObject *ring, int fd, PyObject *data, PyObject *address, unsigned int flags,
                                          PyObject *user_data);
static int UringApiCapi_RingSubmitSendmsgZc(PyObject *ring, int fd, PyObject *data, PyObject *address,
                                            unsigned int flags, PyObject *user_data);
static int UringApiCapi_RingSubmitAccept(PyObject *ring, int fd, unsigned int flags, PyObject *user_data);
static int UringApiCapi_RingSubmitAcceptMultishot(PyObject *ring, int fd, unsigned int flags, PyObject *user_data);
static int UringApiCapi_RingSubmitConnect(PyObject *ring, int fd, PyObject *address, PyObject *user_data);
static int UringApiCapi_RingSubmitShutdown(PyObject *ring, int fd, int how, PyObject *user_data);
static int UringApiCapi_RingSubmitClose(PyObject *ring, int fd, PyObject *user_data);
static int UringApiCapi_RingSubmitSocket(PyObject *ring, int domain, int type, int protocol, unsigned int flags,
                                         PyObject *user_data);
static int UringApiCapi_RingBreakWait(PyObject *ring);
static PyObject *UringApiCapi_RingWait(PyObject *ring, double timeout);
static int UringApiCapi_RingSetCallback(PyObject *ring, PyObject *callback);
static int UringApiCapi_RingSetCCallback(PyObject *ring, UringApi_CCompletionCallback callback, void *user_data);
static int UringApiCapi_RingServeCompletions(PyObject *ring);
static int UringApiCapi_RingStopServing(PyObject *ring);
static int UringApiCapi_RingResetServing(PyObject *ring);
static int UringApiCapi_CompletionCheck(PyObject *completion);
static PyObject *UringApiCapi_CompletionUserData(PyObject *completion);
static int UringApiCapi_CompletionRes(PyObject *completion, int *value);
static int UringApiCapi_CompletionFlags(PyObject *completion, unsigned int *value);
static int UringApiCapi_CompletionSequence(PyObject *completion, unsigned long long *value);
static PyObject *UringApiCapi_CompletionResult(PyObject *completion);

#define URING_API_CAPI_FEATURES (URING_API_CAPI_FEATURE_CORE)

#include "_uring_api_core.c"

#include "_uring_api_probe.c"

#include "_uring_api_ring.c"

#include "_uring_api_submit.c"

#include "_uring_api_dispatch.c"

#include "_uring_api_properties.c"

#include "_uring_api_capi.c"

static PyMethodDef UringApiRing_methods[] = {
    {"close", (PyCFunction)UringApiRing_close, METH_NOARGS, "Close the io_uring instance."},
    {"serve_completions", (PyCFunction)UringApiRing_serve_completions, METH_NOARGS,
     "Serve completions until stop_serving is called."},
    {"stop_serving", (PyCFunction)UringApiRing_stop_serving, METH_NOARGS, "Ask completion workers to stop."},
    {"reset_serving", (PyCFunction)UringApiRing_reset_serving, METH_NOARGS, "Clear the completion service stop flag."},
    {"submit_recv", _PyCFunction_CAST(UringApiRing_submit_recv), METH_VARARGS | METH_KEYWORDS,
     "Submit a recv operation."},
    {"submit_recv_multishot", _PyCFunction_CAST(UringApiRing_submit_recv_multishot), METH_VARARGS | METH_KEYWORDS,
     "Submit a multishot recv operation."},
    {"submit_send", _PyCFunction_CAST(UringApiRing_submit_send), METH_VARARGS | METH_KEYWORDS,
     "Submit a send operation."},
    {"submit_send_zc", _PyCFunction_CAST(UringApiRing_submit_send_zc), METH_VARARGS | METH_KEYWORDS,
     "Submit a zero-copy send operation."},
    {"submit_recvmsg", _PyCFunction_CAST(UringApiRing_submit_recvmsg), METH_VARARGS | METH_KEYWORDS,
     "Submit a recvmsg operation."},
    {"submit_sendto", _PyCFunction_CAST(UringApiRing_submit_sendto), METH_VARARGS | METH_KEYWORDS,
     "Submit a sendto operation."},
    {"submit_sendmsg", _PyCFunction_CAST(UringApiRing_submit_sendmsg), METH_VARARGS | METH_KEYWORDS,
     "Submit a sendmsg operation."},
    {"submit_sendmsg_zc", _PyCFunction_CAST(UringApiRing_submit_sendmsg_zc), METH_VARARGS | METH_KEYWORDS,
     "Submit a zero-copy sendmsg operation."},
    {"submit_accept", _PyCFunction_CAST(UringApiRing_submit_accept), METH_VARARGS | METH_KEYWORDS,
     "Submit an accept operation."},
    {"submit_accept_multishot", _PyCFunction_CAST(UringApiRing_submit_accept_multishot), METH_VARARGS | METH_KEYWORDS,
     "Submit a multishot accept operation."},
    {"submit_connect", _PyCFunction_CAST(UringApiRing_submit_connect), METH_VARARGS | METH_KEYWORDS,
     "Submit a connect operation."},
    {"submit_cancel", _PyCFunction_CAST(UringApiRing_submit_cancel), METH_VARARGS | METH_KEYWORDS,
     "Submit an async cancel operation targeting a pending completion."},
    {"submit_shutdown", _PyCFunction_CAST(UringApiRing_submit_shutdown), METH_VARARGS | METH_KEYWORDS,
     "Submit a socket shutdown operation."},
    {"submit_close", _PyCFunction_CAST(UringApiRing_submit_close), METH_VARARGS | METH_KEYWORDS,
     "Submit a close operation for a caller-owned fd."},
    {"submit_socket", _PyCFunction_CAST(UringApiRing_submit_socket), METH_VARARGS | METH_KEYWORDS,
     "Submit a socket creation operation."},
    {"break_wait", (PyCFunction)UringApiRing_break_wait, METH_NOARGS,
     "Interrupt a thread blocked in wait without producing a user completion."},
    {"wait", _PyCFunction_CAST(UringApiRing_wait), METH_VARARGS | METH_KEYWORDS,
     "Wait for one completion and return its result."},
    {"__enter__", (PyCFunction)UringApiRing_enter, METH_NOARGS, NULL},
    {"__exit__", (PyCFunction)UringApiRing_exit, METH_VARARGS, NULL},
    {NULL, NULL, 0, NULL}};

static PyGetSetDef UringApiRing_getset[] = {
    {"fd", (getter)UringApiRing_get_fd, NULL, NULL, NULL},
    {"features", (getter)UringApiRing_get_features, NULL, NULL, NULL},
    {"sq_entries", (getter)UringApiRing_get_sq_entries, NULL, NULL, NULL},
    {"cq_entries", (getter)UringApiRing_get_cq_entries, NULL, NULL, NULL},
    {"closed", (getter)UringApiRing_get_closed, NULL, NULL, NULL},
    {"running", (getter)UringApiRing_get_running, NULL, NULL, NULL},
    {"callback", (getter)UringApiRing_get_callback, (setter)UringApiRing_set_callback, NULL, NULL},
    {NULL, NULL, NULL, NULL, NULL}};

static PyTypeObject UringApiRing_Type = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "_uring_api.Ring",
    .tp_basicsize = sizeof(UringApiRing),
    .tp_dealloc = (destructor)UringApiRing_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "io_uring ring",
    .tp_methods = UringApiRing_methods,
    .tp_getset = UringApiRing_getset,
    .tp_init = (initproc)UringApiRing_init,
    .tp_new = PyType_GenericNew,
};

static PyGetSetDef UringApiCompletion_getset[] = {
    {"user_data", (getter)UringApiCompletion_get_user_data, NULL, NULL, NULL},
    {"kind", (getter)UringApiCompletion_get_kind, NULL, NULL, NULL},
    {"res", (getter)UringApiCompletion_get_res, NULL, NULL, NULL},
    {"flags", (getter)UringApiCompletion_get_flags, NULL, NULL, NULL},
    {"result", (getter)UringApiCompletion_get_result, NULL, NULL, NULL},
    {"sequence", (getter)UringApiCompletion_get_sequence, NULL, NULL, NULL},
    {NULL, NULL, NULL, NULL, NULL},
};

static PyTypeObject UringApiCompletion_Type = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "_uring_api.Completion",
    .tp_basicsize = sizeof(UringApiCompletion),
    .tp_dealloc = (destructor)UringApiCompletion_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "io_uring completion result",
    .tp_getset = UringApiCompletion_getset,
};

static PyMethodDef uring_api_methods[] = {
    {"probe", _PyCFunction_CAST(uring_api_probe), METH_VARARGS | METH_KEYWORDS,
     "Probe whether a minimal io_uring instance can be created."},
    {NULL, NULL, 0, NULL},
};

static int uring_api_exec(PyObject *module) {
    PyObject *legacy_version = NULL;
    PyObject *version = NULL;
    PyObject *version_info = NULL;

    if (PyType_Ready(&UringApiCompletion_Type) < 0) {
        return -1;
    }
    if (PyType_Ready(&UringApiRing_Type) < 0) {
        return -1;
    }
    UringApiSubmissionQueueFullError = PyErr_NewException("_uring_api.SubmissionQueueFull", PyExc_RuntimeError, NULL);
    if (!UringApiSubmissionQueueFullError) {
        return -1;
    }
    if (PyModule_AddObjectRef(module, "SubmissionQueueFull", UringApiSubmissionQueueFullError) < 0) {
        return -1;
    }
    Py_INCREF(&UringApiCompletion_Type);
    if (PyModule_AddObject(module, "Completion", (PyObject *)&UringApiCompletion_Type) < 0) {
        Py_DECREF(&UringApiCompletion_Type);
        return -1;
    }
    Py_INCREF(&UringApiRing_Type);
    if (PyModule_AddObject(module, "Ring", (PyObject *)&UringApiRing_Type) < 0) {
        Py_DECREF(&UringApiRing_Type);
        return -1;
    }
    if (module_add_setup_flag_constants(module) < 0 || module_add_cqe_flag_constants(module) < 0 ||
        module_add_recvsend_flag_constants(module) < 0 || module_add_completion_kind_constants(module) < 0) {
        return -1;
    }

    legacy_version = liburing_version_string();
    if (!legacy_version) {
        return -1;
    }
    if (PyModule_AddObject(module, "__liburing_version__", legacy_version) < 0) {
        Py_DECREF(legacy_version);
        return -1;
    }

    version = liburing_version_string();
    if (!version) {
        return -1;
    }
    if (PyModule_AddObject(module, "__compiled_liburing_version__", version) < 0) {
        Py_DECREF(version);
        return -1;
    }

    version_info = liburing_version_info();
    if (!version_info) {
        return -1;
    }
    if (PyModule_AddObject(module, "__compiled_liburing_version_info__", version_info) < 0) {
        Py_DECREF(version_info);
        return -1;
    }

    if (uring_api_export_capi(module) < 0) {
        return -1;
    }

    return 0;
}

/* CPython API uses void* in module slots; this conversion is intentional. */
#if defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#endif
static PyModuleDef_Slot uring_api_slots[] = {{Py_mod_exec, uring_api_exec},
#if defined(Py_mod_gil)
                                             {Py_mod_gil, Py_MOD_GIL_NOT_USED},
#endif
                                             {0, NULL}};
#if defined(__GNUC__)
#pragma GCC diagnostic pop
#endif

static struct PyModuleDef uring_api_module = {PyModuleDef_HEAD_INIT,
                                              "_uring_api",
                                              "Small wrapper around Linux io_uring.",
                                              0,
                                              uring_api_methods,
                                              uring_api_slots,
                                              NULL,
                                              NULL,
                                              NULL};

PyMODINIT_FUNC PyInit__uring_api(void) { return PyModuleDef_Init(&uring_api_module); }
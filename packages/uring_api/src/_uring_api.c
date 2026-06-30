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

typedef struct UringApiRing UringApiRing;

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
    UringApi_CCompletionCallback c_delivery_callback;
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
    PyObject_HEAD UringApiRing *ring;
    struct io_uring_buf_ring *ring_buffer;
    unsigned char *storage;
    unsigned int buffer_size;
    unsigned int buffer_count;
    unsigned short group_id;
    int mask;
} UringApiBufGroup;

typedef struct {
    PyObject_HEAD PyObject *buf_group;
    unsigned int buffer_id;
    unsigned int length;
    unsigned int export_count;
    bool recycled;
} UringApiBufView;

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
    URING_API_PENDING_RECV_MULTISHOT_ZC = 16,
    URING_API_PENDING_RECV_ZC = 17,
} UringApiPendingKind;

typedef struct {
    PyObject_HEAD UringApiPendingKind kind;
    PyObject *user_data;
    int res;
    unsigned int flags;
    PyObject *result;
    PyObject *buffer;
    PyObject *buf_group;
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
static PyTypeObject UringApiBufGroup_Type;
static PyTypeObject UringApiBufView_Type;
static PyTypeObject UringApiCompletion_Type;
static PyObject *UringApiSubmissionQueueFullError;

static PyObject *UringApiRing_new(PyTypeObject *type, PyObject *args, PyObject *kwargs);
static PyObject *UringApiRing_break_wait(UringApiRing *self, PyObject *ignored);
static int UringApiRing_stop_delivery(UringApiRing *self);
static int UringApiRing_traverse(UringApiRing *self, visitproc visit, void *arg);
static int UringApiRing_clear(UringApiRing *self);
static bool delivery_should_stop(UringApiRing *self);
static PyObject *build_capability_dict(void);
static int UringApiCompletion_traverse(UringApiCompletion *self, visitproc visit, void *arg);
static int UringApiCompletion_clear(UringApiCompletion *self);

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

static PyObject *UringApiBufGroup_create(UringApiRing *ring, unsigned int buffer_size, unsigned int buffer_count);
static void UringApiBufGroup_recycle(UringApiBufGroup *self, unsigned int buffer_id);
static PyObject *UringApiBufView_create(PyObject *buf_group_obj, unsigned int buffer_id, unsigned int length);

#include "_uring_api_core.c"

#include "_uring_api_probe.c"

#include "_uring_api_ring.c"

#include "_uring_api_bufgroup.c"

#include "_uring_api_bufview.c"

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
    {"create_buf_group", _PyCFunction_CAST(UringApiRing_create_buf_group), METH_VARARGS | METH_KEYWORDS,
     "Create a provided-buffer group for multishot receive operations."},
    {"create_buf_view", _PyCFunction_CAST(UringApiRing_create_buf_view), METH_VARARGS | METH_KEYWORDS,
     "Create a read-only leased view into a buffer group slot."},
    {"submit_recv", _PyCFunction_CAST(UringApiRing_submit_recv), METH_VARARGS | METH_KEYWORDS,
     "Submit a recv operation."},
    {"submit_recv_zc", _PyCFunction_CAST(UringApiRing_submit_recv_zc), METH_VARARGS | METH_KEYWORDS,
     "Submit a zero-copy recv operation using a provided-buffer group."},
    {"submit_recv_multishot", _PyCFunction_CAST(UringApiRing_submit_recv_multishot), METH_VARARGS | METH_KEYWORDS,
     "Submit a multishot recv operation."},
    {"submit_recv_multishot_zc", _PyCFunction_CAST(UringApiRing_submit_recv_multishot_zc), METH_VARARGS | METH_KEYWORDS,
     "Submit a zero-copy multishot recv operation."},
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
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_traverse = (traverseproc)UringApiRing_traverse,
    .tp_clear = (inquiry)UringApiRing_clear,
    .tp_doc = "io_uring ring",
    .tp_methods = UringApiRing_methods,
    .tp_getset = UringApiRing_getset,
    .tp_init = (initproc)UringApiRing_init,
    .tp_new = UringApiRing_new,
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

static PyGetSetDef UringApiBufGroup_getset[] = {
    {"buffer_size", (getter)UringApiBufGroup_get_buffer_size, NULL, NULL, NULL},
    {"buffer_count", (getter)UringApiBufGroup_get_buffer_count, NULL, NULL, NULL},
    {"group_id", (getter)UringApiBufGroup_get_group_id, NULL, NULL, NULL},
    {"ring", (getter)UringApiBufGroup_get_ring, NULL, NULL, NULL},
    {NULL, NULL, NULL, NULL, NULL},
};

static PyTypeObject UringApiBufGroup_Type = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "_uring_api.BufGroup",
    .tp_basicsize = sizeof(UringApiBufGroup),
    .tp_dealloc = (destructor)UringApiBufGroup_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_traverse = (traverseproc)UringApiBufGroup_traverse,
    .tp_clear = (inquiry)UringApiBufGroup_clear,
    .tp_doc = "io_uring provided-buffer group",
    .tp_getset = UringApiBufGroup_getset,
};

static PyMethodDef UringApiBufView_methods[] = {
    {"close", (PyCFunction)UringApiBufView_close, METH_NOARGS, "Release the leased buffer back to its group."},
    {NULL, NULL, 0, NULL},
};

static PyGetSetDef UringApiBufView_getset[] = {
    {"length", (getter)UringApiBufView_get_length, NULL, NULL, NULL},
    {"buffer_id", (getter)UringApiBufView_get_buffer_id, NULL, NULL, NULL},
    {"buf_group", (getter)UringApiBufView_get_buf_group, NULL, NULL, NULL},
    {"recycled", (getter)UringApiBufView_get_recycled, NULL, NULL, NULL},
    {NULL, NULL, NULL, NULL, NULL},
};

static PyBufferProcs UringApiBufView_bufferprocs = {
    .bf_getbuffer = UringApiBufView_getbuffer,
    .bf_releasebuffer = UringApiBufView_releasebuffer,
};

static PyTypeObject UringApiBufView_Type = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "_uring_api.BufView",
    .tp_basicsize = sizeof(UringApiBufView),
    .tp_dealloc = (destructor)UringApiBufView_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_traverse = (traverseproc)UringApiBufView_traverse,
    .tp_clear = (inquiry)UringApiBufView_clear,
    .tp_doc = "Read-only leased view into a provided-buffer group slot",
    .tp_methods = UringApiBufView_methods,
    .tp_getset = UringApiBufView_getset,
    .tp_as_buffer = &UringApiBufView_bufferprocs,
    .tp_new = UringApiBufView_new,
};

static PyTypeObject UringApiCompletion_Type = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "_uring_api.Completion",
    .tp_basicsize = sizeof(UringApiCompletion),
    .tp_dealloc = (destructor)UringApiCompletion_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_traverse = (traverseproc)UringApiCompletion_traverse,
    .tp_clear = (inquiry)UringApiCompletion_clear,
    .tp_doc = "io_uring completion result",
    .tp_getset = UringApiCompletion_getset,
};

static PyMethodDef uring_api_methods[] = {
    {"probe", _PyCFunction_CAST(uring_api_probe), METH_VARARGS | METH_KEYWORDS,
     "Probe whether a minimal io_uring instance can be created."},
    {NULL, NULL, 0, NULL},
};

static int uring_api_exec(PyObject *module) {
    PyObject *version = NULL;
    PyObject *version_info = NULL;

    if (PyType_Ready(&UringApiCompletion_Type) < 0) {
        return -1;
    }
    if (PyType_Ready(&UringApiBufGroup_Type) < 0) {
        return -1;
    }
    if (PyType_Ready(&UringApiBufView_Type) < 0) {
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
    Py_INCREF(&UringApiBufGroup_Type);
    if (PyModule_AddObject(module, "BufGroup", (PyObject *)&UringApiBufGroup_Type) < 0) {
        Py_DECREF(&UringApiBufGroup_Type);
        return -1;
    }
    Py_INCREF(&UringApiBufView_Type);
    if (PyModule_AddObject(module, "BufView", (PyObject *)&UringApiBufView_Type) < 0) {
        Py_DECREF(&UringApiBufView_Type);
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

    version = liburing_version_string();
    if (!version) {
        return -1;
    }
    if (PyModule_AddObjectRef(module, "__liburing_version__", version) < 0 ||
        PyModule_AddObjectRef(module, "__compiled_liburing_version__", version) < 0) {
        Py_DECREF(version);
        return -1;
    }
    Py_DECREF(version);

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
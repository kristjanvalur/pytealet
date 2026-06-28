#define PY_SSIZE_T_CLEAN

#include <Python.h>
#include <errno.h>
#include <liburing.h>
#include <limits.h>
#include <pythread.h>
#include <stdbool.h>
#include <string.h>

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

#ifndef _PyCFunction_CAST
#define _PyCFunction_CAST(func) ((PyCFunction)(void (*)(void))(func))
#endif

typedef struct {
    PyObject_HEAD
    struct io_uring ring;
    PyObject *pending;
    PyObject *delivery_callback;
    UringApi_CCompletionCallback c_delivery_callback;
    void *c_delivery_callback_user_data;
    UringApiMutex receive_mutex;
    PyThread_type_lock delivery_done_lock;
    unsigned long long next_wakeup_data;
    unsigned long delivery_thread_id;
    unsigned int receive_state;
    bool delivery_stop_requested;
    bool initialized;
} UringApiRing;

typedef enum {
    URING_API_RECEIVE_IDLE = 0,
    URING_API_RECEIVE_WAITING = 1,
    URING_API_RECEIVE_DELIVERING = 2,
} UringApiReceiveState;

typedef enum {
    URING_API_PENDING_RECV = 1,
    URING_API_PENDING_SEND = 2,
    URING_API_PENDING_WAKE = 3,
} UringApiPendingKind;

typedef struct {
    PyObject_HEAD
    UringApiPendingKind kind;
    PyObject *buffer;
    Py_buffer view;
    bool has_view;
} UringApiPending;

static PyTypeObject UringApiRing_Type;
static PyTypeObject UringApiPending_Type;

static PyObject *UringApiRing_break_wait(UringApiRing *self, PyObject *ignored);
static int UringApiRing_stop_delivery(UringApiRing *self);

static PyObject *UringApiCapi_RingNew(unsigned int entries, unsigned int flags);
static int UringApiCapi_RingCheck(PyObject *ring);
static int UringApiCapi_RingClose(PyObject *ring);
static int UringApiCapi_RingFd(PyObject *ring);
static unsigned int UringApiCapi_RingFeatures(PyObject *ring);
static unsigned int UringApiCapi_RingSqEntries(PyObject *ring);
static unsigned int UringApiCapi_RingCqEntries(PyObject *ring);
static int UringApiCapi_RingClosed(PyObject *ring);
static int UringApiCapi_RingRunning(PyObject *ring);
static int UringApiCapi_RingSubmitRecv(PyObject *ring, int fd, Py_ssize_t n, unsigned long long user_data);
static int UringApiCapi_RingSubmitSend(PyObject *ring, int fd, PyObject *data, unsigned long long user_data);
static int UringApiCapi_RingBreakWait(PyObject *ring);
static PyObject *UringApiCapi_RingWait(PyObject *ring, double timeout);
static int UringApiCapi_RingSetCallback(PyObject *ring, PyObject *callback);
static int UringApiCapi_RingSetCCallback(PyObject *ring, UringApi_CCompletionCallback callback, void *user_data);
static int UringApiCapi_RingStart(PyObject *ring);
static int UringApiCapi_RingStop(PyObject *ring);

#define URING_API_CAPI_FEATURES                                                                                      \
    (URING_API_CAPI_FEATURE_PROBE | URING_API_CAPI_FEATURE_RING | URING_API_CAPI_FEATURE_C_CALLBACK)

static int ring_type_check(PyObject *ring) {
    if (!PyObject_TypeCheck(ring, &UringApiRing_Type)) {
        PyErr_SetString(PyExc_TypeError, "ring must be an _uring_api.Ring instance");
        return 0;
    }
    return 1;
}

static int normalize_ret_errno(int ret) {
    if (ret < 0) {
        return -ret;
    }
    if (errno) {
        return errno;
    }
    return EINVAL;
}

static PyObject *liburing_version_string(void) {
    return PyUnicode_FromFormat("%d.%d", IO_URING_VERSION_MAJOR, IO_URING_VERSION_MINOR);
}

static PyObject *liburing_version_info(void) {
    return Py_BuildValue("(ii)", IO_URING_VERSION_MAJOR, IO_URING_VERSION_MINOR);
}

static int module_add_uint64_constant(PyObject *module, const char *name, unsigned long long value) {
    PyObject *value_obj = PyLong_FromUnsignedLongLong(value);
    if (!value_obj) {
        return -1;
    }
    if (PyModule_AddObject(module, name, value_obj) < 0) {
        Py_DECREF(value_obj);
        return -1;
    }
    return 0;
}

static void sqe_set_data(UringApiRing *self, struct io_uring_sqe *sqe, unsigned long long user_data) {
    (void)self;
    io_uring_sqe_set_data64(sqe, user_data);
}

static unsigned long long cqe_get_data(UringApiRing *self, struct io_uring_cqe *cqe) {
    (void)self;
    return io_uring_cqe_get_data64(cqe);
}

static unsigned int ring_sq_entries(UringApiRing *self) {
    return self->ring.sq.ring_entries;
}

static unsigned int ring_cq_entries(UringApiRing *self) {
    return self->ring.cq.ring_entries;
}

static int dict_set_owned(PyObject *dict, const char *key, PyObject *value) {
    int ret;
    if (!value) {
        return -1;
    }
    ret = PyDict_SetItemString(dict, key, value);
    Py_DECREF(value);
    return ret;
}

static int parse_entries_flags(PyObject *args, PyObject *kwargs, unsigned int default_entries, unsigned int *entries,
                               unsigned int *flags) {
    static char *keywords[] = {"entries", "flags", NULL};
    unsigned long entries_value = default_entries;
    unsigned long flags_value = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|kk", keywords, &entries_value, &flags_value)) {
        return -1;
    }
    if (entries_value == 0 || entries_value > UINT_MAX) {
        PyErr_SetString(PyExc_ValueError, "entries must be between 1 and UINT_MAX");
        return -1;
    }
    if (flags_value > UINT_MAX) {
        PyErr_SetString(PyExc_ValueError, "flags must fit in an unsigned int");
        return -1;
    }
    *entries = (unsigned int)entries_value;
    *flags = (unsigned int)flags_value;
    return 0;
}

static int ring_check_open(UringApiRing *self) {
    if (!self->initialized) {
        PyErr_SetString(PyExc_RuntimeError, "ring is closed");
        return -1;
    }
    return 0;
}

static void UringApiPending_dealloc(UringApiPending *self) {
    if (self->has_view) {
        PyBuffer_Release(&self->view);
        self->has_view = false;
    }
    Py_CLEAR(self->buffer);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *UringApiPending_new(UringApiPendingKind kind, PyObject *buffer) {
    UringApiPending *pending = PyObject_New(UringApiPending, &UringApiPending_Type);
    if (!pending) {
        return NULL;
    }
    pending->kind = kind;
    pending->buffer = Py_NewRef(buffer);
    pending->has_view = false;
    return (PyObject *)pending;
}

static PyObject *UringApiPending_new_view(UringApiPendingKind kind, Py_buffer *view) {
    UringApiPending *pending = PyObject_New(UringApiPending, &UringApiPending_Type);
    if (!pending) {
        return NULL;
    }
    pending->kind = kind;
    pending->buffer = NULL;
    pending->view = *view;
    pending->has_view = true;
    return (PyObject *)pending;
}

static int pending_store(UringApiRing *self, unsigned long long user_data, UringApiPendingKind kind, PyObject *buffer) {
    PyObject *key = NULL;
    PyObject *pending = NULL;
    int ret;

    key = PyLong_FromUnsignedLongLong(user_data);
    if (!key) {
        return -1;
    }
    pending = UringApiPending_new(kind, buffer);
    if (!pending) {
        Py_DECREF(key);
        return -1;
    }
    ret = PyDict_SetItem(self->pending, key, pending);
    Py_DECREF(pending);
    Py_DECREF(key);
    return ret;
}

static int pending_store_view(UringApiRing *self, unsigned long long user_data, UringApiPendingKind kind,
                              Py_buffer *view) {
    PyObject *key = NULL;
    PyObject *pending = NULL;
    int ret;

    key = PyLong_FromUnsignedLongLong(user_data);
    if (!key) {
        return -1;
    }
    pending = UringApiPending_new_view(kind, view);
    if (!pending) {
        Py_DECREF(key);
        return -1;
    }
    ret = PyDict_SetItem(self->pending, key, pending);
    if (ret < 0) {
        ((UringApiPending *)pending)->has_view = false;
    }
    Py_DECREF(pending);
    Py_DECREF(key);
    return ret;
}

static PyObject *pending_pop(UringApiRing *self, unsigned long long user_data) {
    PyObject *key = PyLong_FromUnsignedLongLong(user_data);
    PyObject *pending;

    if (!key) {
        return NULL;
    }
    pending = PyDict_GetItemWithError(self->pending, key);
    if (!pending) {
        Py_DECREF(key);
        if (!PyErr_Occurred()) {
            Py_RETURN_NONE;
        }
        return NULL;
    }
    Py_INCREF(pending);
    if (PyDict_DelItem(self->pending, key) < 0) {
        Py_DECREF(key);
        Py_DECREF(pending);
        return NULL;
    }
    Py_DECREF(key);
    return pending;
}

static int submit_one(UringApiRing *self) {
    int ret;

    errno = 0;
    ret = io_uring_submit(&self->ring);

    if (ret < 0) {
        int errnum = normalize_ret_errno(ret);
        errno = errnum;
        PyErr_SetFromErrno(PyExc_OSError);
        return -1;
    }
    if (ret == 0) {
        PyErr_SetString(PyExc_RuntimeError, "io_uring_submit submitted no operations");
        return -1;
    }
    return 0;
}

static void pending_discard(UringApiRing *self, unsigned long long user_data) {
    PyObject *ignored = pending_pop(self, user_data);
    Py_XDECREF(ignored);
}

static int receive_wait_begin(UringApiRing *self, bool from_delivery_thread) {
    int ret = 0;

    Py_BEGIN_CRITICAL_SECTION_MUTEX(&self->receive_mutex);
    if (from_delivery_thread) {
        if (self->receive_state != URING_API_RECEIVE_DELIVERING) {
            PyErr_SetString(PyExc_RuntimeError, "delivery thread is not active");
            ret = -1;
        }
    } else if (self->receive_state == URING_API_RECEIVE_DELIVERING) {
        PyErr_SetString(PyExc_RuntimeError, "delivery thread is active");
        ret = -1;
    } else if (self->receive_state != URING_API_RECEIVE_IDLE) {
        PyErr_SetString(PyExc_RuntimeError, "another wait is already active");
        ret = -1;
    } else {
        self->receive_state = URING_API_RECEIVE_WAITING;
    }
    Py_END_CRITICAL_SECTION();
    return ret;
}

static void receive_wait_end(UringApiRing *self, bool from_delivery_thread) {
    if (from_delivery_thread) {
        return;
    }

    Py_BEGIN_CRITICAL_SECTION_MUTEX(&self->receive_mutex);
    self->receive_state = URING_API_RECEIVE_IDLE;
    Py_END_CRITICAL_SECTION();
}

static bool delivery_is_running_locked(UringApiRing *self) {
    return self->receive_state == URING_API_RECEIVE_DELIVERING;
}

static void delivery_mark_exited(UringApiRing *self) {
    PyThread_type_lock lock = NULL;

    Py_BEGIN_CRITICAL_SECTION_MUTEX(&self->receive_mutex);
    if (self->receive_state == URING_API_RECEIVE_DELIVERING) {
        self->receive_state = URING_API_RECEIVE_IDLE;
    }
    self->delivery_stop_requested = false;
    self->delivery_thread_id = 0;
    lock = self->delivery_done_lock;
    Py_END_CRITICAL_SECTION();

    if (lock) {
        PyThread_release_lock(lock);
    }
}

static struct io_uring_sqe *get_sqe(UringApiRing *self) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&self->ring);
    if (sqe) {
        return sqe;
    }
    if (submit_one(self) < 0) {
        return NULL;
    }
    sqe = io_uring_get_sqe(&self->ring);
    if (!sqe) {
        PyErr_SetString(PyExc_RuntimeError, "no submission queue entries available");
        return NULL;
    }
    return sqe;
}

static PyObject *build_probe_result(bool available, int errnum, const char *message, struct io_uring_params *params) {
    PyObject *result = PyDict_New();
    if (!result) {
        return NULL;
    }

    if (PyDict_SetItemString(result, "available", available ? Py_True : Py_False) < 0 ||
        dict_set_owned(result, "errno", errnum ? PyLong_FromLong(errnum) : Py_NewRef(Py_None)) < 0 ||
        dict_set_owned(result, "message", message ? PyUnicode_FromString(message) : Py_NewRef(Py_None)) < 0 ||
        dict_set_owned(result, "features", PyLong_FromUnsignedLong(params ? params->features : 0)) < 0 ||
        dict_set_owned(result, "sq_entries", PyLong_FromUnsignedLong(params ? params->sq_entries : 0)) < 0 ||
        dict_set_owned(result, "cq_entries", PyLong_FromUnsignedLong(params ? params->cq_entries : 0)) < 0 ||
        dict_set_owned(result, "liburing_version", liburing_version_string()) < 0 ||
        dict_set_owned(result, "compiled_liburing_version", liburing_version_string()) < 0 ||
        dict_set_owned(result, "compiled_liburing_version_info", liburing_version_info()) < 0) {
        Py_DECREF(result);
        return NULL;
    }
    return result;
}

static PyObject *uring_api_probe_impl(unsigned int entries, unsigned int flags) {
    struct io_uring ring;
    struct io_uring_params params;
    int ret;

    if (entries == 0) {
        PyErr_SetString(PyExc_ValueError, "entries must be between 1 and UINT_MAX");
        return NULL;
    }

    memset(&ring, 0, sizeof(ring));
    memset(&params, 0, sizeof(params));
    params.flags = flags;

    errno = 0;
    Py_BEGIN_ALLOW_THREADS
    ret = io_uring_queue_init_params(entries, &ring, &params);
    Py_END_ALLOW_THREADS

    if (ret < 0) {
        int errnum = normalize_ret_errno(ret);
        return build_probe_result(false, errnum, strerror(errnum), &params);
    }

    io_uring_queue_exit(&ring);
    return build_probe_result(true, 0, NULL, &params);
}

static PyObject *uring_api_probe(PyObject *self, PyObject *args, PyObject *kwargs) {
    unsigned int entries;
    unsigned int flags;

    if (parse_entries_flags(args, kwargs, 2, &entries, &flags) < 0) {
        return NULL;
    }
    return uring_api_probe_impl(entries, flags);
}

static PyObject *UringApiCapi_Probe(unsigned int entries, unsigned int flags) {
    return uring_api_probe_impl(entries, flags);
}

static const UringApi_CAPI uring_api_capi_table = {
    URING_API_CAPI_ABI_VERSION,
    sizeof(UringApi_CAPI),
    URING_API_CAPI_FEATURES,
    IO_URING_VERSION_MAJOR,
    IO_URING_VERSION_MINOR,
    UringApiCapi_Probe,
    UringApiCapi_RingNew,
    UringApiCapi_RingCheck,
    UringApiCapi_RingClose,
    UringApiCapi_RingFd,
    UringApiCapi_RingFeatures,
    UringApiCapi_RingSqEntries,
    UringApiCapi_RingCqEntries,
    UringApiCapi_RingClosed,
    UringApiCapi_RingRunning,
    UringApiCapi_RingSubmitRecv,
    UringApiCapi_RingSubmitSend,
    UringApiCapi_RingBreakWait,
    UringApiCapi_RingWait,
    UringApiCapi_RingSetCallback,
    UringApiCapi_RingSetCCallback,
    UringApiCapi_RingStart,
    UringApiCapi_RingStop,
    {NULL},
};

static int uring_api_export_capi(PyObject *module) {
    PyObject *capsule;

    capsule = PyCapsule_New((void *)&uring_api_capi_table, URING_API_CAPI_CAPSULE_NAME, NULL);
    if (!capsule) {
        return -1;
    }
    if (PyModule_AddObject(module, "_C_API", capsule) < 0) {
        Py_DECREF(capsule);
        return -1;
    }
    if (PyModule_AddIntConstant(module, "C_API_ABI_VERSION", (long)URING_API_CAPI_ABI_VERSION) < 0) {
        return -1;
    }
    if (module_add_uint64_constant(module, "C_API_FEATURE_PROBE", URING_API_CAPI_FEATURE_PROBE) < 0) {
        return -1;
    }
    if (module_add_uint64_constant(module, "C_API_FEATURE_RING", URING_API_CAPI_FEATURE_RING) < 0) {
        return -1;
    }
    if (module_add_uint64_constant(module, "C_API_FEATURE_C_CALLBACK", URING_API_CAPI_FEATURE_C_CALLBACK) < 0) {
        return -1;
    }
    if (module_add_uint64_constant(module, "C_API_FEATURES", URING_API_CAPI_FEATURES) < 0) {
        return -1;
    }
    return 0;
}

static int UringApiRing_init(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    struct io_uring_params params;
    unsigned int entries;
    unsigned int flags;
    int ret;

    if (parse_entries_flags(args, kwargs, 8, &entries, &flags) < 0) {
        return -1;
    }

    if (UringApiRing_stop_delivery(self) < 0) {
        return -1;
    }

    if (self->initialized) {
        io_uring_queue_exit(&self->ring);
        self->initialized = false;
    }
    if (self->pending == NULL) {
        self->pending = PyDict_New();
        if (!self->pending) {
            return -1;
        }
    } else {
        PyDict_Clear(self->pending);
    }
    self->next_wakeup_data = ULLONG_MAX;
    self->receive_state = URING_API_RECEIVE_IDLE;
    self->delivery_stop_requested = false;
    self->delivery_thread_id = 0;

    memset(&self->ring, 0, sizeof(self->ring));
    memset(&params, 0, sizeof(params));
    params.flags = flags;

    errno = 0;
    Py_BEGIN_ALLOW_THREADS
    ret = io_uring_queue_init_params(entries, &self->ring, &params);
    Py_END_ALLOW_THREADS

    if (ret < 0) {
        int errnum = normalize_ret_errno(ret);
        errno = errnum;
        PyErr_SetFromErrno(PyExc_OSError);
        return -1;
    }

    self->initialized = true;
    return 0;
}

static void UringApiRing_dealloc(UringApiRing *self) {
    (void)UringApiRing_stop_delivery(self);
    if (self->initialized) {
        io_uring_queue_exit(&self->ring);
        self->initialized = false;
    }
    Py_CLEAR(self->pending);
    Py_CLEAR(self->delivery_callback);
    self->c_delivery_callback = NULL;
    self->c_delivery_callback_user_data = NULL;
    if (self->delivery_done_lock) {
        PyThread_free_lock(self->delivery_done_lock);
        self->delivery_done_lock = NULL;
    }
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *UringApiRing_close(UringApiRing *self, PyObject *Py_UNUSED(ignored)) {
    if (UringApiRing_stop_delivery(self) < 0) {
        return NULL;
    }
    if (self->initialized) {
        io_uring_queue_exit(&self->ring);
        self->initialized = false;
    }
    if (self->pending) {
        PyDict_Clear(self->pending);
    }
    self->receive_state = URING_API_RECEIVE_IDLE;
    self->delivery_stop_requested = false;
    self->delivery_thread_id = 0;
    Py_RETURN_NONE;
}

static PyObject *UringApiRing_enter(UringApiRing *self, PyObject *Py_UNUSED(ignored)) {
    Py_INCREF(self);
    return (PyObject *)self;
}

static PyObject *UringApiRing_exit(UringApiRing *self, PyObject *args) {
    if (UringApiRing_stop_delivery(self) < 0) {
        return NULL;
    }
    if (self->initialized) {
        io_uring_queue_exit(&self->ring);
        self->initialized = false;
    }
    if (self->pending) {
        PyDict_Clear(self->pending);
    }
    self->receive_state = URING_API_RECEIVE_IDLE;
    self->delivery_stop_requested = false;
    self->delivery_thread_id = 0;
    Py_RETURN_NONE;
}

static PyObject *UringApiRing_submit_recv(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "n", "user_data", NULL};
    struct io_uring_sqe *sqe;
    PyObject *buffer = NULL;
    long fd;
    Py_ssize_t n;
    unsigned long long user_data;
    int failed = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "lnK", keywords, &fd, &n, &user_data)) {
        return NULL;
    }
    if (fd < 0 || fd > INT_MAX) {
        PyErr_SetString(PyExc_ValueError, "fd must fit in a non-negative int");
        return NULL;
    }
    if (n < 0) {
        PyErr_SetString(PyExc_ValueError, "n must be non-negative");
        return NULL;
    }

    buffer = PyBytes_FromStringAndSize(NULL, n);
    if (!buffer) {
        return NULL;
    }

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else if (pending_store(self, user_data, URING_API_PENDING_RECV, buffer) < 0) {
        failed = 1;
    } else {
        sqe = get_sqe(self);
        if (!sqe) {
            pending_discard(self, user_data);
            failed = 1;
        } else {
            io_uring_prep_recv(sqe, (int)fd, PyBytes_AS_STRING(buffer), (size_t)n, 0);
            sqe_set_data(self, sqe, user_data);
            if (submit_one(self) < 0) {
                pending_discard(self, user_data);
                failed = 1;
            }
        }
    }
    Py_END_CRITICAL_SECTION();

    Py_DECREF(buffer);
    if (failed) {
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *UringApiRing_submit_send(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "data", "user_data", NULL};
    struct io_uring_sqe *sqe;
    Py_buffer view;
    long fd;
    unsigned long long user_data;
    int failed = 0;
    bool view_transferred = false;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "ly*K", keywords, &fd, &view, &user_data)) {
        return NULL;
    }
    if (fd < 0 || fd > INT_MAX) {
        PyBuffer_Release(&view);
        PyErr_SetString(PyExc_ValueError, "fd must fit in a non-negative int");
        return NULL;
    }

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else if (pending_store_view(self, user_data, URING_API_PENDING_SEND, &view) < 0) {
        failed = 1;
    } else {
        view_transferred = true;
        sqe = get_sqe(self);
        if (!sqe) {
            pending_discard(self, user_data);
            failed = 1;
        } else {
            io_uring_prep_send(sqe, (int)fd, view.buf, (size_t)view.len, 0);
            sqe_set_data(self, sqe, user_data);
            if (submit_one(self) < 0) {
                pending_discard(self, user_data);
                failed = 1;
            }
        }
    }
    Py_END_CRITICAL_SECTION();

    if (!view_transferred) {
        PyBuffer_Release(&view);
    }
    if (failed) {
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *UringApiRing_break_wait(UringApiRing *self, PyObject *Py_UNUSED(ignored)) {
    struct io_uring_sqe *sqe;
    unsigned long long user_data;
    int failed = 0;

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else {
        user_data = self->next_wakeup_data--;
        if (pending_store(self, user_data, URING_API_PENDING_WAKE, Py_None) < 0) {
            failed = 1;
        } else {
            sqe = get_sqe(self);
            if (!sqe) {
                pending_discard(self, user_data);
                failed = 1;
            } else {
                io_uring_prep_nop(sqe);
                sqe_set_data(self, sqe, user_data);
                if (submit_one(self) < 0) {
                    pending_discard(self, user_data);
                    failed = 1;
                }
            }
        }
    }
    Py_END_CRITICAL_SECTION();

    if (failed) {
        return NULL;
    }
    Py_RETURN_NONE;
}

static int UringApiRing_stop_delivery(UringApiRing *self) {
    PyThread_type_lock lock = NULL;
    PyObject *wakeup = NULL;
    bool running;
    bool same_thread;

    Py_BEGIN_CRITICAL_SECTION_MUTEX(&self->receive_mutex);
    running = delivery_is_running_locked(self);
    same_thread = running && self->delivery_thread_id == PyThread_get_thread_ident();
    if (running) {
        self->delivery_stop_requested = true;
        lock = self->delivery_done_lock;
    }
    Py_END_CRITICAL_SECTION();

    if (!running) {
        return 0;
    }
    if (same_thread) {
        return 0;
    }

    wakeup = UringApiRing_break_wait(self, NULL);
    if (!wakeup) {
        return -1;
    }
    Py_DECREF(wakeup);

    if (lock) {
        Py_BEGIN_ALLOW_THREADS
        PyThread_acquire_lock(lock, WAIT_LOCK);
        Py_END_ALLOW_THREADS
        PyThread_release_lock(lock);
    }
    return 0;
}

static PyObject *UringApiRing_stop(UringApiRing *self, PyObject *Py_UNUSED(ignored)) {
    if (UringApiRing_stop_delivery(self) < 0) {
        return NULL;
    }
    Py_RETURN_NONE;
}

static int parse_timeout(PyObject *timeout_obj, struct __kernel_timespec *timeout) {
    double seconds;
    if (timeout_obj == NULL || timeout_obj == Py_None) {
        return 0;
    }
    seconds = PyFloat_AsDouble(timeout_obj);
    if (PyErr_Occurred()) {
        return -1;
    }
    if (seconds < 0.0) {
        PyErr_SetString(PyExc_ValueError, "timeout must be non-negative or None");
        return -1;
    }
    timeout->tv_sec = (long long)seconds;
    timeout->tv_nsec = (long long)((seconds - (double)timeout->tv_sec) * 1000000000.0);
    if (timeout->tv_nsec < 0) {
        timeout->tv_nsec = 0;
    }
    if (timeout->tv_nsec > 999999999) {
        timeout->tv_nsec = 999999999;
    }
    return 1;
}

static PyObject *build_cqe_result(UringApiRing *self, struct io_uring_cqe *cqe) {
    PyObject *result = NULL;
    PyObject *payload = NULL;
    PyObject *pending_obj = NULL;
    unsigned long long user_data = cqe_get_data(self, cqe);
    int res = cqe->res;
    unsigned int flags = cqe->flags;

    pending_obj = pending_pop(self, user_data);
    if (!pending_obj) {
        return NULL;
    }
    if (pending_obj != Py_None) {
        UringApiPending *pending = (UringApiPending *)pending_obj;
        if (pending->kind == URING_API_PENDING_WAKE) {
            Py_DECREF(pending_obj);
            Py_RETURN_NONE;
        }
        if (res >= 0 && pending->kind == URING_API_PENDING_RECV) {
            payload = PyBytes_FromStringAndSize(PyBytes_AS_STRING(pending->buffer), res);
        } else if (res >= 0 && pending->kind == URING_API_PENDING_SEND) {
            payload = PyLong_FromLong(res);
        } else {
            payload = Py_NewRef(Py_None);
        }
    } else {
        payload = Py_NewRef(Py_None);
    }
    Py_DECREF(pending_obj);
    if (!payload) {
        return NULL;
    }

    result = PyDict_New();
    if (!result) {
        Py_DECREF(payload);
        return NULL;
    }
    if (dict_set_owned(result, "user_data", PyLong_FromUnsignedLongLong(user_data)) < 0 ||
        dict_set_owned(result, "res", PyLong_FromLong(res)) < 0 ||
        dict_set_owned(result, "flags", PyLong_FromUnsignedLong(flags)) < 0 ||
        PyDict_SetItemString(result, "result", payload) < 0) {
        Py_DECREF(payload);
        Py_DECREF(result);
        return NULL;
    }
    Py_DECREF(payload);
    return result;
}

static PyObject *UringApiRing_wait_impl(UringApiRing *self, int timeout_kind, struct __kernel_timespec *timeout,
                                        bool from_delivery_thread) {
    struct io_uring_cqe *cqe = NULL;
    PyObject *result;
    int ret;

    if (ring_check_open(self) < 0) {
        return NULL;
    }
    if (receive_wait_begin(self, from_delivery_thread) < 0) {
        return NULL;
    }

    errno = 0;
    if (timeout_kind == 0) {
        Py_BEGIN_ALLOW_THREADS
        ret = io_uring_wait_cqe(&self->ring, &cqe);
        Py_END_ALLOW_THREADS
    } else if (timeout->tv_sec == 0 && timeout->tv_nsec == 0) {
        ret = io_uring_peek_cqe(&self->ring, &cqe);
    } else {
        Py_BEGIN_ALLOW_THREADS
        ret = io_uring_wait_cqe_timeout(&self->ring, &cqe, timeout);
        Py_END_ALLOW_THREADS
    }

    if (ret < 0) {
        int errnum = normalize_ret_errno(ret);
        if (errnum == EAGAIN || errnum == ETIME || errnum == ETIMEDOUT) {
            receive_wait_end(self, from_delivery_thread);
            Py_RETURN_NONE;
        }
        errno = errnum;
        PyErr_SetFromErrno(PyExc_OSError);
        receive_wait_end(self, from_delivery_thread);
        return NULL;
    }
    if (!cqe) {
        receive_wait_end(self, from_delivery_thread);
        Py_RETURN_NONE;
    }

    Py_BEGIN_CRITICAL_SECTION_MUTEX(&self->receive_mutex);
    result = build_cqe_result(self, cqe);
    io_uring_cqe_seen(&self->ring, cqe);
    if (!from_delivery_thread) {
        self->receive_state = URING_API_RECEIVE_IDLE;
    }
    Py_END_CRITICAL_SECTION();
    return result;
}

static bool delivery_should_stop(UringApiRing *self) {
    bool stop;

    Py_BEGIN_CRITICAL_SECTION_MUTEX(&self->receive_mutex);
    stop = self->delivery_stop_requested || self->receive_state != URING_API_RECEIVE_DELIVERING ||
           !self->initialized;
    Py_END_CRITICAL_SECTION();
    return stop;
}

static PyObject *delivery_get_callback(UringApiRing *self) {
    PyObject *callback;

    Py_BEGIN_CRITICAL_SECTION_MUTEX(&self->receive_mutex);
    callback = Py_XNewRef(self->delivery_callback);
    Py_END_CRITICAL_SECTION();
    if (!callback) {
        PyErr_SetString(PyExc_RuntimeError, "delivery callback is not set");
    }
    return callback;
}

static int delivery_get_c_callback(UringApiRing *self, UringApi_CCompletionCallback *callback, void **user_data) {
    int found;

    Py_BEGIN_CRITICAL_SECTION_MUTEX(&self->receive_mutex);
    *callback = self->c_delivery_callback;
    *user_data = self->c_delivery_callback_user_data;
    found = *callback != NULL;
    Py_END_CRITICAL_SECTION();
    return found;
}

static void UringApiRing_delivery_thread(void *arg) {
    UringApiRing *self = (UringApiRing *)arg;
    PyGILState_STATE gil_state = PyGILState_Ensure();

    while (!delivery_should_stop(self)) {
        UringApi_CCompletionCallback c_callback;
        void *c_callback_user_data;
        PyObject *result = UringApiRing_wait_impl(self, 0, NULL, true);

        if (!result) {
            PyErr_WriteUnraisable((PyObject *)self);
            break;
        }
        if (result == Py_None) {
            Py_DECREF(result);
            continue;
        }

        if (delivery_get_c_callback(self, &c_callback, &c_callback_user_data)) {
            int callback_ret = c_callback((PyObject *)self, result, c_callback_user_data);
            Py_DECREF(result);
            if (callback_ret < 0) {
                PyErr_WriteUnraisable((PyObject *)self);
                break;
            }
        } else {
            PyObject *callback = delivery_get_callback(self);
            PyObject *call_result;
            if (!callback) {
                Py_DECREF(result);
                PyErr_WriteUnraisable((PyObject *)self);
                break;
            }
            call_result = PyObject_CallOneArg(callback, result);
            Py_DECREF(callback);
            Py_DECREF(result);
            if (!call_result) {
                PyErr_WriteUnraisable((PyObject *)self);
                break;
            }
            Py_DECREF(call_result);
        }
    }

    delivery_mark_exited(self);
    Py_DECREF(self);
    PyGILState_Release(gil_state);
}

static PyObject *UringApiRing_start(UringApiRing *self, PyObject *Py_UNUSED(ignored)) {
    PyThread_type_lock lock = NULL;
    bool allocated_lock = false;
    bool failed = false;
    unsigned long thread_id;

    Py_BEGIN_CRITICAL_SECTION_MUTEX(&self->receive_mutex);
    if (!self->initialized) {
        PyErr_SetString(PyExc_RuntimeError, "ring is closed");
        failed = true;
    } else if (!self->delivery_callback && !self->c_delivery_callback) {
        PyErr_SetString(PyExc_RuntimeError, "delivery callback is not set");
        failed = true;
    } else if (self->receive_state == URING_API_RECEIVE_DELIVERING) {
        PyErr_SetString(PyExc_RuntimeError, "delivery thread is already running");
        failed = true;
    } else if (self->receive_state != URING_API_RECEIVE_IDLE) {
        PyErr_SetString(PyExc_RuntimeError, "another wait is already active");
        failed = true;
    } else {
        if (!self->delivery_done_lock) {
            self->delivery_done_lock = PyThread_allocate_lock();
            allocated_lock = true;
        }
        if (!self->delivery_done_lock) {
            PyErr_NoMemory();
            failed = true;
        } else {
            lock = self->delivery_done_lock;
            PyThread_acquire_lock(lock, WAIT_LOCK);
        }
    }
    if (!failed) {
        self->receive_state = URING_API_RECEIVE_DELIVERING;
        self->delivery_stop_requested = false;
        Py_INCREF(self);
        thread_id = PyThread_start_new_thread(UringApiRing_delivery_thread, self);
        if (thread_id == (unsigned long)-1) {
            self->receive_state = URING_API_RECEIVE_IDLE;
            Py_DECREF(self);
            if (lock) {
                PyThread_release_lock(lock);
            }
            if (allocated_lock) {
                PyThread_free_lock(self->delivery_done_lock);
                self->delivery_done_lock = NULL;
            }
            PyErr_SetString(PyExc_RuntimeError, "failed to start delivery thread");
            failed = true;
        } else {
            self->delivery_thread_id = thread_id;
            lock = NULL;
        }
    }
    Py_END_CRITICAL_SECTION();

    if (failed) {
        return NULL;
    }
    Py_RETURN_NONE;
}

static int UringApiRing_set_c_callback_impl(UringApiRing *self, UringApi_CCompletionCallback callback, void *user_data) {
    int ret = 0;

    Py_BEGIN_CRITICAL_SECTION_MUTEX(&self->receive_mutex);
    if (delivery_is_running_locked(self)) {
        PyErr_SetString(PyExc_RuntimeError, "cannot change callback while delivery thread is running");
        ret = -1;
    } else {
        self->c_delivery_callback = callback;
        self->c_delivery_callback_user_data = callback ? user_data : NULL;
    }
    Py_END_CRITICAL_SECTION();
    return ret;
}

static PyObject *UringApiRing_wait(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"timeout", NULL};
    struct __kernel_timespec timeout;
    PyObject *timeout_obj = Py_None;
    int timeout_kind;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|O", keywords, &timeout_obj)) {
        return NULL;
    }
    timeout_kind = parse_timeout(timeout_obj, &timeout);
    if (timeout_kind < 0) {
        return NULL;
    }

    return UringApiRing_wait_impl(self, timeout_kind, &timeout, false);
}

static PyObject *UringApiRing_get_fd(UringApiRing *self, void *closure) {
    if (!self->initialized) {
        return PyLong_FromLong(-1);
    }
    return PyLong_FromLong(self->ring.ring_fd);
}

static PyObject *UringApiRing_get_features(UringApiRing *self, void *closure) {
    if (!self->initialized) {
        return PyLong_FromUnsignedLong(0);
    }
    return PyLong_FromUnsignedLong(self->ring.features);
}

static PyObject *UringApiRing_get_sq_entries(UringApiRing *self, void *closure) {
    if (!self->initialized) {
        return PyLong_FromUnsignedLong(0);
    }
    return PyLong_FromUnsignedLong(ring_sq_entries(self));
}

static PyObject *UringApiRing_get_cq_entries(UringApiRing *self, void *closure) {
    if (!self->initialized) {
        return PyLong_FromUnsignedLong(0);
    }
    return PyLong_FromUnsignedLong(ring_cq_entries(self));
}

static PyObject *UringApiRing_get_closed(UringApiRing *self, void *closure) {
    if (self->initialized) {
        Py_RETURN_FALSE;
    }
    Py_RETURN_TRUE;
}

static PyObject *UringApiRing_get_running(UringApiRing *self, void *closure) {
    if (self->receive_state == URING_API_RECEIVE_DELIVERING) {
        Py_RETURN_TRUE;
    }
    Py_RETURN_FALSE;
}

static PyObject *UringApiRing_get_callback(UringApiRing *self, void *closure) {
    PyObject *callback;

    Py_BEGIN_CRITICAL_SECTION_MUTEX(&self->receive_mutex);
    callback = Py_XNewRef(self->delivery_callback);
    Py_END_CRITICAL_SECTION();
    if (!callback) {
        Py_RETURN_NONE;
    }
    return callback;
}

static int UringApiRing_set_callback(UringApiRing *self, PyObject *value, void *closure) {
    PyObject *callback;
    PyObject *old_callback = NULL;
    int ret = 0;

    if (!value) {
        PyErr_SetString(PyExc_TypeError, "cannot delete callback");
        return -1;
    }
    if (value != Py_None && !PyCallable_Check(value)) {
        PyErr_SetString(PyExc_TypeError, "callback must be callable or None");
        return -1;
    }

    callback = value == Py_None ? NULL : Py_NewRef(value);
    Py_BEGIN_CRITICAL_SECTION_MUTEX(&self->receive_mutex);
    if (delivery_is_running_locked(self)) {
        PyErr_SetString(PyExc_RuntimeError, "cannot change callback while delivery thread is running");
        ret = -1;
    } else {
        old_callback = self->delivery_callback;
        self->delivery_callback = callback;
        callback = NULL;
    }
    Py_END_CRITICAL_SECTION();
    Py_XDECREF(callback);
    Py_XDECREF(old_callback);
    return ret;
}

static PyObject *UringApiCapi_RingNew(unsigned int entries, unsigned int flags) {
    PyObject *args = Py_BuildValue("(II)", entries, flags);
    PyObject *ring;

    if (!args) {
        return NULL;
    }
    ring = PyObject_CallObject((PyObject *)&UringApiRing_Type, args);
    Py_DECREF(args);
    return ring;
}

static int UringApiCapi_RingCheck(PyObject *ring) { return ring_type_check(ring); }

static int UringApiCapi_RingClose(PyObject *ring) {
    PyObject *result;
    if (!ring_type_check(ring)) {
        return -1;
    }
    result = UringApiRing_close((UringApiRing *)ring, NULL);
    if (!result) {
        return -1;
    }
    Py_DECREF(result);
    return 0;
}

static int UringApiCapi_RingFd(PyObject *ring) {
    if (!ring_type_check(ring)) {
        return -1;
    }
    if (!((UringApiRing *)ring)->initialized) {
        return -1;
    }
    return ((UringApiRing *)ring)->ring.ring_fd;
}

static unsigned int UringApiCapi_RingFeatures(PyObject *ring) {
    if (!ring_type_check(ring) || !((UringApiRing *)ring)->initialized) {
        return 0;
    }
    return ((UringApiRing *)ring)->ring.features;
}

static unsigned int UringApiCapi_RingSqEntries(PyObject *ring) {
    if (!ring_type_check(ring) || !((UringApiRing *)ring)->initialized) {
        return 0;
    }
    return ring_sq_entries((UringApiRing *)ring);
}

static unsigned int UringApiCapi_RingCqEntries(PyObject *ring) {
    if (!ring_type_check(ring) || !((UringApiRing *)ring)->initialized) {
        return 0;
    }
    return ring_cq_entries((UringApiRing *)ring);
}

static int UringApiCapi_RingClosed(PyObject *ring) {
    if (!ring_type_check(ring)) {
        return -1;
    }
    return !((UringApiRing *)ring)->initialized;
}

static int UringApiCapi_RingRunning(PyObject *ring) {
    if (!ring_type_check(ring)) {
        return -1;
    }
    return ((UringApiRing *)ring)->receive_state == URING_API_RECEIVE_DELIVERING;
}

static int UringApiCapi_RingSubmitRecv(PyObject *ring, int fd, Py_ssize_t n, unsigned long long user_data) {
    PyObject *result;
    if (!ring_type_check(ring)) {
        return -1;
    }
    result = PyObject_CallMethod(ring, "submit_recv", "inK", fd, n, user_data);
    if (!result) {
        return -1;
    }
    Py_DECREF(result);
    return 0;
}

static int UringApiCapi_RingSubmitSend(PyObject *ring, int fd, PyObject *data, unsigned long long user_data) {
    PyObject *result;
    if (!ring_type_check(ring)) {
        return -1;
    }
    result = PyObject_CallMethod(ring, "submit_send", "iOK", fd, data, user_data);
    if (!result) {
        return -1;
    }
    Py_DECREF(result);
    return 0;
}

static int UringApiCapi_RingBreakWait(PyObject *ring) {
    PyObject *result;
    if (!ring_type_check(ring)) {
        return -1;
    }
    result = UringApiRing_break_wait((UringApiRing *)ring, NULL);
    if (!result) {
        return -1;
    }
    Py_DECREF(result);
    return 0;
}

static PyObject *UringApiCapi_RingWait(PyObject *ring, double timeout) {
    struct __kernel_timespec timeout_value;
    int timeout_kind;
    if (!ring_type_check(ring)) {
        return NULL;
    }
    if (timeout < 0.0) {
        return UringApiRing_wait_impl((UringApiRing *)ring, 0, NULL, false);
    }
    timeout_value.tv_sec = (long long)timeout;
    timeout_value.tv_nsec = (long long)((timeout - (double)timeout_value.tv_sec) * 1000000000.0);
    if (timeout_value.tv_nsec < 0) {
        timeout_value.tv_nsec = 0;
    }
    if (timeout_value.tv_nsec > 999999999) {
        timeout_value.tv_nsec = 999999999;
    }
    timeout_kind = timeout == 0.0 ? 2 : 1;
    return UringApiRing_wait_impl((UringApiRing *)ring, timeout_kind, &timeout_value, false);
}

static int UringApiCapi_RingSetCallback(PyObject *ring, PyObject *callback) {
    if (!ring_type_check(ring)) {
        return -1;
    }
    return UringApiRing_set_callback((UringApiRing *)ring, callback ? callback : Py_None, NULL);
}

static int UringApiCapi_RingSetCCallback(PyObject *ring, UringApi_CCompletionCallback callback, void *user_data) {
    if (!ring_type_check(ring)) {
        return -1;
    }
    return UringApiRing_set_c_callback_impl((UringApiRing *)ring, callback, user_data);
}

static int UringApiCapi_RingStart(PyObject *ring) {
    PyObject *result;
    if (!ring_type_check(ring)) {
        return -1;
    }
    result = UringApiRing_start((UringApiRing *)ring, NULL);
    if (!result) {
        return -1;
    }
    Py_DECREF(result);
    return 0;
}

static int UringApiCapi_RingStop(PyObject *ring) {
    PyObject *result;
    if (!ring_type_check(ring)) {
        return -1;
    }
    result = UringApiRing_stop((UringApiRing *)ring, NULL);
    if (!result) {
        return -1;
    }
    Py_DECREF(result);
    return 0;
}

static PyMethodDef UringApiRing_methods[] = {
    {"close", (PyCFunction)UringApiRing_close, METH_NOARGS, "Close the io_uring instance."},
    {"start", (PyCFunction)UringApiRing_start, METH_NOARGS, "Start the delivery callback thread."},
    {"stop", (PyCFunction)UringApiRing_stop, METH_NOARGS, "Stop the delivery callback thread."},
    {"submit_recv", _PyCFunction_CAST(UringApiRing_submit_recv), METH_VARARGS | METH_KEYWORDS,
     "Submit a recv operation."},
    {"submit_send", _PyCFunction_CAST(UringApiRing_submit_send), METH_VARARGS | METH_KEYWORDS,
     "Submit a send operation."},
    {"break_wait", (PyCFunction)UringApiRing_break_wait, METH_NOARGS,
     "Interrupt a thread blocked in wait without producing a user completion."},
    {"wait", _PyCFunction_CAST(UringApiRing_wait), METH_VARARGS | METH_KEYWORDS,
     "Wait for one completion and return its result."},
    {"__enter__", (PyCFunction)UringApiRing_enter, METH_NOARGS, NULL},
    {"__exit__", (PyCFunction)UringApiRing_exit, METH_VARARGS, NULL},
    {NULL, NULL, 0, NULL}};

static PyGetSetDef UringApiRing_getset[] = {{"fd", (getter)UringApiRing_get_fd, NULL, NULL, NULL},
                                            {"features", (getter)UringApiRing_get_features, NULL, NULL, NULL},
                                            {"sq_entries", (getter)UringApiRing_get_sq_entries, NULL, NULL, NULL},
                                            {"cq_entries", (getter)UringApiRing_get_cq_entries, NULL, NULL, NULL},
                                            {"closed", (getter)UringApiRing_get_closed, NULL, NULL, NULL},
                                            {"running", (getter)UringApiRing_get_running, NULL, NULL, NULL},
                                            {"callback", (getter)UringApiRing_get_callback,
                                             (setter)UringApiRing_set_callback, NULL, NULL},
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

static PyTypeObject UringApiPending_Type = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "_uring_api._Pending",
    .tp_basicsize = sizeof(UringApiPending),
    .tp_dealloc = (destructor)UringApiPending_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "pending io_uring operation state",
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

    if (PyType_Ready(&UringApiPending_Type) < 0) {
        return -1;
    }
    if (PyType_Ready(&UringApiRing_Type) < 0) {
        return -1;
    }
    Py_INCREF(&UringApiRing_Type);
    if (PyModule_AddObject(module, "Ring", (PyObject *)&UringApiRing_Type) < 0) {
        Py_DECREF(&UringApiRing_Type);
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